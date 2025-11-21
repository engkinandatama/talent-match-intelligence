import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------------------------------
# 1. CONFIG & DB CONNECTION
# -------------------------------------------------

st.set_page_config(
    page_title="Talent Match Intelligence",
    page_icon="🧠",
    layout="wide"
)

st.title("🧠 Talent Match Intelligence")
st.caption("Professional minimalist dashboard – powered by Supabase & SQL Talent Engine")

# 🔹 Ambil DB_URL dari secrets
DB_URL = st.secrets["DB_URL"]

# 🔹 Siapkan SQLAlchemy engine
engine = create_engine(DB_URL, pool_pre_ping=True)

# 🔹 (Opsional tapi sangat berguna) — Test koneksi
def test_connection():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        st.success("✓ Connected to Supabase Postgres database")
    except Exception as e:
        st.error("✗ Database Connection FAILED")
        st.exception(e)

test_connection()


# -------------------------------------------------
# 2. HELPER FUNCTIONS
# -------------------------------------------------

@st.cache_data(ttl=600)
def load_positions():
    sql = "SELECT position_id, name FROM dim_positions ORDER BY position_id"
    return pd.read_sql(sql, engine)

@st.cache_data(ttl=600)
def load_high_performers(min_rating: int = 5):
    sql = """
        SELECT DISTINCT e.employee_id, e.fullname
        FROM employees e
        JOIN performance_yearly py USING(employee_id)
        WHERE py.rating >= :min_rating
        ORDER BY e.fullname
    """
    return pd.read_sql(text(sql), engine, params={"min_rating": min_rating})


# -------------------------------------------------
# 3. BUILD TALENT MATCH SQL ENGINE
# -------------------------------------------------

def build_match_sql(manual_hp_ids, role_position_id, min_hp_rating: int) -> str:

    # Mode A: Manual HP
    if manual_hp_ids:
        manual_list_sql = ",".join(f"'{emp}'" for emp in manual_hp_ids)
        manual_array_sql = f"ARRAY[{manual_list_sql}]::text[]"
    else:
        manual_array_sql = "ARRAY[]::text[]"

    # Mode B: Role-based
    role_sql = "NULL" if role_position_id is None else str(role_position_id)

    # RAW SQL ENGINE – sudah aman
    sql = f"""
WITH params AS (
    SELECT
        {manual_array_sql} AS manual_hp,
        {role_sql}::int AS role_position_id,
        {min_hp_rating}::int AS min_hp_rating
),

manual_set AS (
    SELECT unnest(manual_hp) AS employee_id FROM params
),

role_set AS (
    SELECT DISTINCT e.employee_id
    FROM employees e
    JOIN performance_yearly py USING(employee_id)
    JOIN params p ON TRUE
    WHERE py.rating >= p.min_hp_rating
      AND p.role_position_id IS NOT NULL
      AND e.position_id = p.role_position_id
),

benchmark_set AS (
    SELECT employee_id FROM manual_set
    UNION
    SELECT employee_id FROM role_set
),

fallback_benchmark AS (
    SELECT py.employee_id
    FROM performance_yearly py
    JOIN params p ON TRUE
    WHERE py.rating >= p.min_hp_rating
),

final_bench AS (
    SELECT DISTINCT employee_id FROM benchmark_set
    UNION
    SELECT DISTINCT employee_id FROM fallback_benchmark
    WHERE NOT EXISTS (SELECT 1 FROM benchmark_set)
),

latest AS (
    SELECT (SELECT MAX(year) FROM competencies_yearly) AS comp_year
),

baseline_numeric AS (
    SELECT 
        tv_name,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score) AS baseline_score
    FROM (
        SELECT c.pillar_code AS tv_name, c.score::numeric AS score
        FROM competencies_yearly c
        JOIN latest l ON c.year = l.comp_year
        WHERE c.employee_id IN (SELECT employee_id FROM final_bench)

        UNION ALL SELECT 'iq', p.iq::numeric FROM profiles_psych p WHERE p.employee_id IN (SELECT employee_id FROM final_bench)
        UNION ALL SELECT 'gtq', p.gtq::numeric FROM profiles_psych p WHERE p.employee_id IN (SELECT employee_id FROM final_bench)
        UNION ALL SELECT 'tiki', p.tiki::numeric FROM profiles_psych p WHERE p.employee_id IN (SELECT employee_id FROM final_bench)
        UNION ALL SELECT 'faxtor', p.faxtor::numeric FROM profiles_psych p WHERE p.employee_id IN (SELECT employee_id FROM final_bench)
        UNION ALL SELECT 'pauli', p.pauli::numeric FROM profiles_psych p WHERE p.employee_id IN (SELECT employee_id FROM final_bench)
    ) x
    GROUP BY tv_name
),

all_numeric_scores AS (
    SELECT c.employee_id, c.pillar_code AS tv_name, c.score::numeric AS user_score
    FROM competencies_yearly c
    JOIN latest l ON c.year = l.comp_year

    UNION ALL SELECT employee_id, 'iq', iq::numeric FROM profiles_psych
    UNION ALL SELECT employee_id, 'gtq', gtq::numeric FROM profiles_psych
    UNION ALL SELECT employee_id, 'tiki', tiki::numeric FROM profiles_psych
    UNION ALL SELECT employee_id, 'faxtor', faxtor::numeric FROM profiles_psych
    UNION ALL SELECT employee_id, 'pauli', pauli::numeric FROM profiles_psych
),

numeric_tv AS (
    SELECT
        sc.employee_id,
        bn.tv_name,
        bn.baseline_score,
        sc.user_score,
        (sc.user_score / NULLIF(bn.baseline_score,0)) * 100 AS tv_match_rate
    FROM all_numeric_scores sc
    JOIN baseline_numeric bn ON sc.tv_name = bn.tv_name
),

reverse_list AS (
    SELECT UNNEST(ARRAY['Papi_I','Papi_K','Papi_Z','Papi_T']) AS scale_code
),

baseline_papi AS (
    SELECT
        ps.scale_code AS tv_name,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ps.score) AS baseline_score,
        CASE WHEN rl.scale_code IS NULL THEN FALSE ELSE TRUE END AS is_reverse
    FROM papi_scores ps
    JOIN final_bench fb ON ps.employee_id = fb.employee_id
    LEFT JOIN reverse_list rl ON rl.scale_code = ps.scale_code
    GROUP BY ps.scale_code, rl.scale_code
),

papi_tv AS (
    SELECT
        ps.employee_id,
        bp.tv_name,
        bp.baseline_score,
        ps.score::numeric AS user_score,
        CASE 
            WHEN bp.is_reverse THEN ((2 * bp.baseline_score - ps.score::numeric) / NULLIF(bp.baseline_score,0)) * 100
            ELSE (ps.score::numeric / NULLIF(bp.baseline_score,0)) * 100
        END AS tv_match_rate
    FROM papi_scores ps
    JOIN baseline_papi bp ON ps.scale_code = bp.tv_name
),

baseline_cat AS (
    SELECT
        'mbti' AS tv_name,
        MODE() WITHIN GROUP (ORDER BY UPPER(TRIM(mbti))) AS baseline_value
    FROM profiles_psych p
    JOIN final_bench fb ON fb.employee_id = p.employee_id

    UNION ALL
    SELECT
        'disc',
        MODE() WITHIN GROUP (ORDER BY UPPER(TRIM(disc)))
    FROM profiles_psych p
    JOIN final_bench fb ON fb.employee_id = p.employee_id
),

categorical_tv AS (
    SELECT
        p.employee_id,
        bc.tv_name,
        1::numeric AS baseline_score,
        1::numeric AS user_score,
        CASE 
            WHEN (bc.tv_name='mbti' AND UPPER(TRIM(p.mbti)) = bc.baseline_value)
              OR (bc.tv_name='disc' AND UPPER(TRIM(p.disc)) = bc.baseline_value)
            THEN 100 ELSE 0 END AS tv_match_rate
    FROM profiles_psych p
    CROSS JOIN baseline_cat bc
),

all_tv AS (
    SELECT * FROM numeric_tv
    UNION ALL
    SELECT * FROM papi_tv
    UNION ALL
    SELECT * FROM categorical_tv
),

tv_map AS (
    SELECT tv_name, tgv_name, tv_weight
    FROM talent_variables_mapping
),

tgv_match AS (
    SELECT
        a.employee_id,
        m.tgv_name,
        SUM(a.tv_match_rate * m.tv_weight) / SUM(m.tv_weight) AS tgv_match_rate
    FROM all_tv a
    JOIN tv_map m USING(tv_name)
    GROUP BY a.employee_id, m.tgv_name
),

final_match AS (
    SELECT
        t.employee_id,
        SUM(t.tgv_match_rate * g.tgv_weight) AS final_match_rate
    FROM tgv_match t
    JOIN talent_group_weights g USING(tgv_name)
    GROUP BY t.employee_id
)

SELECT 
    e.employee_id,
    e.fullname,
    fm.final_match_rate
FROM final_match fm
JOIN employees e USING(employee_id)
ORDER BY final_match_rate DESC
LIMIT 200;
"""
    return sql


def run_match_query(manual_hp_ids, role_position_id, min_hp_rating):
    sql = build_match_sql(manual_hp_ids, role_position_id, min_hp_rating)
    return pd.read_sql(sql, engine)


# -------------------------------------------------
# 4. SIDEBAR INPUT
# -------------------------------------------------

st.sidebar.header("⚙️ Benchmark Settings")

min_rating = st.sidebar.slider("Minimum rating as High Performer", 1, 5, 5)

positions_df = load_positions()
position_options = {row["name"]: row["position_id"] for _, row in positions_df.iterrows()}

position_label = st.sidebar.selectbox(
    "Target Position (Mode B – optional)",
    ["(None)"] + list(position_options.keys())
)

selected_position_id = None if position_label == "(None)" else position_options[position_label]

hp_df = load_high_performers(min_rating)
hp_df["label"] = hp_df["employee_id"] + " – " + hp_df["fullname"]

manual_selected = st.sidebar.multiselect(
    "Manual Benchmark High Performers (Mode A – optional)",
    options=hp_df["label"].tolist(),
    default=[]
)

manual_ids = [
    label.split(" – ")[0] for label in manual_selected
]

run_button = st.sidebar.button("🚀 Run Talent Match")


# -------------------------------------------------
# 5. MAIN OUTPUT
# -------------------------------------------------

if run_button:
    with st.spinner("Running Talent Match Engine..."):
        result_df = run_match_query(manual_ids, selected_position_id, min_rating)

    st.subheader("📊 Ranked Talent List")

    st.write(
        f"Benchmark based on **{len(manual_ids)} manual HP(s)** "
        f"and **position: {position_label}** (min rating **{min_rating}**)."
    )

    df_view = result_df.copy()
    df_view["final_match_rate"] = df_view["final_match_rate"].round(2)

    st.dataframe(df_view, hide_index=True, use_container_width=True)

    if not df_view.empty:
        top_row = df_view.iloc[0]
        st.markdown("---")
        st.subheader("🏅 Top Match")

        col1, col2 = st.columns([2, 1])
        with col1:
            st.markdown(
                f"""
                **{top_row['fullname']}**  
                `ID: {top_row['employee_id']}`  
                **Final Match Score:** {top_row['final_match_rate']:.2f}
                """
            )

        with col2:
            st.metric("Final Match", f"{top_row['final_match_rate']:.2f}")

        # Download
        st.markdown("### ⬇️ Download Results")
        csv = df_view.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name="talent_match_results.csv",
            mime="text/csv"
        )

else:
    st.info("Set benchmark di sidebar, lalu klik **Run Talent Match** untuk melihat ranking.")
