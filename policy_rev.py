import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

# ============================================================
# PAGE CONFIG + STYLING
# ============================================================
st.set_page_config(page_title="Policies Revenue Dashboard", layout="wide")

st.markdown(
    """
    <style>
      .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }

      /* Slightly smaller metric values + allow wrapping (avoid ... truncation) */
      [data-testid="stMetricValue"] {
        font-size: 26px !important;
        line-height: 1.15 !important;
        white-space: normal !important;
        overflow: visible !important;
        text-overflow: clip !important;
      }
      [data-testid="stMetricLabel"] { font-size: 13px; opacity: 0.85; }
      div[data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)

REV_FILE_PATH = "policies revenue dashboard.xlsx"
COST_FILE_PATH = "Cost breakdown.xlsx"

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# ============================================================
# HELPERS
# ============================================================
def safe_num(x):
    return pd.to_numeric(x, errors="coerce")

def clean_pct_series(x):
    """If values look like 0-1 proportions, convert to % scale (0-100)."""
    x = pd.to_numeric(x, errors="coerce")
    mx = x.max(skipna=True)
    if pd.notna(mx) and mx <= 1.5:
        return x * 100
    return x

def format_table(df: pd.DataFrame, int_cols=None, float_cols=None, pct_cols=None, money_cols=None):
    int_cols = int_cols or []
    float_cols = float_cols or []
    pct_cols = pct_cols or []
    money_cols = money_cols or []

    fmt = {}
    for c in int_cols:
        if c in df.columns:
            fmt[c] = "{:,.0f}"
    for c in money_cols:
        if c in df.columns:
            fmt[c] = "{:,.0f}"
    for c in float_cols:
        if c in df.columns:
            fmt[c] = "{:,.2f}"
    for c in pct_cols:
        if c in df.columns:
            fmt[c] = "{:,.2f}%"
    return df.style.format(fmt, na_rep="")

def month_to_quarter(m):
    m = str(m).strip()[:3]
    if m in ["Jan", "Feb", "Mar"]:
        return "Q1"
    if m in ["Apr", "May", "Jun"]:
        return "Q2"
    if m in ["Jul", "Aug", "Sep"]:
        return "Q3"
    return "Q4"

def fmt_pkr(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return f"{x:,.0f}"

def _parse_month_col_to_month_abbr(col) -> str | None:
    """
    Convert columns like 'Jan-25' or datetime(2025-01-01) into 'Jan'.
    Return None if it can't be interpreted as a month.
    """
    try:
        dt = pd.to_datetime(col)
        if pd.notna(dt):
            return dt.strftime("%b")
    except Exception:
        pass

    s = str(col).strip()
    if len(s) >= 3:
        abbr = s[:3].title()
        if abbr in MONTHS:
            return abbr
    return None

def extract_monthly_cost_totals(cost_raw: pd.DataFrame) -> pd.DataFrame:
    """
    FIX: Avoid double counting.
    Prefer a 'Total' row if present; otherwise pick the most total-like row.
    Only use Jan–Dec columns (ignore any annual total column).
    """
    df = cost_raw.copy()
    df.columns = [str(c).strip() for c in df.columns]

    label_col = df.columns[0]
    labels = df[label_col].astype(str).str.strip()

    month_col_map = {}
    for c in df.columns[1:]:
        m = _parse_month_col_to_month_abbr(c)
        if m in MONTHS:
            month_col_map[c] = m

    month_cols = list(month_col_map.keys())
    if not month_cols:
        raise ValueError("Could not detect month columns in Cost breakdown.xlsx")

    df[month_cols] = df[month_cols].apply(pd.to_numeric, errors="coerce")

    total_candidates = df[labels.str.contains("total", case=False, na=False)].copy()
    if len(total_candidates) >= 1:
        total_candidates["_row_sum"] = total_candidates[month_cols].sum(axis=1, skipna=True)
        total_row = total_candidates.sort_values("_row_sum", ascending=False).iloc[0]
        monthly_totals = total_row[month_cols]
    else:
        tmp = df.copy()
        tmp["_non_null_months"] = tmp[month_cols].notna().sum(axis=1)
        tmp["_row_sum"] = tmp[month_cols].sum(axis=1, skipna=True)
        cand = tmp[tmp["_non_null_months"] >= 8].copy()
        if len(cand) == 0:
            monthly_totals = df[month_cols].sum(axis=0, skipna=True)
        else:
            total_row = cand.sort_values("_row_sum", ascending=False).iloc[0]
            monthly_totals = total_row[month_cols]

    month_cost = {m: 0.0 for m in MONTHS}
    for col, m in month_col_map.items():
        val = monthly_totals.get(col, np.nan)
        month_cost[m] = float(val) if pd.notna(val) else 0.0

    return pd.DataFrame({"Month": MONTHS, "Cost_PKR": [month_cost[m] for m in MONTHS]})

def abbr_pkr(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    x = float(x)
    ax = abs(x)
    if ax >= 1_000_000_000:
        return f"{x/1_000_000_000:.1f}b".replace(".0b", "b")
    if ax >= 1_000_000:
        return f"{x/1_000_000:.0f}m"
    if ax >= 1_000:
        return f"{x/1_000:.0f}k"
    return f"{x:.0f}"

def abbr_k(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    x = float(x)
    ax = abs(x)
    if ax >= 1_000_000:
        return f"{x/1_000_000:.1f}M".replace(".0M", "M")
    if ax >= 1_000:
        return f"{x/1_000:.0f}K"
    return f"{x:.0f}"



# ============================================================
# LOAD
# ============================================================
@st.cache_data
def load_all():
    dash = pd.read_excel(REV_FILE_PATH, sheet_name="Dashboard", header=None)
    work = pd.read_excel(REV_FILE_PATH, sheet_name="Workings")
    bundles = pd.read_excel(REV_FILE_PATH, sheet_name="Bundles")
    cost_raw = pd.read_excel(COST_FILE_PATH, sheet_name=0)
    return dash, work, bundles, cost_raw

dash, work, bundles, cost_raw = load_all()

# ============================================================
# PARSE: TOPLINE BY CHANNEL (Dashboard)
# ============================================================
topline = dash.iloc[2:7, 0:3].copy()
topline.columns = ["Channel", "PKR_millions", "Share_topline"]
topline["PKR_millions"] = safe_num(topline["PKR_millions"])
topline["Share_topline"] = clean_pct_series(safe_num(topline["Share_topline"]))
topline["Revenue_PKR"] = topline["PKR_millions"] * 1_000_000

topline_ytd_pkr = float(
    topline.loc[topline["Channel"].astype(str).str.contains("YTD", case=False, na=False), "Revenue_PKR"].iloc[0]
)

# ============================================================
# PARSE: POLICYWISE (Dashboard)
# ============================================================
policy_tbl = dash.iloc[11:16, 0:6].copy()
policy_tbl.columns = ["Policy", "DCB", "Bundle", "Postpaid_JC", "Total", "Share_topline"]
for c in ["DCB", "Bundle", "Postpaid_JC", "Total", "Share_topline"]:
    policy_tbl[c] = safe_num(policy_tbl[c])

policy_tbl["Share_topline"] = clean_pct_series(policy_tbl["Share_topline"])
policy_tbl_no_total = policy_tbl[~policy_tbl["Policy"].astype(str).str.strip().eq("Total")].copy()

# Rename JC -> Jazzcash (everywhere)
policy_tbl_no_total = policy_tbl_no_total.rename(columns={"Postpaid_JC": "Postpaid_Jazzcash"})
policy_tbl = policy_tbl.rename(columns={"Postpaid_JC": "Postpaid_Jazzcash"})

# ============================================================
# PARSE: PARTNERWISE (Dashboard)  (keep table, remove insight later)
# ============================================================
partner_tbl = dash.iloc[20:26, 0:9].copy()
partner_tbl.columns = [
    "Partner", "Prepaid", "Bundle", "Postpaid", "JazzCash", "Total",
    "Share_topline", "Claims_Paid", "Claims_Ratio"
]
for c in ["Prepaid", "Bundle", "Postpaid", "JazzCash", "Total", "Share_topline", "Claims_Paid", "Claims_Ratio"]:
    partner_tbl[c] = safe_num(partner_tbl[c])
partner_tbl["Share_topline"] = clean_pct_series(partner_tbl["Share_topline"])

# Rename JazzCash -> Jazzcash in display
partner_tbl = partner_tbl.rename(columns={"JazzCash": "Jazzcash"})

# ============================================================
# MONTH-ON-MONTH OVERALL REVENUE (Workings -> Total row)
# ============================================================
month_map = {
    "Jan": "(Multiple Items)",
    "Feb": "Unnamed: 2",
    "Mar": "Unnamed: 3",
    "Apr": "Unnamed: 4",
    "May": "Unnamed: 5",
    "Jun": "Unnamed: 6",
    "Jul": "Unnamed: 7",
    "Aug": "Unnamed: 8",
    "Sep": "Unnamed: 9",
    "Oct": "Unnamed: 10",
    "Nov": "Unnamed: 11",
    "Dec": "Unnamed: 12",
}

total_row = work[work["Policy"].astype(str).str.strip().eq("Total")].iloc[0]
mom_rev = pd.DataFrame(
    {"Month": MONTHS, "Revenue_PKR": [safe_num(total_row.get(month_map[m], np.nan)) for m in MONTHS]}
)
mom_rev["Revenue_PKR"] = mom_rev["Revenue_PKR"].fillna(0)

# ============================================================
# COST (FIXED): Use totals row (no double counting)
# ============================================================
mom_cost = extract_monthly_cost_totals(cost_raw)

# ============================================================
# NET (Profit) TABLE (MONTHLY)
# ============================================================
mom = mom_rev.merge(mom_cost, on="Month", how="left")
mom["Cost_PKR"] = mom["Cost_PKR"].fillna(0)
mom["Net_PKR"] = mom["Revenue_PKR"] - mom["Cost_PKR"]  # Net = Profit
mom["Net_Margin_%"] = np.where(mom["Revenue_PKR"] > 0, (mom["Net_PKR"] / mom["Revenue_PKR"]) * 100, np.nan)

# Topline cost/net (sum over months)
topline_cost_pkr = float(mom["Cost_PKR"].sum())
topline_net_pkr = float(mom["Net_PKR"].sum())

avg_rev_per_month = float(mom["Revenue_PKR"].mean())
avg_net_per_month = float(mom["Net_PKR"].mean())

# ============================================================
# QUARTERS: Average Revenue per Quarter + QoQ comparison
# ============================================================
q = mom.copy()
q["Quarter"] = q["Month"].apply(month_to_quarter)

quarter_rev = (
    q.groupby("Quarter", as_index=False)["Revenue_PKR"]
     .mean()
     .rename(columns={"Revenue_PKR": "Avg_Revenue_PKR"})
     .sort_values("Quarter")
)

quarter_rev["QoQ_Change_%"] = np.where(
    quarter_rev["Avg_Revenue_PKR"].shift(1) > 0,
    (quarter_rev["Avg_Revenue_PKR"] / quarter_rev["Avg_Revenue_PKR"].shift(1) - 1) * 100,
    np.nan
)


# ============================================================
# BUNDLES: only TOP performing bundles (remove BIMA/WebDoc sections)
# ============================================================
bund = bundles.copy()
bund.columns = bund.columns.astype(str).str.strip()
for col in ["Partner", "Bundle Name", "Date", "Revenue", "Subs", "Rate"]:
    if col not in bund.columns:
        bund[col] = np.nan

bund["Revenue"] = pd.to_numeric(bund["Revenue"], errors="coerce")
bund["Date"] = pd.to_datetime(bund["Date"], errors="coerce")

bund = bund[bund["Partner"].isin(["BIMA", "WebDoc"])].copy()
bund = bund.dropna(subset=["Revenue", "Date"])
bund = bund[bund["Revenue"] > 0].copy()

top_bundles_overall = (
    bund.groupby(["Partner", "Bundle Name"], as_index=False)["Revenue"]
    .sum()
    .sort_values("Revenue", ascending=False)
    .head(12)
)

# ============================================================
# SIDEBAR FILTERS
# ============================================================
st.sidebar.title("Filters")

partner_filter = st.sidebar.multiselect(
    "Partners (partner section)",
    options=partner_tbl["Partner"].astype(str).tolist(),
    default=partner_tbl["Partner"].astype(str).tolist(),
)

policy_filter = st.sidebar.multiselect(
    "Policies (policy section)",
    options=policy_tbl_no_total["Policy"].astype(str).tolist(),
    default=policy_tbl_no_total["Policy"].astype(str).tolist(),
)

partner_tbl_f = partner_tbl[partner_tbl["Partner"].astype(str).isin(partner_filter)].copy()
policy_tbl_f = policy_tbl_no_total[policy_tbl_no_total["Policy"].astype(str).isin(policy_filter)].copy()

# ============================================================
# DASHBOARD
# ============================================================
st.title("📊 Policies Revenue Dashboard (FikrFree)")

# ------------------------------------------------------------
# TOP METRICS + 71% NOTE
# ------------------------------------------------------------
st.subheader("💰 Topline (YTD)")

r1c1, r1c2, r1c3 = st.columns([1.6, 1.6, 1.6])
with r1c1:
    st.metric("Topline Revenue (PKR)", fmt_pkr(topline_ytd_pkr))
    st.caption("▲ 71% vs last year")
with r1c2:
    st.metric("Topline Cost (PKR)", fmt_pkr(topline_cost_pkr))
with r1c3:
    st.metric("Topline Net (PKR)", fmt_pkr(topline_net_pkr))

r2c1, r2c2 = st.columns([1.6, 1.6])
r2c1.metric("Avg Revenue / Month (PKR)", fmt_pkr(avg_rev_per_month))

# ------------------------------------------------------------
st.subheader("📈 Month-on-Month Trend: Topline vs Cost vs Net")

trend = mom[["Month", "Revenue_PKR", "Cost_PKR", "Net_PKR"]].copy()
trend = trend.rename(columns={"Revenue_PKR": "Topline", "Cost_PKR": "Cost", "Net_PKR": "Net"})

# Ensure month order
month_order = {m: i for i, m in enumerate(MONTHS, start=1)}
trend["Month_num"] = trend["Month"].map(month_order)
trend = trend.sort_values("Month_num")

fig = go.Figure()

def add_line(name, y):
    fig.add_trace(
        go.Scatter(
            x=trend["Month"],
            y=y,
            mode="lines+markers+text",
            name=name,
            text=[abbr_pkr(v) for v in y],
            textposition="top center",
        )
    )

add_line("Topline", trend["Topline"])
add_line("Cost", trend["Cost"])
add_line("Net", trend["Net"])

fig.update_layout(
    xaxis_title="Month",
    yaxis_title="PKR",
    hovermode="x unified",
    margin=dict(l=10, r=10, t=20, b=10),
)

st.plotly_chart(fig, use_container_width=True)



# ------------------------------------------------------------
# MONTH-ON-MONTH TABLE (rename: Revenue, Cost, Net)
# ------------------------------------------------------------
st.subheader("📅 Month-on-Month (Revenue, Cost, Net)")

mom_show = mom.copy()
mom_show["Share of YTD Revenue (%)"] = np.where(
    topline_ytd_pkr > 0, (mom_show["Revenue_PKR"] / topline_ytd_pkr) * 100, 0
)

st.dataframe(
    format_table(
        mom_show.rename(
            columns={
                "Revenue_PKR": "Revenue (PKR)",
                "Cost_PKR": "Cost (PKR)",
                "Net_PKR": "Net (PKR)",
                "Net_Margin_%": "Net Margin (%)",
            }
        )[["Month", "Revenue (PKR)", "Cost (PKR)", "Net (PKR)", "Net Margin (%)", "Share of YTD Revenue (%)"]],
        money_cols=["Revenue (PKR)", "Cost (PKR)", "Net (PKR)"],
        pct_cols=["Net Margin (%)", "Share of YTD Revenue (%)"],
    ),
    use_container_width=True,
    hide_index=True,
)

# ------------------------------------------------------------
# QUARTERS: Avg Revenue + comparison vs previous quarter + graph
# ------------------------------------------------------------
st.subheader("🧮 Quarterly Average Revenue + QoQ Comparison")

qtbl_display = quarter_rev.rename(
    columns={
        "Avg_Revenue_PKR": "Avg Revenue (PKR)",
        "QoQ_Change_%": "QoQ Change (%)",
    }
)

st.dataframe(
    format_table(
        qtbl_display[["Quarter", "Avg Revenue (PKR)", "QoQ Change (%)"]],
        money_cols=["Avg Revenue (PKR)"],
        pct_cols=["QoQ Change (%)"],
    ),
    use_container_width=True,
    hide_index=True,
)

# Graph for quarterly averages
q_chart = quarter_rev.set_index("Quarter")[["Avg_Revenue_PKR"]].rename(columns={"Avg_Revenue_PKR": "Avg Revenue"})
qbar = quarter_rev.copy()
qbar["Avg Revenue Label"] = qbar["Avg_Revenue_PKR"].apply(abbr_pkr)

fig_q = px.bar(
    qbar,
    x="Quarter",
    y="Avg_Revenue_PKR",
    text="Avg Revenue Label",
)
fig_q.update_traces(textposition="outside")
fig_q.update_layout(yaxis_title="Avg Revenue (PKR)", xaxis_title="")
st.plotly_chart(fig_q, use_container_width=True)

# ------------------------------------------------------------
# Revenue Breakdown via Policy Type (table + graph)
# ------------------------------------------------------------
st.subheader("🧾 Revenue Breakdown via Policy Type")

pol_tbl_show = policy_tbl_f[["Policy", "DCB", "Bundle", "Postpaid_Jazzcash", "Total", "Share_topline"]].copy()
pol_tbl_show = pol_tbl_show.sort_values("Total", ascending=False)

st.dataframe(
    format_table(
        pol_tbl_show.rename(
            columns={
                "Postpaid_Jazzcash": "Postpaid + Jazzcash",
                "Share_topline": "Share (%)",
            }
        ),
        money_cols=["DCB", "Bundle", "Postpaid + Jazzcash", "Total"],
        pct_cols=["Share (%)"],
    ),
    use_container_width=True,
    hide_index=True,
)

# Policy graph (stacked-style via multi-series bar chart)
pol_chart = pol_tbl_show.set_index("Policy")[["DCB", "Bundle", "Postpaid_Jazzcash"]].copy()
pol_chart.columns = ["DCB", "Bundle", "Postpaid + Jazzcash"]
pol_bar = pol_tbl_show[["Policy", "Total"]].copy()
pol_bar["TotalLabel"] = pol_bar["Total"].apply(abbr_pkr)

fig_pol = px.bar(
    pol_bar,
    x="Policy",
    y="Total",
    text="TotalLabel",
)
fig_pol.update_traces(textposition="outside")
fig_pol.update_layout(yaxis_title="Revenue (PKR)", xaxis_title="")
st.plotly_chart(fig_pol, use_container_width=True)



# ------------------------------------------------------------
# Partner-wise Breakdown (keep table; remove claims ratio insight)
st.subheader("🏥 Partner-wise Breakdown")

pt = partner_tbl_f.copy().sort_values("Total", ascending=False)

# Remove ALL claims-related columns
cols_to_show = ["Partner", "Prepaid", "Bundle", "Postpaid", "Jazzcash", "Total", "Share_topline"]
cols_to_show = [c for c in cols_to_show if c in pt.columns]

pt_show = pt[cols_to_show].copy()

st.dataframe(
    format_table(
        pt_show.rename(columns={"Share_topline": "Share (%)"}),
        money_cols=["Prepaid", "Bundle", "Postpaid", "Jazzcash", "Total"],
        pct_cols=["Share (%)"],
    ),
    use_container_width=True,
    hide_index=True,
)

# ------------------------------------------------------------
# Revenue by Channel Type (renamed + moved lower)
# ------------------------------------------------------------
st.subheader("📌 Revenue by Channel Type")

src_tbl = topline[topline["Channel"].astype(str).str.strip().str.lower().ne("ytd total")][
    ["Channel", "Revenue_PKR", "Share_topline"]
].copy().sort_values("Revenue_PKR", ascending=False)

st.dataframe(
    format_table(
        src_tbl.rename(columns={"Revenue_PKR": "Revenue (PKR)", "Share_topline": "Share (%)"}),
        money_cols=["Revenue (PKR)"],
        pct_cols=["Share (%)"],
    ),
    use_container_width=True,
    hide_index=True,
)

# ------------------------------------------------------------
# Bundles: ONLY top performing bundles
# ------------------------------------------------------------
st.subheader("🎁 Top Performing Bundles (BIMA + WebDoc)")

show = top_bundles_overall.rename(columns={"Revenue": "Revenue (PKR)"}).copy()
st.dataframe(
    format_table(show, money_cols=["Revenue (PKR)"]),
    use_container_width=True,
    hide_index=True,
)

with st.expander("Notes / Assumptions", expanded=False):
    st.write("• 'Net' = Revenue − Cost.")
    st.write("• Cost is taken from the totals row in Cost breakdown.xlsx (to avoid double counting).")
    st.write("• Quarterly averages are the average monthly revenue within each quarter (mean of 3 months).")


st.subheader("💼 Bundle Revenue by Partner (WebDoc vs BIMA)")

bund_partner_rev = (
    bund.groupby("Partner", as_index=False)["Revenue"]
    .sum()
    .sort_values("Revenue", ascending=False)
)

# Optional: standardize partner casing
bund_partner_rev["Partner"] = bund_partner_rev["Partner"].replace({"WebDoc": "WebDoc", "BIMA": "BIMA"})


show_partner = bund_partner_rev.rename(columns={"Revenue": "Revenue (PKR)"}).copy()
st.dataframe(
    format_table(show_partner, money_cols=["Revenue (PKR)"]),
    use_container_width=True,
    hide_index=True,
)

print_mode = st.sidebar.toggle("📄 Print / PDF mode", value=False)


COMPLAINTS_FILE_PATH = "Complaints for 2025.xlsx"

@st.cache_data
def load_complaints():
    return pd.read_excel(COMPLAINTS_FILE_PATH)

complaints_raw = load_complaints()

# ------------------------------------------------------------
# FILTER: ONLY COMPLAINTS (exclude info, activation, deactivation)
# ------------------------------------------------------------
complaints = complaints_raw.copy()

# Ensure DESCRIPTION exists and is string
complaints["DESCRIPTION"] = complaints["DESCRIPTION"].astype(str).fillna("")

def is_complaint(desc: str) -> bool:
    parts = [p.strip() for p in desc.split("-")]
    # We need: <something> - Complaints - <something>
    if len(parts) >= 2:
        return parts[1].lower() in ["complaints", "complaint"]
    return False

complaints = complaints[complaints["DESCRIPTION"].apply(is_complaint)].copy()

# --- Exclude FikrFree complaints completely
complaints = complaints[
    ~complaints["Product"].astype(str).str.contains("fikr", case=False, na=False)
    & ~complaints["DESCRIPTION"].astype(str).str.contains("fikr", case=False, na=False)
].copy()


MONTH_COLS = [
    "Jan_25", "Feb_25", "Mar_25", "Apr_25", "May_25", "Jun_25",
    "Jul_25", "Aug_25", "Sep_25", "Oct_25", "Nov_25", "Dec_25"
]

complaints_long = complaints.melt(
    id_vars=["Product"],
    value_vars=MONTH_COLS,
    var_name="Month",
    value_name="Complaints"
)

complaints_long["Complaints"] = pd.to_numeric(complaints_long["Complaints"], errors="coerce").fillna(0)

# --- Month cleanup + enforce correct order (Jan -> Dec)
MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
month_order = {m: i for i, m in enumerate(MONTHS, start=1)}

complaints_long["Month"] = complaints_long["Month"].astype(str).str.replace("_25", "", regex=False).str.strip()
complaints_long["Month_num"] = complaints_long["Month"].map(month_order)

# Safety: drop any rows where month couldn't be mapped (if any weird columns exist)
complaints_long = complaints_long.dropna(subset=["Month_num"]).copy()
complaints_long["Month_num"] = complaints_long["Month_num"].astype(int)

# Sort once here so everything downstream stays ordered
complaints_long = complaints_long.sort_values("Month_num")



st.subheader("📅 Complaints – Month by Month")

st.subheader("📅 Complaints – Month by Month")

complaints_mom = (
    complaints_long
    .groupby(["Month", "Month_num"], as_index=False)["Complaints"]
    .sum()
    .sort_values("Month_num")
    .drop(columns=["Month_num"])
)

st.dataframe(complaints_mom, use_container_width=True, hide_index=True)

# Ensure correct month order (you already have MONTHS list)
complaints_mom["Month"] = pd.Categorical(complaints_mom["Month"], categories=MONTHS, ordered=True)
complaints_mom = complaints_mom.sort_values("Month")

# --- Line chart with labels
fig_line = go.Figure()
fig_line.add_trace(
    go.Scatter(
        x=complaints_mom["Month"],
        y=complaints_mom["Complaints"],
        mode="lines+markers+text",
        text=[abbr_k(v) for v in complaints_mom["Complaints"]],
        textposition="top center",
        name="Complaints",
    )
)
fig_line.update_layout(
    xaxis_title="Month",
    yaxis_title="Complaints",
    margin=dict(l=10, r=10, t=20, b=10),
)
st.plotly_chart(fig_line, use_container_width=True)

# --- Bar chart with labels
complaints_mom_bar = complaints_mom.copy()
complaints_mom_bar["Label"] = complaints_mom_bar["Complaints"].apply(abbr_k)

fig_bar = px.bar(
    complaints_mom_bar,
    x="Month",
    y="Complaints",
    text="Label"
)
fig_bar.update_traces(textposition="outside")
fig_bar.update_layout(
    xaxis_title="Month",
    yaxis_title="Complaints",
    margin=dict(l=10, r=10, t=20, b=10),
)
st.plotly_chart(fig_bar, use_container_width=True)

complaints_mom_chart = (
    complaints_long
    .groupby(["Month", "Month_num"], as_index=False)["Complaints"]
    .sum()
    .sort_values("Month_num")
)
complaints_mom_chart["Month"] = pd.Categorical(complaints_mom_chart["Month"], categories=MONTHS, ordered=True)

st.line_chart(complaints_mom_chart.set_index("Month")[["Complaints"]])


st.subheader("🏥 Complaints by Partner (YTD)")

complaints_partner = (
    complaints_long
    .groupby("Product", as_index=False)["Complaints"]
    .sum()
    .sort_values("Complaints", ascending=False)
)

st.dataframe(
    complaints_partner.rename(columns={"Product": "Partner"}),
    use_container_width=True,
    hide_index=True
)

complaints_partner_plot = complaints_partner.rename(columns={"Product": "Partner"}).copy()
complaints_partner_plot["Label"] = complaints_partner_plot["Complaints"].apply(abbr_k)

fig_partner = px.bar(
    complaints_partner_plot.sort_values("Complaints", ascending=False),
    x="Partner",
    y="Complaints",
    text="Label"
)
fig_partner.update_traces(textposition="outside")
fig_partner.update_layout(
    xaxis_title="Partner",
    yaxis_title="Complaints",
    margin=dict(l=10, r=10, t=20, b=10),
)
st.plotly_chart(fig_partner, use_container_width=True)


st.subheader("📊 Complaints – Partner × Month")

complaints_pivot = (
    complaints_long
    .pivot_table(
        index="Product",
        columns="Month",
        values="Complaints",
        aggfunc="sum",
        fill_value=0
    )
    .reset_index()
)

st.dataframe(
    complaints_pivot.rename(columns={"Product": "Partner"}),
    use_container_width=True
)
