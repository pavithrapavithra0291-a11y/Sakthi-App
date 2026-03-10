import os
import time
import urllib.parse
import pandas as pd
import numpy as np
import plotly.graph_objects as go

from dash import Dash, html, dcc, Input, Output, dash_table
import dash_bootstrap_components as dbc

try:
    from databricks import sql as dbsql
    DBSQL_AVAILABLE = True
except Exception:
    dbsql = None
    DBSQL_AVAILABLE = False

# =========================================================
# APP
# =========================================================
app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
)
server = app.server
app.title = "SakthiNex OS"

# =========================================================
# THEME
# =========================================================
ORANGE = "#E36A38"
BG = "#F6F7F9"
TEXT = "#111827"
MUTED = "#6B7280"
BORDER = "#E5E7EB"
WHITE = "#FFFFFF"
CARD_SHADOW = "0 4px 12px rgba(0,0,0,0.04)"

DARK_BG = "#111827"
DARK_CARD = "#1F2937"
DARK_TEXT = "#F9FAFB"
DARK_MUTED = "#D1D5DB"
DARK_BORDER = "#374151"

SUGAR_COLOR = "#E36A38"
FINANCE_COLOR = "#2563EB"
ABT_COLOR = "#10B981"


def theme_colors(theme_mode: str):
    if theme_mode == "dark":
        return {
            "bg": DARK_BG,
            "card": DARK_CARD,
            "text": DARK_TEXT,
            "muted": DARK_MUTED,
            "border": DARK_BORDER,
            "sidebar": "#0F172A",
            "sidebar_card": "#1F2937",
            "button_inactive": "#374151",
            "table_header": "#374151",
            "grid": "#374151",
            "paper": DARK_CARD,
            "plot": DARK_CARD,
        }

    return {
        "bg": BG,
        "card": WHITE,
        "text": TEXT,
        "muted": MUTED,
        "border": BORDER,
        "sidebar": WHITE,
        "sidebar_card": "#F9FAFB",
        "button_inactive": "#F3F4F6",
        "table_header": "#F3F4F6",
        "grid": "#E5E7EB",
        "paper": WHITE,
        "plot": WHITE,
    }


# =========================================================
# ENV
# =========================================================
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "").strip()
DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "").strip()
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "").strip()

SUGAR_TABLE = "workspace.public.sugar_data"
FINANCE_TABLE = "workspace.public.finance_data"
ABT_TABLE = "workspace.public.abt_data"

# =========================================================
# CACHE
# =========================================================
CACHE_TTL_SECONDS = 3600  # 1 hour

_data_cache = {
    "sugars_ceo": {"ts": 0, "df": pd.DataFrame()},
    "finance_ceo": {"ts": 0, "df": pd.DataFrame()},
    "abt_ceo": {"ts": 0, "df": pd.DataFrame()},
}

# =========================================================
# ROUTES
# =========================================================
ROUTE_TO_VIEW = {
    "/group": "supreme_ceo",
    "/sugars": "sugars_ceo",
    "/finance": "finance_ceo",
    "/abt": "abt_ceo",
}
VIEW_TO_ROUTE = {v: k for k, v in ROUTE_TO_VIEW.items()}

# =========================================================
# PERSONAS
# =========================================================
PERSONAS = {
    "supreme_ceo": {
        "name": "Supreme CEO",
        "role": "Group Chairman",
        "permissions": ["Enterprise", "Sugars", "Finance", "ABT"],
        "title": "Group Enterprise Dashboard",
        "subtitle": "Cross Sector Overview",
    },
    "sugars_ceo": {
        "name": "Sakthi Sugars",
        "role": "Sector CEO",
        "permissions": ["Sugars"],
        "title": "Sakthi Sugars Operation Console",
        "subtitle": "Manufacturing KPIs",
    },
    "finance_ceo": {
        "name": "Sakthi Finance",
        "role": "Sector CEO",
        "permissions": ["Finance"],
        "title": "Sakthi Finance Command Center",
        "subtitle": "Financial KPIs",
    },
    "abt_ceo": {
        "name": "ABT Logistics",
        "role": "Sector CEO",
        "permissions": ["ABT"],
        "title": "ABT Fleet Dashboard",
        "subtitle": "Transport KPIs",
    },
}

# =========================================================
# HELPERS
# =========================================================
def logo(height=45):
    return html.Img(
        src=app.get_asset_url("logo.png"),
        style={"height": f"{height}px", "width": "auto"},
    )


def format_inr_cr(value):
    try:
        return f"₹{float(value):,.0f} Cr"
    except Exception:
        return "₹0 Cr"


def format_inr(value):
    try:
        return f"₹{float(value):,.0f}"
    except Exception:
        return "₹0"


def format_pct(value):
    try:
        return f"{float(value):.1f}%"
    except Exception:
        return "0.0%"


def numeric_value(val, default=0.0):
    try:
        if val is None:
            return default
        if isinstance(val, str):
            cleaned = val.replace(",", "").replace("%", "").replace("₹", "").replace("Cr", "").strip()
            return float(cleaned)
        return float(val)
    except Exception:
        return default


def parse_user_from_search(search: str):
    if not search:
        return None, "light"

    query = urllib.parse.parse_qs(search.lstrip("?"))
    user = query.get("user", [None])[0]
    theme_mode = query.get("theme", ["light"])[0]

    if theme_mode not in ["light", "dark"]:
        theme_mode = "light"

    if user in PERSONAS:
        return user, theme_mode

    return None, theme_mode


def build_href(path: str, user: str, theme_mode: str = "light"):
    return f"{path}?user={user}&theme={theme_mode}"


def db_ready():
    return all([
        DBSQL_AVAILABLE,
        bool(DATABRICKS_HOST),
        bool(DATABRICKS_HTTP_PATH),
        bool(DATABRICKS_TOKEN),
    ])


def db_connection():
    host = DATABRICKS_HOST.replace("https://", "").replace("http://", "").strip("/")
    return dbsql.connect(
        server_hostname=host,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )


def query_df(query: str) -> pd.DataFrame:
    if not db_ready():
        print("Databricks connection variables missing or connector unavailable")
        return pd.DataFrame()

    try:
        with db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                cols = [c[0] for c in cursor.description] if cursor.description else []
                return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        print(f"SQL query failed: {e}")
        return pd.DataFrame()


def get_recent_rows_from_db(table_name: str, limit: int = 1000) -> pd.DataFrame:
    if table_name == SUGAR_TABLE:
        return query_df(f"""
            SELECT *
            FROM {table_name}
            ORDER BY
                COALESCE(CAST(report_date AS TIMESTAMP), TIMESTAMP('1900-01-01')) DESC,
                COALESCE(year, 0) DESC,
                COALESCE(month, 0) DESC
            LIMIT {limit}
        """)

    if table_name == FINANCE_TABLE:
        return query_df(f"""
            SELECT *
            FROM {table_name}
            ORDER BY
                COALESCE(CAST(disbursement_date AS TIMESTAMP), TIMESTAMP('1900-01-01')) DESC
            LIMIT {limit}
        """)

    if table_name == ABT_TABLE:
        return query_df(f"""
            SELECT *
            FROM {table_name}
            ORDER BY
                COALESCE(CAST(period AS TIMESTAMP), TIMESTAMP('1900-01-01')) DESC
            LIMIT {limit}
        """)

    return query_df(f"SELECT * FROM {table_name} LIMIT {limit}")


def get_domain_df(view: str) -> pd.DataFrame:
    if view == "sugars_ceo":
        return get_recent_rows_from_db(SUGAR_TABLE, 1000)
    if view == "finance_ceo":
        return get_recent_rows_from_db(FINANCE_TABLE, 1000)
    if view == "abt_ceo":
        return get_recent_rows_from_db(ABT_TABLE, 1000)
    return pd.DataFrame()


def get_cached_domain_df(view: str) -> pd.DataFrame:
    now = time.time()
    cached = _data_cache.get(view)

    if cached and (now - cached["ts"] < CACHE_TTL_SECONDS):
        return cached["df"].copy()

    df = get_domain_df(view)
    _data_cache[view] = {"ts": now, "df": df.copy()}
    return df.copy()


def get_domain_live_status(view: str) -> bool:
    df = get_cached_domain_df(view)
    return not df.empty


def empty_prepared_sugar():
    monthly = pd.DataFrame(columns=[
        "month_label",
        "recovery_pct",
        "crushing_capacity_utilization_pct",
        "ebitda",
        "ebitda_per_ton",
        "avg_realization_per_quintal"
    ])
    return {
        "latest": {
            "Recovery %": 0,
            "Crushing Capacity Utilization %": 0,
            "EBITDA": 0,
            "EBITDA per Ton": 0,
            "Average Realization per Quintal": 0,
        },
        "monthly": monthly,
        "raw": pd.DataFrame(),
        "is_live": False
    }


def empty_prepared_finance():
    monthly = pd.DataFrame(columns=[
        "month_label",
        "aum",
        "aum_growth_pct",
        "disbursement_volume",
        "gross_npa_pct",
        "collection_efficiency_pct"
    ])
    return {
        "latest": {
            "Assets Under Management (AUM)": 0,
            "AUM Growth %": 0,
            "Disbursement Volume": 0,
            "Gross NPA %": 0,
            "Collection Efficiency %": 0,
        },
        "monthly": monthly,
        "raw": pd.DataFrame(),
        "is_live": False
    }


def empty_prepared_abt():
    monthly = pd.DataFrame(columns=[
        "month_label",
        "total_revenue",
        "revenue_growth_pct",
        "fleet_utilization_pct",
        "revenue_per_vehicle",
        "on_time_delivery_pct"
    ])
    return {
        "latest": {
            "Total Revenue": 0,
            "Revenue Growth %": 0,
            "Fleet Utilization %": 0,
            "Revenue per Vehicle": 0,
            "On-Time Delivery %": 0,
        },
        "monthly": monthly,
        "raw": pd.DataFrame(),
        "is_live": False
    }


# =========================================================
# DOMAIN PREPARATION
# =========================================================
def prepare_sugar_data():
    df = get_cached_domain_df("sugars_ceo").copy()

    if df.empty:
        return empty_prepared_sugar()

    if "recovery_pct" in df.columns and "crushing_capacity_utilization_pct" in df.columns:
        monthly = df.copy()

        if "report_date" in monthly.columns:
            monthly["report_date"] = pd.to_datetime(monthly["report_date"], errors="coerce")
            monthly = monthly.sort_values("report_date")
        elif "year" in monthly.columns and "month" in monthly.columns:
            monthly = monthly.sort_values(["year", "month"])

        latest = monthly.iloc[-1]
        return {
            "latest": {
                "Recovery %": numeric_value(latest.get("recovery_pct", 0)),
                "Crushing Capacity Utilization %": numeric_value(latest.get("crushing_capacity_utilization_pct", 0)),
                "EBITDA": numeric_value(latest.get("ebitda", 0)),
                "EBITDA per Ton": numeric_value(latest.get("ebitda_per_ton", 0)),
                "Average Realization per Quintal": numeric_value(latest.get("avg_realization_per_quintal", 0)),
            },
            "monthly": monthly,
            "raw": df,
            "is_live": True
        }

    if "report_date" in df.columns:
        df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
        df["month_label"] = df["report_date"].dt.strftime("%b %Y")
        df["sort_date"] = df["report_date"].dt.to_period("M").dt.to_timestamp()
    elif "month" in df.columns and "year" in df.columns:
        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
        }
        if df["month"].dtype == object:
            df["month_num"] = df["month"].astype(str).str.strip().str[:3].str.lower().map(month_map)
        else:
            df["month_num"] = pd.to_numeric(df["month"], errors="coerce")
        df["year_num"] = pd.to_numeric(df["year"], errors="coerce")
        month_name_map = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }
        df["month_label"] = df["month_num"].map(month_name_map).fillna("Unknown") + " " + df["year_num"].fillna(0).astype(int).astype(str)
        df["sort_date"] = pd.to_datetime(
            dict(year=df["year_num"].fillna(1900).astype(int), month=df["month_num"].fillna(1).astype(int), day=1),
            errors="coerce"
        )
    else:
        df["month_label"] = df.index.astype(str)
        df["sort_date"] = pd.NaT

    required_cols = {
        "cane_crushed_tons": 0,
        "sugar_produced_tons": 0,
        "installed_crushing_capacity": 0,
        "actual_crushing_capacity": 0,
        "ebitda": 0,
        "sugar_sold_quintal": 0,
        "sugar_sales_revenue": 0,
    }
    for col, default in required_cols.items():
        if col not in df.columns:
            df[col] = default

    monthly = (
        df.groupby("month_label", as_index=False)
        .agg({
            "cane_crushed_tons": "sum",
            "sugar_produced_tons": "sum",
            "installed_crushing_capacity": "sum",
            "actual_crushing_capacity": "sum",
            "ebitda": "sum",
            "sugar_sold_quintal": "sum",
            "sugar_sales_revenue": "sum",
            "sort_date": "max",
        })
        .sort_values("sort_date", na_position="last")
        .reset_index(drop=True)
    )

    monthly["recovery_pct"] = np.where(
        monthly["cane_crushed_tons"] > 0,
        (monthly["sugar_produced_tons"] / monthly["cane_crushed_tons"]) * 100,
        0,
    )
    monthly["crushing_capacity_utilization_pct"] = np.where(
        monthly["installed_crushing_capacity"] > 0,
        (monthly["actual_crushing_capacity"] / monthly["installed_crushing_capacity"]) * 100,
        0,
    )
    monthly["ebitda_per_ton"] = np.where(
        monthly["cane_crushed_tons"] > 0,
        monthly["ebitda"] / monthly["cane_crushed_tons"],
        0,
    )
    monthly["avg_realization_per_quintal"] = np.where(
        monthly["sugar_sold_quintal"] > 0,
        monthly["sugar_sales_revenue"] / monthly["sugar_sold_quintal"],
        0,
    )

    latest = monthly.iloc[-1] if not monthly.empty else {}

    return {
        "latest": {
            "Recovery %": numeric_value(latest.get("recovery_pct", 0)),
            "Crushing Capacity Utilization %": numeric_value(latest.get("crushing_capacity_utilization_pct", 0)),
            "EBITDA": numeric_value(latest.get("ebitda", 0)),
            "EBITDA per Ton": numeric_value(latest.get("ebitda_per_ton", 0)),
            "Average Realization per Quintal": numeric_value(latest.get("avg_realization_per_quintal", 0)),
        },
        "monthly": monthly,
        "raw": df,
        "is_live": True
    }


def prepare_finance_data():
    df = get_cached_domain_df("finance_ceo").copy()

    if df.empty:
        return empty_prepared_finance()

    if "aum_growth_pct" in df.columns and "disbursement_volume" in df.columns:
        monthly = df.copy()

        if "disbursement_date" in monthly.columns:
            monthly["disbursement_date"] = pd.to_datetime(monthly["disbursement_date"], errors="coerce")
            monthly = monthly.sort_values("disbursement_date")
        elif "month" in monthly.columns and "year" in monthly.columns:
            monthly = monthly.sort_values(["year", "month"])

        latest = monthly.iloc[-1]
        return {
            "latest": {
                "Assets Under Management (AUM)": numeric_value(latest.get("aum", 0)),
                "AUM Growth %": numeric_value(latest.get("aum_growth_pct", 0)),
                "Disbursement Volume": numeric_value(latest.get("disbursement_volume", 0)),
                "Gross NPA %": numeric_value(latest.get("gross_npa_pct", 0)),
                "Collection Efficiency %": numeric_value(latest.get("collection_efficiency_pct", 0)),
            },
            "monthly": monthly,
            "raw": df,
            "is_live": True
        }

    if "disbursement_date" in df.columns:
        df["disbursement_date"] = pd.to_datetime(df["disbursement_date"], errors="coerce")
        df["month_label"] = df["disbursement_date"].dt.strftime("%b %Y")
        df["sort_date"] = df["disbursement_date"].dt.to_period("M").dt.to_timestamp()
    else:
        df["month_label"] = df.index.astype(str)
        df["sort_date"] = pd.NaT

    for col in [
        "outstanding_principal",
        "accrued_interest",
        "principal_due",
        "interest_due",
        "principal_paid",
        "interest_paid",
        "loan_amount_disbursed",
        "gross_npa_amount"
    ]:
        if col not in df.columns:
            df[col] = 0

    df["aum_value"] = df["outstanding_principal"].fillna(0) + df["accrued_interest"].fillna(0)
    df["collection_due"] = df["principal_due"].fillna(0) + df["interest_due"].fillna(0)
    df["collection_paid"] = df["principal_paid"].fillna(0) + df["interest_paid"].fillna(0)

    monthly = (
        df.groupby("month_label", as_index=False)
        .agg({
            "aum_value": "sum",
            "loan_amount_disbursed": "sum",
            "gross_npa_amount": "sum",
            "collection_due": "sum",
            "collection_paid": "sum",
            "sort_date": "max",
        })
        .sort_values("sort_date", na_position="last")
        .reset_index(drop=True)
    )

    monthly["aum_growth_pct"] = monthly["aum_value"].pct_change().fillna(0) * 100
    monthly["gross_npa_pct"] = np.where(
        monthly["aum_value"] > 0,
        (monthly["gross_npa_amount"] / monthly["aum_value"]) * 100,
        0,
    )
    monthly["collection_efficiency_pct"] = np.where(
        monthly["collection_due"] > 0,
        (monthly["collection_paid"] / monthly["collection_due"]) * 100,
        0,
    )
    monthly.rename(columns={
        "aum_value": "aum",
        "loan_amount_disbursed": "disbursement_volume"
    }, inplace=True)

    latest = monthly.iloc[-1] if not monthly.empty else {}

    return {
        "latest": {
            "Assets Under Management (AUM)": numeric_value(latest.get("aum", 0)),
            "AUM Growth %": numeric_value(latest.get("aum_growth_pct", 0)),
            "Disbursement Volume": numeric_value(latest.get("disbursement_volume", 0)),
            "Gross NPA %": numeric_value(latest.get("gross_npa_pct", 0)),
            "Collection Efficiency %": numeric_value(latest.get("collection_efficiency_pct", 0)),
        },
        "monthly": monthly,
        "raw": df,
        "is_live": True
    }


def prepare_abt_data():
    df = get_cached_domain_df("abt_ceo").copy()

    if df.empty:
        return empty_prepared_abt()

    # Robust ABT mapping for your uploaded dataset columns
    if "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"], errors="coerce")
        df["month_label"] = df["period"].dt.strftime("%b %Y")
        df["sort_date"] = df["period"].dt.to_period("M").dt.to_timestamp()
    elif "month" in df.columns and "year" in df.columns:
        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
        }
        if df["month"].dtype == object:
            df["month_num"] = df["month"].astype(str).str.strip().str[:3].str.lower().map(month_map)
        else:
            df["month_num"] = pd.to_numeric(df["month"], errors="coerce")
        df["year_num"] = pd.to_numeric(df["year"], errors="coerce")
        month_name_map = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }
        df["month_label"] = df["month_num"].map(month_name_map).fillna("Unknown") + " " + df["year_num"].fillna(0).astype(int).astype(str)
        df["sort_date"] = pd.to_datetime(
            dict(year=df["year_num"].fillna(1900).astype(int), month=df["month_num"].fillna(1).astype(int), day=1),
            errors="coerce"
        )
    else:
        df["month_label"] = df.index.astype(str)
        df["sort_date"] = pd.NaT

    required_cols = {
        "total_revenue": 0,
        "revenue_growth_pct": np.nan,
        "fleet_utilization_pct": np.nan,
        "revenue_per_vehicle": np.nan,
        "on_time_delivery_pct": np.nan,
        "active_vehicle_hours": 0,
        "available_vehicle_hours": 0,
        "deliveries_on_time": 0,
        "total_deliveries": 0,
    }
    for col, default in required_cols.items():
        if col not in df.columns:
            df[col] = default

    if "vehicle_id" not in df.columns:
        df["vehicle_id"] = df.index.astype(str)

    monthly = (
        df.groupby("month_label", as_index=False)
        .agg({
            "total_revenue": "sum",
            "revenue_growth_pct": "mean",
            "fleet_utilization_pct": "mean",
            "revenue_per_vehicle": "mean",
            "on_time_delivery_pct": "mean",
            "active_vehicle_hours": "sum",
            "available_vehicle_hours": "sum",
            "deliveries_on_time": "sum",
            "total_deliveries": "sum",
            "vehicle_id": pd.Series.nunique,
            "sort_date": "max",
        })
        .sort_values("sort_date", na_position="last")
        .reset_index(drop=True)
    )

    # Backfill KPI formulas only if the precomputed columns are missing
    if monthly["revenue_growth_pct"].isna().all():
        monthly["revenue_growth_pct"] = monthly["total_revenue"].pct_change().fillna(0) * 100
    else:
        monthly["revenue_growth_pct"] = monthly["revenue_growth_pct"].fillna(0)

    if monthly["fleet_utilization_pct"].isna().all():
        monthly["fleet_utilization_pct"] = np.where(
            monthly["available_vehicle_hours"] > 0,
            (monthly["active_vehicle_hours"] / monthly["available_vehicle_hours"]) * 100,
            0,
        )
    else:
        monthly["fleet_utilization_pct"] = monthly["fleet_utilization_pct"].fillna(0)

    if monthly["revenue_per_vehicle"].isna().all():
        monthly["revenue_per_vehicle"] = np.where(
            monthly["vehicle_id"] > 0,
            monthly["total_revenue"] / monthly["vehicle_id"],
            0,
        )
    else:
        monthly["revenue_per_vehicle"] = monthly["revenue_per_vehicle"].fillna(0)

    if monthly["on_time_delivery_pct"].isna().all():
        monthly["on_time_delivery_pct"] = np.where(
            monthly["total_deliveries"] > 0,
            (monthly["deliveries_on_time"] / monthly["total_deliveries"]) * 100,
            0,
        )
    else:
        monthly["on_time_delivery_pct"] = monthly["on_time_delivery_pct"].fillna(0)

    latest = monthly.iloc[-1] if not monthly.empty else {}

    return {
        "latest": {
            "Total Revenue": numeric_value(latest.get("total_revenue", 0)),
            "Revenue Growth %": numeric_value(latest.get("revenue_growth_pct", 0)),
            "Fleet Utilization %": numeric_value(latest.get("fleet_utilization_pct", 0)),
            "Revenue per Vehicle": numeric_value(latest.get("revenue_per_vehicle", 0)),
            "On-Time Delivery %": numeric_value(latest.get("on_time_delivery_pct", 0)),
        },
        "monthly": monthly,
        "raw": df,
        "is_live": True
    }


# =========================================================
# KPI BUILDERS
# =========================================================
def build_sugar_metrics():
    prepared = prepare_sugar_data()
    latest = prepared["latest"]
    live_tag = "Live" if prepared["is_live"] else "No Live Data"
    return [
        {"label": "Recovery %", "value": format_pct(latest["Recovery %"]), "trend": live_tag, "positive": prepared["is_live"]},
        {"label": "Crushing Capacity Utilization %", "value": format_pct(latest["Crushing Capacity Utilization %"]), "trend": live_tag, "positive": prepared["is_live"]},
        {"label": "EBITDA", "value": format_inr_cr(latest["EBITDA"]), "trend": live_tag, "positive": prepared["is_live"]},
        {"label": "EBITDA per Ton", "value": format_inr(latest["EBITDA per Ton"]), "trend": live_tag, "positive": prepared["is_live"]},
        {"label": "Average Realization per Quintal", "value": format_inr(latest["Average Realization per Quintal"]), "trend": live_tag, "positive": prepared["is_live"]},
    ]


def build_finance_metrics():
    prepared = prepare_finance_data()
    latest = prepared["latest"]
    live_tag = "Live" if prepared["is_live"] else "No Live Data"
    return [
        {"label": "Assets Under Management (AUM)", "value": format_inr_cr(latest["Assets Under Management (AUM)"]), "trend": live_tag, "positive": prepared["is_live"]},
        {"label": "AUM Growth %", "value": format_pct(latest["AUM Growth %"]), "trend": live_tag, "positive": prepared["is_live"]},
        {"label": "Disbursement Volume", "value": format_inr_cr(latest["Disbursement Volume"]), "trend": live_tag, "positive": prepared["is_live"]},
        {"label": "Gross NPA %", "value": format_pct(latest["Gross NPA %"]), "trend": live_tag, "positive": prepared["is_live"]},
        {"label": "Collection Efficiency %", "value": format_pct(latest["Collection Efficiency %"]), "trend": live_tag, "positive": prepared["is_live"]},
    ]


def build_abt_metrics():
    prepared = prepare_abt_data()
    latest = prepared["latest"]
    live_tag = "Live" if prepared["is_live"] else "No Live Data"
    return [
        {"label": "Total Revenue", "value": format_inr_cr(latest["Total Revenue"]), "trend": live_tag, "positive": prepared["is_live"]},
        {"label": "Revenue Growth %", "value": format_pct(latest["Revenue Growth %"]), "trend": live_tag, "positive": prepared["is_live"]},
        {"label": "Fleet Utilization %", "value": format_pct(latest["Fleet Utilization %"]), "trend": live_tag, "positive": prepared["is_live"]},
        {"label": "Revenue per Vehicle", "value": format_inr(latest["Revenue per Vehicle"]), "trend": live_tag, "positive": prepared["is_live"]},
        {"label": "On-Time Delivery %", "value": format_pct(latest["On-Time Delivery %"]), "trend": live_tag, "positive": prepared["is_live"]},
    ]


def build_group_metrics():
    sugar = prepare_sugar_data()
    finance = prepare_finance_data()
    abt = prepare_abt_data()

    sugar_latest = sugar["latest"]
    finance_latest = finance["latest"]
    abt_latest = abt["latest"]

    total_revenue = numeric_value(sugar_latest["EBITDA"]) + numeric_value(abt_latest["Total Revenue"])
    workforce = 15420
    all_live = sugar["is_live"] or finance["is_live"] or abt["is_live"]
    live_tag = "Live" if all_live else "No Live Data"

    return [
        {
            "label": "Total Revenue",
            "value": f"₹{int(total_revenue):,} Cr",
            "trend": live_tag,
            "positive": all_live
        },
        {
            "label": "Group EBITDA",
            "value": f"₹{int(numeric_value(sugar_latest['EBITDA'])):,} Cr",
            "trend": live_tag,
            "positive": all_live
        },
        {
            "label": "Total Workforce",
            "value": f"{workforce:,}",
            "trend": "+120",
            "positive": True
        },
        {
            "label": "Finance AUM",
            "value": f"₹{int(numeric_value(finance_latest['Assets Under Management (AUM)'])):,} Cr",
            "trend": live_tag,
            "positive": all_live
        },
    ]


def build_metrics(view):
    if view == "supreme_ceo":
        return build_group_metrics()
    if view == "sugars_ceo":
        return build_sugar_metrics()
    if view == "finance_ceo":
        return build_finance_metrics()
    if view == "abt_ceo":
        return build_abt_metrics()
    return []


def get_group_df():
    sugar = build_sugar_metrics()
    finance = build_finance_metrics()
    abt = build_abt_metrics()
    return pd.DataFrame([
        {"Domain": "Sakthi Sugars", "Metric": "EBITDA", "Value": sugar[2]["value"]},
        {"Domain": "Sakthi Finance", "Metric": "AUM", "Value": finance[0]["value"]},
        {"Domain": "ABT Logistics", "Metric": "Total Revenue", "Value": abt[0]["value"]},
    ])


def get_recent_df(view):
    if view == "supreme_ceo":
        return get_group_df()
    return get_cached_domain_df(view)


def get_detail_cards(view):
    if view == "sugars_ceo":
        return [
            {"title": "Production Insight", "value": "Recovery and crushing capacity utilization are now the primary CEO operations KPIs", "note": "Charts show month trends and factory-level comparisons where available"},
            {"title": "Commercial Insight", "value": "EBITDA, EBITDA per Ton and Average Realization per Quintal are aligned to sugar CEO view", "note": "Refreshes automatically from source data"},
        ]
    if view == "finance_ceo":
        return [
            {"title": "Portfolio Health", "value": "AUM, AUM Growth %, Gross NPA % and Collection Efficiency % are now the main CEO KPIs", "note": "Charts show month-wise portfolio movement and quality"},
            {"title": "Disbursement Insight", "value": "Disbursement Volume is included as a direct growth KPI", "note": "Refreshes automatically from source data"},
        ]
    if view == "abt_ceo":
        return [
            {"title": "Revenue Insight", "value": "Total Revenue, Revenue Growth % and Revenue per Vehicle are the main commercial KPIs", "note": "Charts show trend and vehicle/route efficiency"},
            {"title": "Service Insight", "value": "Fleet Utilization % and On-Time Delivery % are now the core operational KPIs", "note": "Refreshes automatically from source data"},
        ]
    return []


# =========================================================
# CHART HELPERS
# =========================================================
def apply_chart_theme(fig, theme_mode="light"):
    colors = theme_colors(theme_mode)
    fig.update_layout(
        paper_bgcolor=colors["paper"],
        plot_bgcolor=colors["plot"],
        font=dict(color=colors["text"], family="Arial"),
        margin=dict(l=50, r=20, t=60, b=50),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(color=colors["text"])
        ),
        hovermode="x unified",
    )
    fig.update_xaxes(
        showgrid=False,
        linecolor=colors["border"],
        tickfont=dict(color=colors["text"])
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor=colors["grid"],
        zeroline=False,
        tickfont=dict(color=colors["text"])
    )
    return fig


def chart_card(title, fig, theme_mode="light"):
    colors = theme_colors(theme_mode)
    return dbc.Card(
        dbc.CardBody([
            html.Div(
                title,
                style={
                    "fontSize": "18px",
                    "fontWeight": "800",
                    "color": colors["text"],
                    "marginBottom": "8px"
                }
            ),
            dcc.Graph(
                figure=fig,
                config={
                    "displayModeBar": True,
                    "scrollZoom": True,
                    "displaylogo": False,
                    "responsive": True,
                },
                style={"height": "330px"}
            )
        ]),
        style={
            "border": f"1px solid {colors['border']}",
            "borderRadius": "14px",
            "backgroundColor": colors["card"],
            "boxShadow": CARD_SHADOW
        }
    )


def empty_chart(title, x_label, y_label, theme_mode="light"):
    fig = go.Figure()
    fig.update_layout(title=title)
    fig.update_xaxes(title=x_label)
    fig.update_yaxes(title=y_label)
    return chart_card(title, apply_chart_theme(fig, theme_mode), theme_mode)


# =========================================================
# VISUALS
# =========================================================
def sugar_visuals(theme_mode="light"):
    prepared = prepare_sugar_data()
    monthly = prepared["monthly"].copy()
    raw = prepared["raw"].copy()

    if monthly.empty:
        return [
            empty_chart("Recovery % Trend", "X-axis: Month", "Y-axis: Recovery %", theme_mode),
            empty_chart("Crushing Capacity Utilization %", "X-axis: Month", "Y-axis: Crushing Capacity Utilization %", theme_mode),
            empty_chart("EBITDA Trend", "X-axis: Month", "Y-axis: EBITDA (₹ Cr)", theme_mode),
            empty_chart("EBITDA per Ton", "X-axis: Month", "Y-axis: EBITDA per Ton (₹)", theme_mode),
            empty_chart("Average Realization per Quintal", "X-axis: Month", "Y-axis: Average Realization per Quintal (₹)", theme_mode),
        ]

    charts = []
    xvals = monthly["month_label"]

    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(
        x=xvals, y=monthly["recovery_pct"], mode="lines+markers",
        line=dict(color=SUGAR_COLOR, width=3), name="Recovery %"
    ))
    fig1.update_layout(title="Recovery % Trend")
    fig1.update_xaxes(title="X-axis: Month")
    fig1.update_yaxes(title="Y-axis: Recovery %")
    charts.append(chart_card("Recovery % Trend", apply_chart_theme(fig1, theme_mode), theme_mode))

    if {"factory_name", "installed_crushing_capacity", "actual_crushing_capacity"}.issubset(raw.columns) and not raw.empty:
        fac = raw.groupby("factory_name", as_index=False).agg({
            "installed_crushing_capacity": "sum",
            "actual_crushing_capacity": "sum"
        })
        fac["crushing_capacity_utilization_pct"] = np.where(
            fac["installed_crushing_capacity"] > 0,
            (fac["actual_crushing_capacity"] / fac["installed_crushing_capacity"]) * 100,
            0,
        )
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=fac["factory_name"], y=fac["crushing_capacity_utilization_pct"],
            marker_color=SUGAR_COLOR, name="Capacity Utilization %"
        ))
        fig2.update_layout(title="Crushing Capacity Utilization by Factory")
        fig2.update_xaxes(title="X-axis: Factory")
        fig2.update_yaxes(title="Y-axis: Crushing Capacity Utilization %")
    else:
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=xvals, y=monthly["crushing_capacity_utilization_pct"],
            marker_color=SUGAR_COLOR, name="Capacity Utilization %"
        ))
        fig2.update_layout(title="Crushing Capacity Utilization Trend")
        fig2.update_xaxes(title="X-axis: Month")
        fig2.update_yaxes(title="Y-axis: Crushing Capacity Utilization %")
    charts.append(chart_card("Crushing Capacity Utilization %", apply_chart_theme(fig2, theme_mode), theme_mode))

    fig3 = go.Figure()
    fig3.add_trace(go.Bar(
        x=xvals, y=monthly["ebitda"], marker_color=SUGAR_COLOR, name="EBITDA"
    ))
    fig3.update_layout(title="EBITDA Trend")
    fig3.update_xaxes(title="X-axis: Month")
    fig3.update_yaxes(title="Y-axis: EBITDA (₹ Cr)")
    charts.append(chart_card("EBITDA Trend", apply_chart_theme(fig3, theme_mode), theme_mode))

    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(
        x=xvals, y=monthly["ebitda_per_ton"], mode="lines+markers",
        line=dict(color=SUGAR_COLOR, width=3), name="EBITDA per Ton"
    ))
    fig4.update_layout(title="EBITDA per Ton Trend")
    fig4.update_xaxes(title="X-axis: Month")
    fig4.update_yaxes(title="Y-axis: EBITDA per Ton (₹)")
    charts.append(chart_card("EBITDA per Ton", apply_chart_theme(fig4, theme_mode), theme_mode))

    fig5 = go.Figure()
    fig5.add_trace(go.Scatter(
        x=xvals, y=monthly["avg_realization_per_quintal"], mode="lines+markers",
        line=dict(color=SUGAR_COLOR, width=3), fill="tozeroy", name="Average Realization"
    ))
    fig5.update_layout(title="Average Realization per Quintal Trend")
    fig5.update_xaxes(title="X-axis: Month")
    fig5.update_yaxes(title="Y-axis: Average Realization per Quintal (₹)")
    charts.append(chart_card("Average Realization per Quintal", apply_chart_theme(fig5, theme_mode), theme_mode))

    return charts


def finance_visuals(theme_mode="light"):
    prepared = prepare_finance_data()
    monthly = prepared["monthly"].copy()
    raw = prepared["raw"].copy()

    if monthly.empty:
        return [
            empty_chart("Assets Under Management (AUM)", "X-axis: Month", "Y-axis: AUM (₹ Cr)", theme_mode),
            empty_chart("AUM Growth %", "X-axis: Month", "Y-axis: AUM Growth %", theme_mode),
            empty_chart("Disbursement Volume", "X-axis: Month", "Y-axis: Disbursement Volume (₹)", theme_mode),
            empty_chart("Gross NPA %", "X-axis: Month", "Y-axis: Gross NPA %", theme_mode),
            empty_chart("Collection Efficiency %", "X-axis: Month", "Y-axis: Collection Efficiency %", theme_mode),
        ]

    charts = []
    xvals = monthly["month_label"]

    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(
        x=xvals, y=monthly["aum"], mode="lines+markers",
        line=dict(color=FINANCE_COLOR, width=3), fill="tozeroy", name="AUM"
    ))
    fig1.update_layout(title="Assets Under Management (AUM) Trend")
    fig1.update_xaxes(title="X-axis: Month")
    fig1.update_yaxes(title="Y-axis: AUM (₹ Cr)")
    charts.append(chart_card("Assets Under Management (AUM)", apply_chart_theme(fig1, theme_mode), theme_mode))

    fig2 = go.Figure()
    fig2.add_trace(go.Bar(
        x=xvals, y=monthly["aum_growth_pct"], marker_color=FINANCE_COLOR, name="AUM Growth %"
    ))
    fig2.update_layout(title="AUM Growth % Trend")
    fig2.update_xaxes(title="X-axis: Month")
    fig2.update_yaxes(title="Y-axis: AUM Growth %")
    charts.append(chart_card("AUM Growth %", apply_chart_theme(fig2, theme_mode), theme_mode))

    if {"product_type", "loan_amount_disbursed"}.issubset(raw.columns) and not raw.empty:
        prod = (
            raw.groupby("product_type", as_index=False)["loan_amount_disbursed"]
            .sum()
            .sort_values("loan_amount_disbursed", ascending=False)
            .head(10)
        )
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=prod["product_type"], y=prod["loan_amount_disbursed"],
            marker_color=FINANCE_COLOR, name="Disbursement Volume"
        ))
        fig3.update_layout(title="Disbursement Volume by Product")
        fig3.update_xaxes(title="X-axis: Product Type")
        fig3.update_yaxes(title="Y-axis: Disbursement Volume (₹)")
    else:
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=xvals, y=monthly["disbursement_volume"],
            marker_color=FINANCE_COLOR, name="Disbursement Volume"
        ))
        fig3.update_layout(title="Disbursement Volume Trend")
        fig3.update_xaxes(title="X-axis: Month")
        fig3.update_yaxes(title="Y-axis: Disbursement Volume (₹)")
    charts.append(chart_card("Disbursement Volume", apply_chart_theme(fig3, theme_mode), theme_mode))

    if {"region", "gross_npa_amount", "outstanding_principal", "accrued_interest"}.issubset(raw.columns) and not raw.empty:
        raw["aum_calc"] = raw["outstanding_principal"].fillna(0) + raw["accrued_interest"].fillna(0)
        reg = raw.groupby("region", as_index=False).agg({
            "gross_npa_amount": "sum",
            "aum_calc": "sum"
        })
        reg["gross_npa_pct"] = np.where(reg["aum_calc"] > 0, (reg["gross_npa_amount"] / reg["aum_calc"]) * 100, 0)
        fig4 = go.Figure()
        fig4.add_trace(go.Bar(
            x=reg["region"], y=reg["gross_npa_pct"],
            marker_color=FINANCE_COLOR, name="Gross NPA %"
        ))
        fig4.update_layout(title="Gross NPA % by Region")
        fig4.update_xaxes(title="X-axis: Region")
        fig4.update_yaxes(title="Y-axis: Gross NPA %")
    else:
        fig4 = go.Figure()
        fig4.add_trace(go.Scatter(
            x=xvals, y=monthly["gross_npa_pct"], mode="lines+markers",
            line=dict(color=FINANCE_COLOR, width=3), name="Gross NPA %"
        ))
        fig4.update_layout(title="Gross NPA % Trend")
        fig4.update_xaxes(title="X-axis: Month")
        fig4.update_yaxes(title="Y-axis: Gross NPA %")
    charts.append(chart_card("Gross NPA %", apply_chart_theme(fig4, theme_mode), theme_mode))

    if {"branch_name", "principal_due", "interest_due", "principal_paid", "interest_paid"}.issubset(raw.columns) and not raw.empty:
        br = raw.copy()
        br["due_amt"] = br["principal_due"].fillna(0) + br["interest_due"].fillna(0)
        br["paid_amt"] = br["principal_paid"].fillna(0) + br["interest_paid"].fillna(0)
        br = br.groupby("branch_name", as_index=False).agg({
            "due_amt": "sum",
            "paid_amt": "sum"
        })
        br["collection_efficiency_pct"] = np.where(br["due_amt"] > 0, (br["paid_amt"] / br["due_amt"]) * 100, 0)
        br = br.sort_values("collection_efficiency_pct", ascending=False).head(10)
        fig5 = go.Figure()
        fig5.add_trace(go.Bar(
            x=br["branch_name"], y=br["collection_efficiency_pct"],
            marker_color=FINANCE_COLOR, name="Collection Efficiency %"
        ))
        fig5.update_layout(title="Collection Efficiency % by Branch")
        fig5.update_xaxes(title="X-axis: Branch")
        fig5.update_yaxes(title="Y-axis: Collection Efficiency %")
    else:
        fig5 = go.Figure()
        fig5.add_trace(go.Scatter(
            x=xvals, y=monthly["collection_efficiency_pct"], mode="lines+markers",
            line=dict(color=FINANCE_COLOR, width=3), name="Collection Efficiency %"
        ))
        fig5.update_layout(title="Collection Efficiency % Trend")
        fig5.update_xaxes(title="X-axis: Month")
        fig5.update_yaxes(title="Y-axis: Collection Efficiency %")
    charts.append(chart_card("Collection Efficiency %", apply_chart_theme(fig5, theme_mode), theme_mode))

    return charts


def abt_visuals(theme_mode="light"):
    prepared = prepare_abt_data()
    monthly = prepared["monthly"].copy()
    raw = prepared["raw"].copy()

    if monthly.empty:
        return [
            empty_chart("Total Revenue", "X-axis: Month", "Y-axis: Total Revenue (₹ Cr)", theme_mode),
            empty_chart("Revenue Growth %", "X-axis: Month", "Y-axis: Revenue Growth %", theme_mode),
            empty_chart("Fleet Utilization %", "X-axis: Month", "Y-axis: Fleet Utilization %", theme_mode),
            empty_chart("Revenue per Vehicle", "X-axis: Month", "Y-axis: Revenue per Vehicle (₹)", theme_mode),
            empty_chart("On-Time Delivery %", "X-axis: Month", "Y-axis: On-Time Delivery %", theme_mode),
        ]

    charts = []
    xvals = monthly["month_label"]

    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(
        x=xvals, y=monthly["total_revenue"], mode="lines+markers",
        line=dict(color=ABT_COLOR, width=3), fill="tozeroy", name="Total Revenue"
    ))
    fig1.update_layout(title="Total Revenue Trend")
    fig1.update_xaxes(title="X-axis: Month")
    fig1.update_yaxes(title="Y-axis: Total Revenue (₹ Cr)")
    charts.append(chart_card("Total Revenue", apply_chart_theme(fig1, theme_mode), theme_mode))

    fig2 = go.Figure()
    fig2.add_trace(go.Bar(
        x=xvals, y=monthly["revenue_growth_pct"], marker_color=ABT_COLOR, name="Revenue Growth %"
    ))
    fig2.update_layout(title="Revenue Growth % Trend")
    fig2.update_xaxes(title="X-axis: Month")
    fig2.update_yaxes(title="Y-axis: Revenue Growth %")
    charts.append(chart_card("Revenue Growth %", apply_chart_theme(fig2, theme_mode), theme_mode))

    if {"route_id", "active_vehicle_hours", "available_vehicle_hours"}.issubset(raw.columns) and not raw.empty:
        rt = raw.groupby("route_id", as_index=False).agg({
            "active_vehicle_hours": "sum",
            "available_vehicle_hours": "sum"
        })
        rt["fleet_utilization_pct"] = np.where(
            rt["available_vehicle_hours"] > 0,
            (rt["active_vehicle_hours"] / rt["available_vehicle_hours"]) * 100,
            0
        )
        rt = rt.sort_values("fleet_utilization_pct", ascending=False).head(12)
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=rt["route_id"].astype(str), y=rt["fleet_utilization_pct"],
            marker_color=ABT_COLOR, name="Fleet Utilization %"
        ))
        fig3.update_layout(title="Fleet Utilization % by Route")
        fig3.update_xaxes(title="X-axis: Route ID")
        fig3.update_yaxes(title="Y-axis: Fleet Utilization %")
    else:
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(
            x=xvals, y=monthly["fleet_utilization_pct"], mode="lines+markers",
            line=dict(color=ABT_COLOR, width=3), name="Fleet Utilization %"
        ))
        fig3.update_layout(title="Fleet Utilization % Trend")
        fig3.update_xaxes(title="X-axis: Month")
        fig3.update_yaxes(title="Y-axis: Fleet Utilization %")
    charts.append(chart_card("Fleet Utilization %", apply_chart_theme(fig3, theme_mode), theme_mode))

    if {"vehicle_id", "total_revenue"}.issubset(raw.columns) and not raw.empty:
        vh = raw.groupby("vehicle_id", as_index=False)["total_revenue"].sum().sort_values("total_revenue", ascending=False).head(12)
        fig4 = go.Figure()
        fig4.add_trace(go.Bar(
            x=vh["vehicle_id"].astype(str), y=vh["total_revenue"],
            marker_color=ABT_COLOR, name="Revenue per Vehicle"
        ))
        fig4.update_layout(title="Revenue per Vehicle - Top Vehicles")
        fig4.update_xaxes(title="X-axis: Vehicle ID")
        fig4.update_yaxes(title="Y-axis: Revenue per Vehicle (₹)")
    else:
        fig4 = go.Figure()
        fig4.add_trace(go.Scatter(
            x=xvals, y=monthly["revenue_per_vehicle"], mode="lines+markers",
            line=dict(color=ABT_COLOR, width=3), name="Revenue per Vehicle"
        ))
        fig4.update_layout(title="Revenue per Vehicle Trend")
        fig4.update_xaxes(title="X-axis: Month")
        fig4.update_yaxes(title="Y-axis: Revenue per Vehicle (₹)")
    charts.append(chart_card("Revenue per Vehicle", apply_chart_theme(fig4, theme_mode), theme_mode))

    if {"route_id", "deliveries_on_time", "total_deliveries"}.issubset(raw.columns) and not raw.empty:
        rt2 = raw.groupby("route_id", as_index=False).agg({
            "deliveries_on_time": "sum",
            "total_deliveries": "sum"
        })
        rt2["on_time_delivery_pct"] = np.where(
            rt2["total_deliveries"] > 0,
            (rt2["deliveries_on_time"] / rt2["total_deliveries"]) * 100,
            0
        )
        rt2 = rt2.sort_values("on_time_delivery_pct", ascending=False).head(12)
        fig5 = go.Figure()
        fig5.add_trace(go.Bar(
            x=rt2["route_id"].astype(str), y=rt2["on_time_delivery_pct"],
            marker_color=ABT_COLOR, name="On-Time Delivery %"
        ))
        fig5.update_layout(title="On-Time Delivery % by Route")
        fig5.update_xaxes(title="X-axis: Route ID")
        fig5.update_yaxes(title="Y-axis: On-Time Delivery %")
    else:
        fig5 = go.Figure()
        fig5.add_trace(go.Scatter(
            x=xvals, y=monthly["on_time_delivery_pct"], mode="lines+markers",
            line=dict(color=ABT_COLOR, width=3), name="On-Time Delivery %"
        ))
        fig5.update_layout(title="On-Time Delivery % Trend")
        fig5.update_xaxes(title="X-axis: Month")
        fig5.update_yaxes(title="Y-axis: On-Time Delivery %")
    charts.append(chart_card("On-Time Delivery %", apply_chart_theme(fig5, theme_mode), theme_mode))

    return charts


def group_visuals(theme_mode="light"):
    sugar = prepare_sugar_data()["latest"]
    finance = prepare_finance_data()["latest"]
    abt = prepare_abt_data()["latest"]

    comp = pd.DataFrame([
        {"Domain": "Sugars", "Value": numeric_value(sugar["EBITDA"])},
        {"Domain": "Finance", "Value": numeric_value(finance["Assets Under Management (AUM)"])},
        {"Domain": "ABT", "Value": numeric_value(abt["Total Revenue"])},
    ])

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=comp["Domain"],
        y=comp["Value"],
        marker_color=[SUGAR_COLOR, FINANCE_COLOR, ABT_COLOR],
        name="Latest KPI Value"
    ))
    fig.update_layout(title="Group Comparison")
    fig.update_xaxes(title="X-axis: Domain")
    fig.update_yaxes(title="Y-axis: Latest KPI Value")
    return [chart_card("Group Comparison", apply_chart_theme(fig, theme_mode), theme_mode)]


def get_visual_section(view, theme_mode="light"):
    if view == "sugars_ceo":
        cards = sugar_visuals(theme_mode)
    elif view == "finance_ceo":
        cards = finance_visuals(theme_mode)
    elif view == "abt_ceo":
        cards = abt_visuals(theme_mode)
    else:
        cards = group_visuals(theme_mode)

    return dbc.Row([
        dbc.Col(card, lg=6, className="mb-4") for card in cards
    ])


# =========================================================
# UI COMPONENTS
# =========================================================
def kpi_card(label, value, trend, positive, theme_mode="light"):
    colors = theme_colors(theme_mode)
    color = "#16A34A" if positive else "#DC2626"

    return dbc.Card(
        dbc.CardBody([
            html.Div(label, style={
                "fontSize": "12px",
                "color": colors["muted"],
                "fontWeight": "700",
                "textTransform": "uppercase",
                "letterSpacing": "0.04em"
            }),
            html.H3(value, style={
                "marginTop": "10px",
                "color": colors["text"],
                "fontWeight": "800"
            }),
            html.Div(trend, style={"color": color, "fontWeight": "700"}),
        ]),
        style={
            "border": f"1px solid {colors['border']}",
            "borderRadius": "14px",
            "backgroundColor": colors["card"],
            "boxShadow": CARD_SHADOW
        }
    )


def summary_card(title, body, theme_mode="light"):
    colors = theme_colors(theme_mode)

    return dbc.Card(
        dbc.CardBody([
            html.Div(title, style={
                "fontSize": "20px",
                "fontWeight": "800",
                "marginBottom": "14px",
                "color": colors["text"]
            }),
            html.Div(body, style={
                "fontSize": "15px",
                "lineHeight": "1.7",
                "color": colors["muted"]
            }),
        ]),
        style={
            "border": f"1px solid {colors['border']}",
            "borderRadius": "14px",
            "backgroundColor": colors["card"],
            "boxShadow": CARD_SHADOW
        }
    )


def nav_link(label, href, active=False, theme_mode="light"):
    colors = theme_colors(theme_mode)

    return dcc.Link(
        label,
        href=href,
        refresh=False,
        style={
            "display": "block",
            "width": "100%",
            "padding": "12px 14px",
            "marginBottom": "10px",
            "background": ORANGE if active else colors["button_inactive"],
            "color": "white" if active else colors["text"],
            "borderRadius": "10px",
            "fontWeight": "700",
            "textAlign": "left",
            "textDecoration": "none",
            "cursor": "pointer"
        }
    )


def login_card(label, href):
    return dcc.Link(
        [
            html.Div(label, style={"fontSize": "20px", "fontWeight": "700", "color": "#1F2937"}),
            html.Div("Click to continue", style={"fontSize": "14px", "color": MUTED, "marginTop": "4px"})
        ],
        href=href,
        refresh=False,
        style={
            "display": "block",
            "width": "100%",
            "padding": "18px 20px",
            "marginBottom": "16px",
            "background": "#F8F8F9",
            "border": f"1px solid {BORDER}",
            "borderRadius": "14px",
            "textAlign": "left",
            "cursor": "pointer",
            "textDecoration": "none"
        }
    )


def table_card(title, df, theme_mode="light"):
    colors = theme_colors(theme_mode)

    if df is None or df.empty:
        df = pd.DataFrame([{"Status": "No live data found"}])

    columns = [{"name": c, "id": c} for c in df.columns]
    data = df.to_dict("records")

    return dbc.Card(
        dbc.CardBody([
            html.Div(title, style={
                "fontSize": "20px",
                "fontWeight": "800",
                "color": colors["text"],
                "marginBottom": "14px"
            }),
            dash_table.DataTable(
                data=data,
                columns=columns,
                page_size=20,
                filter_action="native",
                sort_action="native",
                sort_mode="multi",
                page_action="native",
                style_table={"overflowX": "auto"},
                style_cell={
                    "padding": "10px",
                    "fontFamily": "Arial",
                    "fontSize": "14px",
                    "border": "none",
                    "textAlign": "left",
                    "maxWidth": "220px",
                    "overflow": "hidden",
                    "textOverflow": "ellipsis",
                    "backgroundColor": colors["card"],
                    "color": colors["text"],
                },
                style_header={
                    "fontWeight": "700",
                    "backgroundColor": colors["table_header"],
                    "color": colors["text"],
                    "border": "none",
                },
                style_data={
                    "backgroundColor": colors["card"],
                    "color": colors["text"],
                    "border": "none"
                },
            )
        ]),
        style={
            "border": f"1px solid {colors['border']}",
            "borderRadius": "14px",
            "backgroundColor": colors["card"],
            "boxShadow": CARD_SHADOW
        }
    )


def detail_section(view, theme_mode="light"):
    cards = get_detail_cards(view)
    if not cards:
        return html.Div()

    return dbc.Row([
        dbc.Col(
            summary_card(item["title"], f"{item['value']}. {item['note']}", theme_mode),
            lg=6,
            className="mb-4"
        )
        for item in cards
    ])


def top_theme_toggle(user, active_view, theme_mode="light"):
    colors = theme_colors(theme_mode)

    toggle_target = "dark" if theme_mode == "light" else "light"
    toggle_icon = "☾" if theme_mode == "light" else "☀"

    return dcc.Link(
        toggle_icon,
        href=build_href(VIEW_TO_ROUTE.get(active_view, "/group"), user, toggle_target),
        refresh=False,
        style={
            "position": "fixed",
            "top": "12px",
            "right": "16px",
            "width": "52px",
            "height": "52px",
            "borderRadius": "50%",
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "center",
            "textDecoration": "none",
            "fontSize": "26px",
            "color": colors["text"],
            "backgroundColor": "#F3F4F6" if theme_mode == "light" else colors["card"],
            "border": f"1px solid {colors['border']}",
            "boxShadow": "0 2px 8px rgba(0,0,0,0.10)",
            "zIndex": "9999",
        }
    )


# =========================================================
# PAGES
# =========================================================
def login_page():
    return html.Div(
        [
            html.Div(
                [
                    html.Div(
                        [logo(56), html.Span("SakthiNex OS", style={"fontSize": "26px", "fontWeight": "800", "color": "#273142", "marginLeft": "12px"})],
                        style={"display": "flex", "alignItems": "center", "marginBottom": "70px"},
                    ),
                    html.Div("Fueling the Future", style={"fontSize": "56px", "fontWeight": "800", "color": "#0F172A", "lineHeight": "1.05"}),
                    html.Div("with Tech", style={"fontSize": "56px", "fontWeight": "800", "color": ORANGE, "lineHeight": "1.05", "marginTop": "4px"}),
                    html.Div(
                        "The unified enterprise operating system. Track KPIs, manage sectors, and drive sustainable dominance.",
                        style={"fontSize": "18px", "color": MUTED, "lineHeight": "1.7", "marginTop": "26px", "maxWidth": "560px"}
                    ),
                ],
                style={"width": "50%", "padding": "50px 60px", "borderRight": f"1px solid {BORDER}", "backgroundColor": BG}
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.Div("Enterprise Portal", style={"fontSize": "34px", "fontWeight": "800", "color": "#273142", "textAlign": "center"}),
                            html.Div("Select your sector console to continue", style={"fontSize": "15px", "color": MUTED, "textAlign": "center", "marginTop": "8px", "marginBottom": "32px"}),
                            login_card("Supreme CEO", build_href("/group", "supreme_ceo", "light")),
                            login_card("Sakthi Sugars", build_href("/sugars", "sugars_ceo", "light")),
                            login_card("Sakthi Finance", build_href("/finance", "finance_ceo", "light")),
                            login_card("ABT Logistics", build_href("/abt", "abt_ceo", "light")),
                        ],
                        style={"width": "100%", "maxWidth": "520px", "padding": "36px", "backgroundColor": "#FAFAFB", "border": f"1px solid {BORDER}", "borderRadius": "22px", "boxShadow": "0 8px 24px rgba(0,0,0,0.06)"}
                    )
                ],
                style={"width": "50%", "display": "flex", "alignItems": "center", "justifyContent": "center", "padding": "36px", "backgroundColor": BG}
            )
        ],
        style={"display": "flex", "minHeight": "100vh", "backgroundColor": BG, "fontFamily": "Arial, sans-serif"}
    )


def sidebar(user, active_view, theme_mode="light"):
    persona = PERSONAS[user]
    colors = theme_colors(theme_mode)

    if "Enterprise" in persona["permissions"]:
        buttons = [
            nav_link("Group Overview", build_href("/group", user, theme_mode), active_view == "supreme_ceo", theme_mode),
            nav_link("Sakthi Sugars", build_href("/sugars", user, theme_mode), active_view == "sugars_ceo", theme_mode),
            nav_link("Sakthi Finance", build_href("/finance", user, theme_mode), active_view == "finance_ceo", theme_mode),
            nav_link("ABT Logistics", build_href("/abt", user, theme_mode), active_view == "abt_ceo", theme_mode),
        ]
    else:
        buttons = [nav_link(persona["name"], build_href(VIEW_TO_ROUTE[user], user, theme_mode), True, theme_mode)]

    return html.Div(
        [
            html.Div(
                [logo(44), html.Span("SakthiNex OS", style={"fontSize": "24px", "fontWeight": "800", "color": colors["text"], "marginLeft": "12px"})],
                style={"display": "flex", "alignItems": "center", "marginBottom": "28px"},
            ),
            html.Div("Console View", style={"fontSize": "11px", "fontWeight": "700", "letterSpacing": "0.08em", "textTransform": "uppercase", "color": colors["muted"], "marginBottom": "12px"}),
            html.Div(buttons),

            html.Div(style={"flex": "1"}),

            html.Div(
                [
                    html.Div(persona["name"], style={"fontSize": "16px", "fontWeight": "700", "color": colors["text"]}),
                    html.Div(persona["role"], style={"fontSize": "12px", "fontWeight": "700", "color": colors["muted"], "marginTop": "4px"}),
                ],
                style={"padding": "14px", "borderRadius": "12px", "backgroundColor": colors["sidebar_card"], "border": f"1px solid {colors['border']}", "marginBottom": "12px"}
            ),

            dcc.Link(
                "Logout",
                href="/",
                refresh=False,
                style={
                    "display": "block",
                    "width": "100%",
                    "padding": "12px",
                    "borderRadius": "10px",
                    "backgroundColor": "#FEF2F2",
                    "color": "#DC2626",
                    "fontWeight": "700",
                    "textAlign": "center",
                    "textDecoration": "none"
                }
            )
        ],
        style={"width": "280px", "minHeight": "100vh", "padding": "24px 18px", "borderRight": f"1px solid {colors['border']}", "backgroundColor": colors["sidebar"], "display": "flex", "flexDirection": "column"}
    )


def get_status_text(view: str) -> str:
    if view == "supreme_ceo":
        sugar_live = get_domain_live_status("sugars_ceo")
        finance_live = get_domain_live_status("finance_ceo")
        abt_live = get_domain_live_status("abt_ceo")
        if sugar_live or finance_live or abt_live:
            return "Connected to live Databricks tables"
        return "No live Databricks data found"

    return "Connected to live Databricks tables" if get_domain_live_status(view) else "No live Databricks data found"


def dashboard(view, theme_mode="light"):
    persona = PERSONAS[view]
    colors = theme_colors(theme_mode)
    metrics = build_metrics(view)
    recent_df = get_recent_df(view)
    db_status_text = get_status_text(view)

    if view == "supreme_ceo":
        cards = [dbc.Col(kpi_card(**m, theme_mode=theme_mode), md=6, lg=3, className="mb-4") for m in metrics]
        return html.Div([
            html.Div(persona["title"], style={"fontSize": "34px", "fontWeight": "800", "color": colors["text"]}),
            html.Div(persona["subtitle"], style={"fontSize": "13px", "fontWeight": "700", "color": colors["muted"], "marginBottom": "10px"}),
            html.Div(db_status_text, style={"fontSize": "13px", "color": ORANGE, "marginBottom": "18px"}),
            dbc.Row(cards),
            get_visual_section(view, theme_mode),
            dbc.Row([
                dbc.Col(table_card("Latest Group Table", recent_df, theme_mode), lg=8, className="mb-4"),
                dbc.Col(
                    summary_card(
                        "Executive Summary",
                        "Supreme CEO can monitor live values from Sugars, Finance, and ABT here and switch to each detailed dashboard from the left sidebar.",
                        theme_mode
                    ),
                    lg=4,
                    className="mb-4"
                )
            ])
        ])

    cards = [dbc.Col(kpi_card(**m, theme_mode=theme_mode), md=6, lg=4, className="mb-4") for m in metrics]
    return html.Div([
        html.Div(persona["title"], style={"fontSize": "34px", "fontWeight": "800", "color": colors["text"]}),
        html.Div(persona["subtitle"], style={"fontSize": "13px", "fontWeight": "700", "color": colors["muted"], "marginBottom": "10px"}),
        html.Div(db_status_text, style={"fontSize": "13px", "color": ORANGE, "marginBottom": "18px"}),
        dbc.Row(cards),
        detail_section(view, theme_mode),
        get_visual_section(view, theme_mode),
        dbc.Row([
            dbc.Col(table_card("Live KPI Table", recent_df, theme_mode), lg=8, className="mb-4"),
            dbc.Col(
                summary_card(
                    "Executive Summary",
                    f"This page shows the same KPI-style detailed view for {persona['name']}. Values refresh automatically every 1 hour.",
                    theme_mode
                ),
                lg=4,
                className="mb-4"
            )
        ])
    ])


# =========================================================
# LAYOUT
# =========================================================
app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    dcc.Interval(id="auto-refresh", interval=60 * 60 * 1000, n_intervals=0),
    html.Div(id="page")
])

# =========================================================
# RENDER
# =========================================================
@app.callback(
    Output("page", "children"),
    Input("url", "pathname"),
    Input("url", "search"),
    Input("auto-refresh", "n_intervals"),
)
def render_page(pathname, search, _n):
    user, theme_mode = parse_user_from_search(search)

    if pathname == "/":
        return login_page()

    if user is None:
        return login_page()

    view = ROUTE_TO_VIEW.get(pathname, "supreme_ceo")

    if user != "supreme_ceo":
        view = user

    colors = theme_colors(theme_mode)

    return html.Div(
        [
            sidebar(user, view, theme_mode),
            html.Div(
                dashboard(view, theme_mode),
                style={
                    "padding": "28px",
                    "flex": "1",
                    "backgroundColor": colors["bg"],
                    "minHeight": "100vh"
                }
            ),
            top_theme_toggle(user, view, theme_mode),
        ],
        style={
            "display": "flex",
            "fontFamily": "Arial, sans-serif",
            "backgroundColor": colors["bg"],
            "position": "relative"
        }
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
