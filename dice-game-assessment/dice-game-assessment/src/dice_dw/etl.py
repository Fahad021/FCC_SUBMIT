
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

RAW = Path(__file__).resolve().parents[2] / "data" / "raw"
WH  = Path(__file__).resolve().parents[2] / "data" / "warehouse"

def read_csv(name: str) -> pd.DataFrame:
    return pd.read_csv(RAW / name)

def build_dim_date(start="2023-12-01", end="2025-12-31") -> pd.DataFrame:
    s = pd.to_datetime(start).date()
    e = pd.to_datetime(end).date()
    days = (e - s).days + 1
    dates = pd.date_range(s, periods=days, freq="D")
    df = pd.DataFrame({
        "date_key": dates.strftime("%Y%m%d").astype(int),
        "date": dates.date,
        "year": dates.year,
        "quarter": dates.quarter,
        "month": dates.month,
        "day": dates.day,
        "day_of_week": dates.dayofweek + 1,  # 1=Mon
        "week_of_year": dates.isocalendar().week.astype(int),
        "is_weekend": dates.weekday>=5
    })
    return df

def to_date_key(d):
    if pd.isna(d): 
        return np.nan
    return int(pd.to_datetime(d).strftime("%Y%m%d"))

def to_ts(x):
    return pd.to_datetime(x, errors="coerce")

def build_dimensions():
    # Plan and payment frequency
    plan = read_csv("plan.csv")
    ppf  = read_csv("plan_payment_frequency.csv").rename(columns={"payment_frequency_code":"payment_frequency_code",
                                                                  "english_description":"payment_frequency_desc_en",
                                                                  "french_description":"payment_frequency_desc_fr"})
    dim_plan = plan.merge(ppf, on="payment_frequency_code", how="left")
    dim_plan = dim_plan[["plan_id","payment_frequency_code","payment_frequency_desc_en","cost_amount"]].copy()
    # Users (combine user and registration for richer info)
    user = read_csv("user.csv")
    reg  = read_csv("user_registration.csv")
    dim_user = reg.merge(user, on="user_id", how="left", suffixes=("_reg",""))
    dim_user = dim_user[["user_id","user_registration_id","username","email_reg","first_name","last_name","ip_address","social_media_handle","email"]]
    dim_user = dim_user.rename(columns={"email_reg":"registration_email"})
    # Channels
    ch = read_csv("channel_code.csv").rename(columns={"play_session_channel_code":"channel_code",
                                                      "english_description":"channel_desc_en",
                                                      "french_description":"channel_desc_fr"})
    dim_channel = ch[["channel_code","channel_desc_en"]].copy()
    # Status
    st = read_csv("status_code.csv").rename(columns={"play_session_status_code":"status_code",
                                                     "english_description":"status_desc_en",
                                                     "french_description":"status_desc_fr"})
    dim_status = st[["status_code","status_desc_en"]].copy()
    # Date
    dim_date = build_dim_date()
    return dim_plan, dim_user, dim_channel, dim_status, dim_date

def build_facts(dim_channel, dim_status):
    # Play sessions
    s = read_csv("user_play_session.csv").copy()
    s["start_ts"] = to_ts(s["start_datetime"])
    s["end_ts"]   = to_ts(s["end_datetime"])
    s["duration_seconds"] = (s["end_ts"] - s["start_ts"]).dt.total_seconds().astype("float")
    s["date_key"] = s["start_ts"].dt.strftime("%Y%m%d").astype(int)
    fact_play_session = s.merge(dim_channel, on="channel_code", how="left") \
                         .merge(dim_status, on="status_code", how="left")
    fact_play_session = fact_play_session[[
        "play_session_id","user_id","date_key","start_ts","end_ts","duration_seconds",
        "channel_code","status_code","total_score"
    ]]
    # User plans (subscriptions/one-time)
    up = read_csv("user_plan.csv").copy()
    up["start_date"] = pd.to_datetime(up["start_date"], errors="coerce").dt.date
    up["end_date"]   = pd.to_datetime(up["end_date"], errors="coerce").dt.date
    up["start_date_key"] = up["start_date"].map(lambda d: int(pd.to_datetime(d).strftime("%Y%m%d")) if pd.notna(d) else np.nan)
    up["end_date_key"]   = up["end_date"].map(lambda d: int(pd.to_datetime(d).strftime("%Y%m%d")) if pd.notna(d) else np.nan)
    fact_user_plan = up
    return fact_play_session, fact_user_plan

def estimate_revenue_2024():
    # Revenue derived from plan cost and active periods in 2024
    up = read_csv("user_plan.csv").copy()
    plan = read_csv("plan.csv").copy()
    up["start"] = pd.to_datetime(up["start_date"], errors="coerce").dt.date
    up["end"]   = pd.to_datetime(up["end_date"], errors="coerce").dt.date
    # bound within 2024
    period_start = pd.to_datetime("2024-01-01").date()
    period_end   = pd.to_datetime("2024-12-31").date()
    up["active_start"] = up["start"].map(lambda d: max(d, period_start) if pd.notna(d) else None)
    up["active_end"]   = up["end"].map(lambda d: min(d, period_end)   if pd.notna(d) else None)
    df = up.merge(plan, on="plan_id", how="left")
    def cycles(row):
        if pd.isna(row["active_start"]) or pd.isna(row["active_end"]) or row["active_start"]>row["active_end"]:
            return 0
        if row["payment_frequency_code"]=="MONTHLY":
            # count number of month boundaries inclusive
            start = pd.to_datetime(row["active_start"])
            end   = pd.to_datetime(row["active_end"])
            r = relativedelta(end, start)
            months = r.years*12 + r.months + 1  # inclusive of start month
            return months
        if row["payment_frequency_code"]=="ANNUALLY":
            start = pd.to_datetime(row["active_start"])
            end   = pd.to_datetime(row["active_end"])
            r = relativedelta(end, start)
            years = r.years + 1  # inclusive
            return years
        if row["payment_frequency_code"]=="ONETIME":
            return 1
        return 0
    df["cycles_2024"] = df.apply(cycles, axis=1)
    df["revenue_2024"] = df["cycles_2024"] * df["cost_amount"]
    return df[["user_registration_id","plan_id","payment_frequency_code","cycles_2024","revenue_2024"]]

def run_pipeline():
    dim_plan, dim_user, dim_channel, dim_status, dim_date = build_dimensions()
    fact_play_session, fact_user_plan = build_facts(dim_channel, dim_status)

    WH.mkdir(parents=True, exist_ok=True)
    dim_plan.to_csv(WH/"dim_plan.csv", index=False)
    dim_user.to_csv(WH/"dim_user.csv", index=False)
    dim_channel.to_csv(WH/"dim_channel.csv", index=False)
    dim_status.to_csv(WH/"dim_status.csv", index=False)
    dim_date.to_csv(WH/"dim_date.csv", index=False)
    fact_play_session.to_csv(WH/"fact_play_session.csv", index=False)
    fact_user_plan.to_csv(WH/"fact_user_plan.csv", index=False)

    # Also CSVs for easy viewing
    for name, df in {
        "dim_plan.csv": dim_plan,
        "dim_user.csv": dim_user,
        "dim_channel.csv": dim_channel,
        "dim_status.csv": dim_status,
        "dim_date.csv": dim_date,
        "fact_play_session.csv": fact_play_session,
        "fact_user_plan.csv": fact_user_plan,
    }.items():
        df.to_csv(WH/name, index=False)

    # Insights
    rev = estimate_revenue_2024()
    rev.to_csv(WH/"revenue_2024_by_subscription.csv", index=False)
    return {
        "dim_plan": len(dim_plan),
        "dim_user": len(dim_user),
        "dim_channel": len(dim_channel),
        "dim_status": len(dim_status),
        "dim_date": len(dim_date),
        "fact_play_session": len(fact_play_session),
        "fact_user_plan": len(fact_user_plan),
        "revenue_rows": len(rev)
    }

if __name__ == "__main__":
    stats = run_pipeline()
    print(stats)
