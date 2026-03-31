import asyncio
import httpx
import os
import sys
import subprocess
import gspread
import json
from datetime import datetime, timedelta

from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

from meta_utils import norm_meta_graph_id, to_float_minor, to_hkd_from_meta_minor
from engine import GOOGLE_CREDENTIALS_PATH
from shop_mapping import SHOP_NAME_MAP

UNMAPPED_LABEL = "Unmapped"

# Single bulk fields string: ad list + targeting + nested creative + insights (no extra per-ad GETs)
def _ads_fields(insights_f: str) -> str:
    return (
        f"id,name,created_time,"
        f"adset{{id,name,daily_budget,optimization_goal,destination_type}},"
        f"campaign{{id,name,daily_budget,objective}},"
        f"creative{{id,actor_id,instagram_actor_id,body,effective_object_story_id}},"
        f"targeting,{insights_f}"
    )

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

load_dotenv()

_ROOT = os.path.dirname(os.path.abspath(__file__))


def _google_credentials_path() -> str:
    p = (GOOGLE_CREDENTIALS_PATH or "credentials.json").strip()
    return p if os.path.isabs(p) else os.path.join(_ROOT, p)


ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("AD_ACCOUNT_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "AdSurvivor_Report")
RAW_SHEET_TAB = os.getenv("RAW_SHEET_TAB", "Sheet1")
HTTP_TIMEOUT = 30.0
# Fewer pagination round-trips (same endpoint, not extra “chatty” pattern)
ADS_PAGE_LIMIT = 100


def get_raw_worksheet():
    try:
        cred_path = _google_credentials_path()
        if not os.path.isfile(cred_path):
            print(f"❌ Google 憑證檔不存在: {cred_path}")
            print("   請將 GCP 服務帳號 JSON 放到專案目錄並命名為 credentials.json，或在 .env 設定 GOOGLE_CREDENTIALS_PATH（可為絕對路徑）。")
            sys.exit(1)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(cred_path, scope)
        client = gspread.authorize(creds)
        ss = client.open(SHEET_NAME)
        try:
            return ss.worksheet(RAW_SHEET_TAB)
        except Exception:
            return ss.add_worksheet(title=RAW_SHEET_TAB, rows=2000, cols=40)
    except Exception as e:
        print(f"❌ Google Sheets 連接失敗: {e}")
        sys.exit(1)


def _norm_id(v) -> str:
    return norm_meta_graph_id(v)


def _normalize_creative_field(creative_raw) -> dict:
    if isinstance(creative_raw, str) and creative_raw.strip():
        return {"id": creative_raw.strip()}
    if isinstance(creative_raw, dict):
        return dict(creative_raw)
    return {}


def _merge_ads_for_richest_creative(*lists) -> dict[str, dict]:
    """Same ad id across four windows: merge creative (and fill empty targeting) without extra API calls."""
    merged: dict[str, dict] = {}

    def score(cr: dict) -> int:
        if not cr:
            return 0
        s = 0
        if cr.get("id"):
            s += 1
        if cr.get("actor_id"):
            s += 4
        if cr.get("instagram_actor_id"):
            s += 4
        if cr.get("body"):
            s += 1
        if cr.get("effective_object_story_id"):
            s += 1
        return s

    for lst in lists:
        for ad in lst:
            aid = _norm_id(ad.get("id"))
            if not aid:
                continue
            ad = dict(ad)
            ad["creative"] = _normalize_creative_field(ad.get("creative"))
            if aid not in merged:
                merged[aid] = ad
                continue
            old = merged[aid]
            cr_old = _normalize_creative_field(old.get("creative"))
            cr_new = _normalize_creative_field(ad.get("creative"))
            if score(cr_new) > score(cr_old):
                old["creative"] = cr_new
            else:
                old["creative"] = {**cr_old, **cr_new}
            if not old.get("targeting") and ad.get("targeting"):
                old["targeting"] = ad["targeting"]
    return merged


def _fb_page_name_local(actor_id: str, instagram_actor_id: str) -> str:
    return (
        SHOP_NAME_MAP.get(actor_id)
        or SHOP_NAME_MAP.get(instagram_actor_id)
        or UNMAPPED_LABEL
    )


def _build_time_ranges():
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    last_7d = (now - timedelta(days=6)).strftime("%Y-%m-%d")
    last_30d = (now - timedelta(days=29)).strftime("%Y-%m-%d")
    this_month = now.replace(day=1).strftime("%Y-%m-%d")
    return {
        "today": {"since": today, "until": today},
        "last_7d": {"since": last_7d, "until": today},
        "last_30d": {"since": last_30d, "until": today},
        "this_month": {"since": this_month, "until": today},
    }


async def get_range_raw_data(client, label: str, since: str, until: str):
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/ads"
    insights_f = f"insights.time_range({{'since':'{since}','until':'{until}'}}){{spend,cpc,clicks}}"
    params = {
        "fields": _ads_fields(insights_f),
        "filtering": json.dumps([{"field": "ad.effective_status", "operator": "IN", "value": ["ACTIVE"]}]),
        "access_token": ACCESS_TOKEN,
        "limit": ADS_PAGE_LIMIT,
    }
    all_ads = []
    current_url = url
    current_params = params
    while current_url:
        res = await client.get(current_url, params=current_params, timeout=HTTP_TIMEOUT)
        res.raise_for_status()
        data = res.json()
        batch_data = data.get("data", [])
        filtered_batch = []
        for ad in batch_data:
            ins_list = ad.get("insights", {}).get("data", [])
            ins = ins_list[0] if ins_list else {}
            try:
                spend = float(ins.get("spend", 0) or 0)
            except Exception:
                spend = 0.0
            if spend > 0:
                filtered_batch.append(ad)
        all_ads.extend(filtered_batch)
        current_url = data.get("paging", {}).get("next")
        current_params = None
        print(f"📦 {label}: 抓取 {len(all_ads)} 則 ACTIVE + spend>0 廣告")
    return all_ads


# Include PAUSED / PENDING_REVIEW so zero-spend & in-review ads reach Refined for P00 seed template selection.
_SUPPLEMENTAL_AD_EFFECTIVE_STATUSES = ["ACTIVE", "PAUSED", "PENDING_REVIEW", "PREAPPROVED"]


async def get_supplemental_ads_for_refined_seed(client, since: str, until: str) -> list:
    """
    Paginate ads with spend>0 gate **disabled** (and broader effective_status) so new seed templates
    and ``處理中`` ads still produce Raw/Refined rows. Merged after the four spend-weighted pulls.
    """
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/ads"
    insights_f = f"insights.time_range({{'since':'{since}','until':'{until}'}}){{spend,cpc,clicks}}"
    params = {
        "fields": _ads_fields(insights_f),
        "filtering": json.dumps(
            [{"field": "ad.effective_status", "operator": "IN", "value": _SUPPLEMENTAL_AD_EFFECTIVE_STATUSES}]
        ),
        "access_token": ACCESS_TOKEN,
        "limit": ADS_PAGE_LIMIT,
    }
    all_ads: list = []
    current_url = url
    current_params = params
    while current_url:
        res = await client.get(current_url, params=current_params, timeout=HTTP_TIMEOUT)
        res.raise_for_status()
        data = res.json()
        batch_data = data.get("data", [])
        for ad in batch_data:
            all_ads.append(ad)
        current_url = data.get("paging", {}).get("next")
        current_params = None
    print(
        f"📦 supplemental_seed: 抓取 {len(all_ads)} 則 "
        f"(effective_status in {len(_SUPPLEMENTAL_AD_EFFECTIVE_STATUSES)} states, 含0花費)"
    )
    return all_ads


async def get_account_min_budget_minor(client) -> tuple[float, str]:
    try:
        r = await client.get(
            f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}",
            params={"fields": "min_daily_budget,currency", "access_token": ACCESS_TOKEN},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json() or {}
        min_minor = to_float_minor(data.get("min_daily_budget", 0))
        currency = str(data.get("currency", "HKD") or "HKD")
        print(f"🧮 帳戶最低日預算(API minor): {min_minor:.0f} | {currency} (HKD≈{to_hkd_from_meta_minor(data.get('min_daily_budget', 0)):.2f})")
        return min_minor, currency
    except Exception as e:
        print(f"⚠️ 抓取帳戶最低日預算失敗，回退 0: {e}")
        return 0.0, "HKD"


def process_batch(data_list, key_prefix: str, report: dict, merged_ads: dict, synced_at: str, account_min_minor: float, account_currency: str):
    for ad in data_list:
        ad_id = str(ad.get("id") or "")
        if not ad_id:
            continue
        base = merged_ads.get(ad_id) or ad
        creative = _normalize_creative_field(base.get("creative"))
        ins_list = ad.get("insights", {}).get("data", [])
        ins = ins_list[0] if ins_list else {}
        spend = float(ins.get("spend", 0) or 0)
        cpc = float(ins.get("cpc", 0) or 0)

        if ad_id not in report:
            actor_s = _norm_id(creative.get("actor_id"))
            ig_s = _norm_id(creative.get("instagram_actor_id"))
            fb_page_name = _fb_page_name_local(actor_s, ig_s)

            adset_obj = base.get("adset") or {}
            campaign_obj = base.get("campaign") or {}
            adset_id = str(adset_obj.get("id", "") or "")
            adset_name = str(adset_obj.get("name", "") or "")
            adset_minor = to_float_minor(adset_obj.get("daily_budget", None))
            campaign_minor = to_float_minor(campaign_obj.get("daily_budget", 0))
            optimization_goal = str(adset_obj.get("optimization_goal") or "").strip()
            destination_type = str(adset_obj.get("destination_type") or "").strip()
            campaign_objective = str(campaign_obj.get("objective") or "").strip()
            cbo = adset_minor <= 0 and campaign_minor > 0
            adset_api_minor = 0.0

            targ = base.get("targeting") or {}
            targeting_json = json.dumps(targ, ensure_ascii=False) if targ else ""

            report[ad_id] = {
                "synced_at": synced_at,
                "actor_id": actor_s,
                "instagram_actor_id": ig_s,
                "fb_page_name": fb_page_name,
                "廣告名稱": base.get("name", "未知廣告"),
                "廣告文案": creative.get("body", "無文案文字"),
                "created_time": base.get("created_time", ""),
                "adset_id": adset_id,
                "adset_name": adset_name,
                "campaign_id": str(campaign_obj.get("id", "") or ""),
                "campaign_name": str(campaign_obj.get("name", "") or ""),
                "optimization_goal": optimization_goal,
                "destination_type": destination_type,
                "campaign_objective": campaign_objective,
                "adset_daily_budget_minor": adset_minor,
                "campaign_daily_budget_minor": campaign_minor,
                "adset_daily_budget_api_minor": adset_api_minor,
                "cbo": cbo,
                "account_min_budget_minor": account_min_minor,
                "currency": account_currency,
                "targeting_json": targeting_json,
                "today_cpc": 0.0,
                "today_spend": 0.0,
                "today_clicks": 0.0,
                "last_7d_avg_cpc": 0.0,
                "last_7d_spend": 0.0,
                "last_7d_clicks": 0.0,
                "last_30d_avg_cpc": 0.0,
                "this_month_spend": 0.0,
                "this_month_clicks": 0.0,
            }
        if key_prefix == "today":
            report[ad_id]["today_cpc"] = cpc
            report[ad_id]["today_spend"] = spend
            report[ad_id]["today_clicks"] = float(ins.get("clicks", 0) or 0)
        elif key_prefix == "last_7d":
            report[ad_id]["last_7d_avg_cpc"] = cpc
            report[ad_id]["last_7d_spend"] = spend
            report[ad_id]["last_7d_clicks"] = float(ins.get("clicks", 0) or 0)
        elif key_prefix == "last_30d":
            report[ad_id]["last_30d_avg_cpc"] = cpc
        elif key_prefix == "this_month":
            report[ad_id]["this_month_spend"] = spend
            report[ad_id]["this_month_clicks"] = float(ins.get("clicks", 0) or 0)


async def run_sync_process():
    ranges = _build_time_ranges()
    synced_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        account_min_minor, account_currency = await get_account_min_budget_minor(client)
        print("📡 正在抓取四個時間維度數據: today / last_7d / last_30d / this_month + supplemental_seed")
        try:
            t_raw, d7_raw, d30_raw, m_raw, sup_raw = await asyncio.gather(
                get_range_raw_data(client, "today", ranges["today"]["since"], ranges["today"]["until"]),
                get_range_raw_data(client, "last_7d", ranges["last_7d"]["since"], ranges["last_7d"]["until"]),
                get_range_raw_data(client, "last_30d", ranges["last_30d"]["since"], ranges["last_30d"]["until"]),
                get_range_raw_data(client, "this_month", ranges["this_month"]["since"], ranges["this_month"]["until"]),
                get_supplemental_ads_for_refined_seed(
                    client, ranges["last_7d"]["since"], ranges["today"]["until"]
                ),
            )
        except Exception as e:
            print(f"❌ 抓取 Meta 數據失敗: {e}")
            return

        merged_ads = _merge_ads_for_richest_creative(t_raw, d7_raw, d30_raw, m_raw, sup_raw)
        report: dict = {}

        process_batch(t_raw, "today", report, merged_ads, synced_at, account_min_minor, account_currency)
        process_batch(d7_raw, "last_7d", report, merged_ads, synced_at, account_min_minor, account_currency)
        process_batch(d30_raw, "last_30d", report, merged_ads, synced_at, account_min_minor, account_currency)
        process_batch(m_raw, "this_month", report, merged_ads, synced_at, account_min_minor, account_currency)
        # Zero-spend / in-review ads only appear in sup_raw — attribute 7d metrics from that pull.
        process_batch(sup_raw, "last_7d", report, merged_ads, synced_at, account_min_minor, account_currency)

        header = [
            "synced_at",
            "廣告ID",
            "actor_id",
            "instagram_actor_id",
            "fb_page_name",
            "AdSet ID",
            "AdSet Name",
            "Campaign ID",
            "Campaign Name",
            "optimization_goal",
            "destination_type",
            "campaign_objective",
            "adset_daily_budget_minor",
            "campaign_daily_budget_minor",
            "adset_daily_budget_api_minor",
            "CBO",
            "今日 CPC",
            "今日花費",
            "今日點擊",
            "7日平均 CPC",
            "7日花費",
            "7日點擊",
            "30日平均 CPC",
            "本月累積花費",
            "本月點擊",
            "帳戶最低日預算_minor",
            "帳戶幣別",
            "targeting_json",
            "廣告名稱",
            "created_time",
            "廣告文案",
        ]
        sheet_output = [header]
        sorted_rows = sorted(report.items(), key=lambda x: x[1].get("today_spend", 0), reverse=True)
        for ad_id, d in sorted_rows:
            sheet_output.append(
                [
                    d.get("synced_at", ""),
                    ad_id,
                    d.get("actor_id", ""),
                    d.get("instagram_actor_id", ""),
                    d.get("fb_page_name", ""),
                    d.get("adset_id", ""),
                    d.get("adset_name", ""),
                    d.get("campaign_id", ""),
                    d.get("campaign_name", ""),
                    d.get("optimization_goal", ""),
                    d.get("destination_type", ""),
                    d.get("campaign_objective", ""),
                    d.get("adset_daily_budget_minor", 0),
                    d.get("campaign_daily_budget_minor", 0),
                    d.get("adset_daily_budget_api_minor", 0),
                    "TRUE" if d.get("cbo") else "FALSE",
                    round(d.get("today_cpc", 0), 2),
                    round(d.get("today_spend", 0), 2),
                    round(d.get("today_clicks", 0), 2),
                    round(d.get("last_7d_avg_cpc", 0), 2),
                    round(d.get("last_7d_spend", 0), 2),
                    round(d.get("last_7d_clicks", 0), 2),
                    round(d.get("last_30d_avg_cpc", 0), 2),
                    round(d.get("this_month_spend", 0), 2),
                    round(d.get("this_month_clicks", 0), 2),
                    d.get("account_min_budget_minor", 0),
                    d.get("currency", "HKD"),
                    d.get("targeting_json", ""),
                    d.get("廣告名稱", ""),
                    d.get("created_time", ""),
                    d.get("廣告文案", ""),
                ]
            )

        total_today = sum(v.get("today_spend", 0) for v in report.values())
        total_month = sum(v.get("this_month_spend", 0) for v in report.values())
        sheet_output.append([])
        stats_row = ["總消耗統計"] + [""] * (len(header) - 3) + [f"today: {total_today:.2f}", f"this_month: {total_month:.2f}"]
        sheet_output.append(stats_row)

        print(f"☁️ 正在同步 Raw 到 Google Sheets: {SHEET_NAME} / {RAW_SHEET_TAB} ...")
        try:
            sheet = get_raw_worksheet()
            sheet.clear()
            sheet.update(range_name="A1", values=sheet_output)
            print(f"✅ Raw 同步完成！今日花費: {total_today:.2f} | 本月花費: {total_month:.2f}")
            print("🚀 檢查待測貼文 → AI_Optimizer...")
            subprocess.run([sys.executable, "check_latest_posts.py"], cwd=_ROOT)
            subprocess.run([sys.executable, "ai_optimizer.py"], cwd=_ROOT)
        except Exception as e:
            print(f"❌ Google Sheets 寫入失敗: {e}")


if __name__ == "__main__":
    if not ACCESS_TOKEN or not AD_ACCOUNT_ID:
        print("❌ 錯誤：請確保環境變量已設定。")
    else:
        asyncio.run(run_sync_process())
