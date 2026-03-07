#!/usr/bin/env python3
"""
Marketing data pipeline:
- Bootstrap historical dataset (default from Jan 1st of current year)
- Incremental daily updates (typically yesterday)

Output:
  reports/yap/yap_historical.json
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import tomllib


META_AD_ACCOUNT_ID = "act_1808641036591815"
GOOGLE_ADS_CUSTOMER_ID = "6495122409"
GA4_PROPERTY_ID = "299663070"
GA4_DEFAULT_CONVERSION_EVENT = "form_gtc_otp_solicitar_codigo"

GRAPH_API_VERSION = "v25.0"
GOOGLE_ADS_API_VERSION = "v20"
GA4_DATA_API_VERSION = "v1beta"

ROOT_DIR = Path(__file__).resolve().parent.parent
CODEX_CONFIG_PATH = Path.home() / ".codex" / "config.toml"
GA4_OAUTH_PATH = ROOT_DIR / "ga4_user_oauth_analytics_ipalmera.json"
TENANTS_CONFIG_PATH = ROOT_DIR / "config" / "tenants.json"
DEFAULT_OUTPUT_PATH = ROOT_DIR / "reports" / "yap" / "yap_historical.json"
DEFAULT_ORGANIC_OUTPUT_PATH = ROOT_DIR / "reports" / "yap" / "yap_organic_historical.json"
DEFAULT_ORGANIC_LOOKBACK_DAYS = 30
META_LEAD_ACTION_PRIORITY = (
    "onsite_conversion.lead_grouped",
    "lead",
    "onsite_conversion.lead",
    "onsite_web_lead",
    "offsite_complete_registration_add_meta_leads",
    "offsite_conversion.fb_pixel_lead",
    "offsite_search_add_meta_leads",
    "offsite_content_view_add_meta_leads",
)


def _redact_sensitive_text(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"(access_token=)[^&\s:]+", r"\1***", text, flags=re.IGNORECASE)
    text = re.sub(r"(\"access_token\"\s*:\s*\")[^\"]+(\")", r"\1***\2", text, flags=re.IGNORECASE)
    text = re.sub(r"(\|)([A-Za-z0-9_\-]{10,})", r"\1***", text)
    return text


def _http_json(
    method: str,
    url: str,
    *,
    headers: Dict[str, str] | None = None,
    json_body: Dict[str, Any] | None = None,
    form_body: Dict[str, str] | None = None,
    timeout: int = 60,
) -> Any:
    request_headers = dict(headers or {})
    body_bytes = None

    if json_body is not None and form_body is not None:
        raise ValueError("Use either json_body or form_body, not both.")

    if json_body is not None:
        body_bytes = json.dumps(json_body).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    elif form_body is not None:
        body_bytes = urlencode(form_body).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/x-www-form-urlencoded")

    req = Request(url=url, method=method.upper(), headers=request_headers, data=body_bytes)

    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"HTTP {exc.code} for {_redact_sensitive_text(url)}: {_redact_sensitive_text(payload)}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(
            f"Network error for {_redact_sensitive_text(url)}: {_redact_sensitive_text(exc)}"
        ) from exc


def _load_codex_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    return tomllib.loads(path.read_text(encoding="utf-8-sig"))


def _resolve_repo_path(raw_path: Any) -> Path:
    p = Path(str(raw_path or "").strip())
    return p if p.is_absolute() else (ROOT_DIR / p).resolve()


def _default_tenants_config() -> Dict[str, Dict[str, Any]]:
    return {
        "yap": {
            "id": "yap",
            "name": "YAP",
            "report_path": str(DEFAULT_OUTPUT_PATH),
            "organic_report_path": str(DEFAULT_ORGANIC_OUTPUT_PATH),
            "meta_ad_account_id": META_AD_ACCOUNT_ID,
            "google_ads_customer_id": GOOGLE_ADS_CUSTOMER_ID,
            "google_ads_login_customer_id": GOOGLE_ADS_CUSTOMER_ID,
            "ga4_property_id": GA4_PROPERTY_ID,
            "ga4_conversion_event_name": GA4_DEFAULT_CONVERSION_EVENT,
        }
    }


def _load_tenants_config(path: Path) -> Dict[str, Dict[str, Any]]:
    default_cfg = _default_tenants_config()
    if not path.exists():
        return default_cfg

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default_cfg

    entries = payload.get("tenants", [])
    if not isinstance(entries, list):
        return default_cfg

    loaded: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        tenant_id = str(entry.get("id", "")).strip().lower()
        if not tenant_id:
            continue
        loaded[tenant_id] = {
            "id": tenant_id,
            "name": str(entry.get("name", tenant_id.upper())),
            "report_path": str(
                _resolve_repo_path(entry.get("report_path", DEFAULT_OUTPUT_PATH))
            ),
            "organic_report_path": str(
                _resolve_repo_path(entry.get("organic_report_path", DEFAULT_ORGANIC_OUTPUT_PATH))
            ),
            "meta_ad_account_id": str(
                entry.get(
                    "meta_ad_account_id",
                    entry.get("meta_account_id", META_AD_ACCOUNT_ID),
                )
            ),
            "google_ads_customer_id": str(
                entry.get(
                    "google_ads_customer_id",
                    entry.get("google_customer_id", GOOGLE_ADS_CUSTOMER_ID),
                )
            ),
            "google_ads_login_customer_id": str(
                entry.get(
                    "google_ads_login_customer_id",
                    entry.get(
                        "google_login_customer_id",
                        entry.get(
                            "google_ads_customer_id",
                            entry.get("google_customer_id", GOOGLE_ADS_CUSTOMER_ID),
                        ),
                    ),
                )
            ),
            "ga4_property_id": str(entry.get("ga4_property_id", GA4_PROPERTY_ID)),
            "ga4_conversion_event_name": str(
                entry.get("ga4_conversion_event_name", GA4_DEFAULT_CONVERSION_EVENT)
            ).strip()
            or GA4_DEFAULT_CONVERSION_EVENT,
        }
    return loaded if loaded else default_cfg


def _resolve_tenant_config(
    *,
    tenant_id: str,
    tenants_config_path: Path,
) -> Dict[str, Any]:
    tenants = _load_tenants_config(tenants_config_path)
    t_id = str(tenant_id or "yap").strip().lower()
    if t_id not in tenants:
        raise RuntimeError(
            f"Tenant '{t_id}' not found in tenants config: {tenants_config_path}"
        )
    return tenants[t_id]


def _safe_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, list):
        return sum(_safe_float(item) for item in value)
    if isinstance(value, dict):
        if "value" in value:
            return _safe_float(value.get("value"))
        return sum(_safe_float(v) for v in value.values())
    if isinstance(value, str):
        value = value.strip().replace(",", "")
    return float(value)


def _clean_enum(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    up = text.upper()
    if up in {"UNSPECIFIED", "UNKNOWN", "UNAVAILABLE"}:
        return ""
    return up


def _chunk_list(values: List[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        size = 1
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def _safe_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    return int(_safe_float(value))


def _calc_cpl(spend: float, conversions: float) -> float | None:
    if conversions <= 0:
        return None
    return spend / conversions


def _meta_actions_to_map(actions: Any) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not isinstance(actions, list):
        return out
    for item in actions:
        if not isinstance(item, dict):
            continue
        action_type = str(item.get("action_type", "")).strip()
        if not action_type:
            continue
        out[action_type] = out.get(action_type, 0.0) + _safe_float(item.get("value"))
    return out


def _meta_conversion_value(row: Dict[str, Any]) -> Tuple[float, float, float, float]:
    """
    Return tuple:
      (selected_conversions, conversions_raw, lead_conversions, fb_pixel_lead_conversions)
    """
    conv_raw = _safe_float(row.get("conversions"))
    action_map = _meta_actions_to_map(row.get("actions"))
    lead_conv = _safe_float(action_map.get("lead"))
    fb_pixel_lead_conv = _safe_float(action_map.get("offsite_conversion.fb_pixel_lead"))

    if conv_raw > 0:
        return conv_raw, conv_raw, lead_conv, fb_pixel_lead_conv
    if lead_conv > 0:
        return lead_conv, conv_raw, lead_conv, fb_pixel_lead_conv
    if fb_pixel_lead_conv > 0:
        return fb_pixel_lead_conv, conv_raw, lead_conv, fb_pixel_lead_conv

    for key in (
        "onsite_web_lead",
        "onsite_conversion.lead_grouped",
        "onsite_conversion.lead",
        "offsite_complete_registration_add_meta_leads",
    ):
        val = _safe_float(action_map.get(key))
        if val > 0:
            return val, conv_raw, lead_conv, fb_pixel_lead_conv

    return conv_raw, conv_raw, lead_conv, fb_pixel_lead_conv


def _safe_div(num: float, den: float) -> float | None:
    if den == 0:
        return None
    return num / den


def _normalize_meta_ad_account_id(raw_id: Any) -> str:
    val = str(raw_id or "").strip()
    if not val:
        return META_AD_ACCOUNT_ID
    return val if val.startswith("act_") else f"act_{val}"


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt)
        except Exception:
            continue
    return None


def _metric_from_insights_row(row: Dict[str, Any]) -> float:
    if isinstance(row.get("total_value"), dict):
        return _safe_float(row.get("total_value", {}))
    values = row.get("values")
    if isinstance(values, list) and values:
        return _safe_float(values[0].get("value"))
    return _safe_float(row.get("value"))


def _urlencode_params(params: Dict[str, Any]) -> str:
    normalized: Dict[str, Any] = {}
    for k, v in params.items():
        if v is None:
            continue
        if isinstance(v, (dict, list)):
            normalized[k] = json.dumps(v)
        else:
            normalized[k] = v
    return urlencode(normalized)


def _meta_graph_get(path: str, token: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    query = dict(params or {})
    query["access_token"] = token
    url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{path}?{_urlencode_params(query)}"
    return _http_json("GET", url)


def _meta_metric_rows(
    path: str,
    token: str,
    metrics: List[str],
    extra_params: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    params = dict(extra_params or {})
    params["metric"] = ",".join(metrics)
    try:
        data = _meta_graph_get(path, token, params)
        rows = data.get("data", [])
        return rows if isinstance(rows, list) else []
    except Exception:
        pass

    merged: List[Dict[str, Any]] = []
    for metric in metrics:
        single = dict(extra_params or {})
        single["metric"] = metric
        try:
            data = _meta_graph_get(path, token, single)
            rows = data.get("data", [])
            if isinstance(rows, list):
                merged.extend(rows)
        except Exception:
            continue
    return merged


def _discover_meta_social_ids(meta_token: str, ad_account_id: str) -> Dict[str, Any]:
    page_id: str | None = None
    ig_user_hint: str | None = None
    next_url = (
        f"https://graph.facebook.com/{GRAPH_API_VERSION}/{ad_account_id}/ads?"
        + _urlencode_params(
            {
                "fields": "id,creative{object_story_spec,effective_object_story_id}",
                "limit": 100,
                "access_token": meta_token,
            }
        )
    )
    page_loops = 0
    while next_url and page_loops < 5 and not page_id:
        data = _http_json("GET", next_url)
        for ad in data.get("data", []):
            creative = ad.get("creative", {}) if isinstance(ad, dict) else {}
            spec = creative.get("object_story_spec", {}) if isinstance(creative, dict) else {}
            if not isinstance(spec, dict):
                continue
            page_id = str(spec.get("page_id", "")).strip() or page_id
            ig_user_hint = (
                str(spec.get("instagram_user_id", "")).strip()
                or str(spec.get("instagram_actor_id", "")).strip()
                or ig_user_hint
            )
            if page_id:
                break
        next_url = data.get("paging", {}).get("next")
        page_loops += 1

    page_name: str | None = None
    ig_business_id: str | None = None
    if page_id:
        try:
            page_data = _meta_graph_get(
                page_id,
                meta_token,
                {"fields": "id,name,instagram_business_account"},
            )
            page_name = str(page_data.get("name", "")).strip() or None
            ig_obj = page_data.get("instagram_business_account", {})
            if isinstance(ig_obj, dict):
                ig_business_id = str(ig_obj.get("id", "")).strip() or None
        except Exception:
            pass

    return {
        "facebook_page_id": page_id,
        "facebook_page_name": page_name,
        "instagram_business_account_id": ig_business_id,
        "instagram_user_hint": ig_user_hint,
    }


def _resolve_page_access_token(
    meta_token: str, facebook_page_id: str, cfg: Dict[str, Any]
) -> str | None:
    meta_env = cfg.get("mcp_servers", {}).get("meta-ads-mcp", {}).get("env", {})
    cfg_token = str(meta_env.get("META_FACEBOOK_PAGE_ACCESS_TOKEN", "")).strip()
    if cfg_token:
        return cfg_token

    try:
        data = _meta_graph_get(
            "me/accounts",
            meta_token,
            {"fields": "id,name,access_token", "limit": 200},
        )
    except Exception:
        return None

    for page in data.get("data", []):
        if str(page.get("id", "")).strip() == str(facebook_page_id).strip():
            token = str(page.get("access_token", "")).strip()
            if token:
                return token
    return None


def _normalize_media_type(
    *,
    platform: str,
    media_type: Any,
    media_product_type: Any = None,
    attachment_count: int = 0,
) -> str:
    mt = str(media_type or "").strip().upper()
    mpt = str(media_product_type or "").strip().upper()
    if platform == "Instagram":
        if mpt == "REELS":
            return "REEL"
        if mt in ("CAROUSEL_ALBUM", "CAROUSEL"):
            return "CAROUSEL"
        if mt in ("IMAGE", "PHOTO"):
            return "IMAGE"
        if mt == "VIDEO":
            return "REEL"
        return "IMAGE"

    # Facebook mapping to normalized buckets.
    if attachment_count > 1:
        return "CAROUSEL"
    if mt in ("PHOTO", "IMAGE"):
        return "IMAGE"
    if "VIDEO" in mt:
        return "REEL"
    return "IMAGE"


def _score_media_quality(
    *, reach: float, likes: float, comments: float, saved: float, shares: float
) -> Dict[str, float | None]:
    engagement = likes + comments + saved + shares
    engagement_rate = _safe_div(engagement, reach)
    amplification_ratio = _safe_div(shares, reach)
    value_score = _safe_div(saved * 2.0 + shares, reach)
    return {
        "engagement": engagement,
        "engagement_rate": engagement_rate,
        "amplification_ratio": None if amplification_ratio is None else amplification_ratio * 100.0,
        "value_score": value_score,
    }


def _fetch_instagram_media_rows(
    *,
    meta_token: str,
    instagram_business_account_id: str,
    start_day: date,
    end_day: date,
) -> List[Dict[str, Any]]:
    fields = (
        "id,caption,media_type,media_product_type,permalink,timestamp,media_url,"
        "thumbnail_url,like_count,comments_count"
    )
    next_url = (
        f"https://graph.facebook.com/{GRAPH_API_VERSION}/{instagram_business_account_id}/media?"
        + _urlencode_params(
            {
                "fields": fields,
                "limit": 100,
                "access_token": meta_token,
            }
        )
    )
    out: List[Dict[str, Any]] = []
    page_loops = 0
    insight_metrics = [
        "reach",
        "impressions",
        "saved",
        "shares",
        "comments",
        "likes",
        "video_views",
        "views",
    ]

    while next_url and page_loops < 20:
        data = _http_json("GET", next_url)
        for media in data.get("data", []):
            if not isinstance(media, dict):
                continue
            media_id = str(media.get("id", "")).strip()
            timestamp_raw = media.get("timestamp")
            dt = _parse_dt(timestamp_raw)
            if not media_id or not dt:
                continue
            day = dt.date()
            if day < start_day or day > end_day:
                continue

            insight_rows = _meta_metric_rows(
                f"{media_id}/insights",
                meta_token,
                insight_metrics,
                extra_params={},
            )
            insight_map: Dict[str, float] = {}
            for row in insight_rows:
                name = str(row.get("name", "")).strip()
                if not name:
                    continue
                insight_map[name] = _metric_from_insights_row(row)

            likes = _safe_float(media.get("like_count")) or _safe_float(insight_map.get("likes"))
            comments = _safe_float(media.get("comments_count")) or _safe_float(
                insight_map.get("comments")
            )
            saved = _safe_float(insight_map.get("saved"))
            shares = _safe_float(insight_map.get("shares"))
            video_views = _safe_float(insight_map.get("video_views")) or _safe_float(
                insight_map.get("views")
            )
            reach = _safe_float(insight_map.get("reach"))
            impressions = _safe_float(insight_map.get("impressions"))
            score = _score_media_quality(
                reach=reach,
                likes=likes,
                comments=comments,
                saved=saved,
                shares=shares,
            )

            out.append(
                {
                    "platform": "Instagram",
                    "media_id": media_id,
                    "media_type": _normalize_media_type(
                        platform="Instagram",
                        media_type=media.get("media_type"),
                        media_product_type=media.get("media_product_type"),
                    ),
                    "timestamp": dt.isoformat(),
                    "date": day.isoformat(),
                    "caption": str(media.get("caption", "")),
                    "permalink": str(media.get("permalink", "")),
                    "media_url": str(media.get("media_url", "")),
                    "thumbnail_url": str(media.get("thumbnail_url", "")),
                    "reach": reach,
                    "impressions": impressions,
                    "saved": saved,
                    "shares": shares,
                    "comments": comments,
                    "likes": likes,
                    "video_views": video_views,
                    "engagement": score["engagement"],
                    "engagement_rate": score["engagement_rate"],
                    "amplification_ratio": score["amplification_ratio"],
                    "value_score": score["value_score"],
                }
            )
        next_url = data.get("paging", {}).get("next")
        page_loops += 1
    return out


def _fetch_instagram_user_insights_rows(
    *,
    meta_token: str,
    instagram_business_account_id: str,
    start_day: date,
    end_day: date,
) -> List[Dict[str, Any]]:
    rows = _meta_metric_rows(
        f"{instagram_business_account_id}/insights",
        meta_token,
        ["follower_count", "profile_views", "website_clicks"],
        extra_params={
            "period": "day",
            "since": start_day.isoformat(),
            "until": end_day.isoformat(),
        },
    )

    by_day: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        metric_name = str(row.get("name", "")).strip()
        if not metric_name:
            continue
        values = row.get("values")
        if not isinstance(values, list) or not values:
            continue
        for item in values:
            d = _parse_dt(item.get("end_time"))
            day_key = (d.date() if d else end_day).isoformat()
            bucket = by_day.setdefault(
                day_key,
                {
                    "date": day_key,
                    "platform": "Instagram",
                    "follower_count": 0.0,
                    "profile_views": 0.0,
                    "website_clicks": 0.0,
                },
            )
            bucket[metric_name] = _safe_float(item.get("value"))

    return [by_day[k] for k in sorted(by_day.keys())]


def _fetch_facebook_page_media_rows(
    *,
    page_token: str,
    facebook_page_id: str,
    start_day: date,
    end_day: date,
) -> List[Dict[str, Any]]:
    fields = (
        "id,message,created_time,permalink_url,full_picture,type,shares,"
        "comments.summary(true),reactions.summary(true),"
        "attachments{media_type,subattachments},"
        "insights.metric(post_impressions,post_impressions_unique,post_saved,post_video_views)"
    )
    next_url = (
        f"https://graph.facebook.com/{GRAPH_API_VERSION}/{facebook_page_id}/posts?"
        + _urlencode_params(
            {
                "fields": fields,
                "since": start_day.isoformat(),
                "until": end_day.isoformat(),
                "limit": 100,
                "access_token": page_token,
            }
        )
    )
    out: List[Dict[str, Any]] = []
    page_loops = 0

    while next_url and page_loops < 20:
        data = _http_json("GET", next_url)
        for post in data.get("data", []):
            if not isinstance(post, dict):
                continue
            post_id = str(post.get("id", "")).strip()
            dt = _parse_dt(post.get("created_time"))
            if not post_id or not dt:
                continue
            day = dt.date()
            if day < start_day or day > end_day:
                continue

            insight_map: Dict[str, float] = {}
            insights = post.get("insights", {}).get("data", [])
            if isinstance(insights, list):
                for item in insights:
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get("name", "")).strip()
                    if not name:
                        continue
                    insight_map[name] = _metric_from_insights_row(item)

            attachments = post.get("attachments", {}).get("data", [])
            attachment_count = len(attachments) if isinstance(attachments, list) else 0
            shares = _safe_float((post.get("shares") or {}).get("count"))
            comments = _safe_float(
                ((post.get("comments") or {}).get("summary") or {}).get("total_count")
            )
            likes = _safe_float(
                ((post.get("reactions") or {}).get("summary") or {}).get("total_count")
            )
            saved = _safe_float(insight_map.get("post_saved"))
            reach = _safe_float(insight_map.get("post_impressions_unique"))
            impressions = _safe_float(insight_map.get("post_impressions"))
            video_views = _safe_float(insight_map.get("post_video_views"))
            score = _score_media_quality(
                reach=reach,
                likes=likes,
                comments=comments,
                saved=saved,
                shares=shares,
            )

            out.append(
                {
                    "platform": "Facebook",
                    "media_id": post_id,
                    "media_type": _normalize_media_type(
                        platform="Facebook",
                        media_type=post.get("type"),
                        attachment_count=attachment_count,
                    ),
                    "timestamp": dt.isoformat(),
                    "date": day.isoformat(),
                    "caption": str(post.get("message", "")),
                    "permalink": str(post.get("permalink_url", "")),
                    "media_url": str(post.get("full_picture", "")),
                    "thumbnail_url": str(post.get("full_picture", "")),
                    "reach": reach,
                    "impressions": impressions,
                    "saved": saved,
                    "shares": shares,
                    "comments": comments,
                    "likes": likes,
                    "video_views": video_views,
                    "engagement": score["engagement"],
                    "engagement_rate": score["engagement_rate"],
                    "amplification_ratio": score["amplification_ratio"],
                    "value_score": score["value_score"],
                }
            )
        next_url = data.get("paging", {}).get("next")
        page_loops += 1
    return out


def _merge_rows_by_key(
    existing: List[Dict[str, Any]],
    new_rows: List[Dict[str, Any]],
    key_fields: Tuple[str, ...],
    sort_field: str,
) -> List[Dict[str, Any]]:
    idx: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for row in existing:
        key = tuple(row.get(k) for k in key_fields)
        if all(v not in (None, "") for v in key):
            idx[key] = row
    for row in new_rows:
        key = tuple(row.get(k) for k in key_fields)
        if all(v not in (None, "") for v in key):
            idx[key] = row
    merged = list(idx.values())
    merged.sort(key=lambda r: str(r.get(sort_field, "")))
    return merged


def _summarize_organic_last_window(
    media_rows: List[Dict[str, Any]],
    account_rows: List[Dict[str, Any]],
    start_day: date,
    end_day: date,
) -> Dict[str, Any]:
    def in_window(row: Dict[str, Any]) -> bool:
        d = _parse_dt(row.get("timestamp")) or _parse_dt(row.get("date"))
        if not d:
            return False
        day = d.date()
        return start_day <= day <= end_day

    media_window = [r for r in media_rows if in_window(r)]
    account_window = [r for r in account_rows if in_window(r)]
    by_platform: Dict[str, Dict[str, float]] = {}
    totals = {
        "reach": 0.0,
        "impressions": 0.0,
        "saved": 0.0,
        "shares": 0.0,
        "comments": 0.0,
        "likes": 0.0,
        "video_views": 0.0,
    }
    for row in media_window:
        platform = str(row.get("platform", "Unknown"))
        slot = by_platform.setdefault(
            platform,
            {
                "posts": 0.0,
                "reach": 0.0,
                "impressions": 0.0,
                "saved": 0.0,
                "shares": 0.0,
                "comments": 0.0,
                "likes": 0.0,
                "video_views": 0.0,
            },
        )
        slot["posts"] += 1.0
        for metric in totals.keys():
            val = _safe_float(row.get(metric))
            slot[metric] += val
            totals[metric] += val

    latest_followers = None
    ig_rows = [r for r in account_window if str(r.get("platform")) == "Instagram"]
    if ig_rows:
        ig_rows_sorted = sorted(ig_rows, key=lambda r: str(r.get("date", "")))
        latest_followers = _safe_float(ig_rows_sorted[-1].get("follower_count"))

    profile_views_total = sum(_safe_float(r.get("profile_views")) for r in ig_rows)
    website_clicks_total = sum(_safe_float(r.get("website_clicks")) for r in ig_rows)

    return {
        "window": {"start": start_day.isoformat(), "end": end_day.isoformat()},
        "posts_total": len(media_window),
        "totals": totals,
        "by_platform": by_platform,
        "instagram_user_insights": {
            "latest_follower_count": latest_followers,
            "profile_views_total": profile_views_total,
            "website_clicks_total": website_clicks_total,
        },
    }


def _build_organic_report(
    *,
    existing: Dict[str, Any],
    cfg: Dict[str, Any],
    meta_token: str,
    meta_ad_account_id: str,
    start_day: date,
    end_day: date,
    run_kind: str,
) -> Dict[str, Any]:
    meta_env = cfg.get("mcp_servers", {}).get("meta-ads-mcp", {}).get("env", {})
    page_id_cfg = str(meta_env.get("META_FACEBOOK_PAGE_ID", "")).strip() or None
    ig_account_cfg = str(meta_env.get("META_INSTAGRAM_BUSINESS_ACCOUNT_ID", "")).strip() or None

    discovered = _discover_meta_social_ids(meta_token, meta_ad_account_id)
    facebook_page_id = page_id_cfg or discovered.get("facebook_page_id")
    instagram_business_account_id = ig_account_cfg or discovered.get(
        "instagram_business_account_id"
    )
    instagram_user_hint = discovered.get("instagram_user_hint")
    if not instagram_business_account_id and instagram_user_hint:
        instagram_business_account_id = instagram_user_hint

    warnings: List[str] = []
    media_new: List[Dict[str, Any]] = []
    account_new: List[Dict[str, Any]] = []

    if instagram_business_account_id:
        try:
            media_new.extend(
                _fetch_instagram_media_rows(
                    meta_token=meta_token,
                    instagram_business_account_id=str(instagram_business_account_id),
                    start_day=start_day,
                    end_day=end_day,
                )
            )
        except Exception as exc:
            warnings.append(
                f"Instagram media extraction skipped: {_redact_sensitive_text(exc)}"
            )

        try:
            account_new.extend(
                _fetch_instagram_user_insights_rows(
                    meta_token=meta_token,
                    instagram_business_account_id=str(instagram_business_account_id),
                    start_day=start_day,
                    end_day=end_day,
                )
            )
        except Exception as exc:
            warnings.append(
                f"Instagram account insights skipped: {_redact_sensitive_text(exc)}"
            )
    else:
        warnings.append("instagram_business_account_id could not be resolved.")

    if facebook_page_id:
        page_token = _resolve_page_access_token(str(meta_token), str(facebook_page_id), cfg)
        if page_token:
            try:
                media_new.extend(
                    _fetch_facebook_page_media_rows(
                        page_token=page_token,
                        facebook_page_id=str(facebook_page_id),
                        start_day=start_day,
                        end_day=end_day,
                    )
                )
            except Exception as exc:
                warnings.append(
                    f"Facebook page extraction skipped: {_redact_sensitive_text(exc)}"
                )
        else:
            warnings.append(
                "facebook_page_access_token unavailable. Configure META_FACEBOOK_PAGE_ACCESS_TOKEN "
                "or grant pages permissions for me/accounts."
            )
    else:
        warnings.append("facebook_page_id could not be resolved.")

    all_media = _merge_rows_by_key(
        existing=existing.get("media", []),
        new_rows=media_new,
        key_fields=("platform", "media_id"),
        sort_field="timestamp",
    )
    all_account = _merge_rows_by_key(
        existing=existing.get("account_insights_daily", []),
        new_rows=account_new,
        key_fields=("platform", "date"),
        sort_field="date",
    )

    return {
        "metadata": {
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "run_kind": run_kind,
            "updated_range": {"start": start_day.isoformat(), "end": end_day.isoformat()},
            "lookback_days": (end_day - start_day).days + 1,
            "ids": {
                "meta_ad_account_id": meta_ad_account_id,
                "facebook_page_id": facebook_page_id,
                "facebook_page_name": discovered.get("facebook_page_name"),
                "instagram_business_account_id": instagram_business_account_id,
                "instagram_user_hint": instagram_user_hint,
            },
            "warnings": warnings,
        },
        "media": all_media,
        "account_insights_daily": all_account,
        "summary_last_window": _summarize_organic_last_window(
            all_media, all_account, start_day, end_day
        ),
    }


def _read_windows_user_env(var_name: str) -> str | None:
    if os.name != "nt":
        return None
    try:
        import winreg  # type: ignore

        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Environment") as key:
            value, _ = winreg.QueryValueEx(key, var_name)
            if value:
                return str(value)
    except Exception:
        return None
    return None


def _fetch_meta_token_status(
    meta_token: str, app_id: str | None = None, app_secret: str | None = None
) -> Dict[str, Any]:
    now_utc = datetime.now(timezone.utc)

    def _parse_debug(payload: Dict[str, Any], method: str) -> Dict[str, Any]:
        data = payload.get("data", {})
        expires_at = _safe_int(data.get("expires_at"))
        expires_at_utc = (
            datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat()
            if expires_at > 0
            else None
        )
        days_left = None
        if expires_at > 0:
            delta_sec = expires_at - int(now_utc.timestamp())
            days_left = int(math.floor(delta_sec / 86400))

        return {
            "is_valid": bool(data.get("is_valid")),
            "type": str(data.get("type", "")),
            "application": str(data.get("application", "")),
            "app_id": str(data.get("app_id", "")),
            "expires_at_unix": expires_at if expires_at > 0 else None,
            "expires_at_utc": expires_at_utc,
            "days_left": days_left,
            "check_method": method,
            "checked_at_utc": now_utc.isoformat(),
            # Long-lived user token usually lasts ~60 days.
            "max_days_reference": 60,
        }

    # Preferred: self-debug using the same token (works in this environment).
    try:
        self_url = (
            f"https://graph.facebook.com/{GRAPH_API_VERSION}/debug_token?"
            + urlencode({"input_token": meta_token, "access_token": meta_token})
        )
        self_data = _http_json("GET", self_url)
        return _parse_debug(self_data, "self_debug_token")
    except Exception:
        pass

    # Fallback: debug using app access token if available.
    if app_id and app_secret:
        try:
            app_access = f"{app_id}|{app_secret}"
            app_url = (
                f"https://graph.facebook.com/{GRAPH_API_VERSION}/debug_token?"
                + urlencode({"input_token": meta_token, "access_token": app_access})
            )
            app_data = _http_json("GET", app_url)
            return _parse_debug(app_data, "app_debug_token")
        except Exception:
            pass

    return {
        "is_valid": None,
        "type": None,
        "application": None,
        "app_id": app_id,
        "expires_at_unix": None,
        "expires_at_utc": None,
        "days_left": None,
        "check_method": "unavailable",
        "checked_at_utc": now_utc.isoformat(),
        "max_days_reference": 60,
    }


def _google_access_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    token_url = "https://oauth2.googleapis.com/token"
    resp = _http_json(
        "POST",
        token_url,
        form_body={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        },
    )
    access_token = resp.get("access_token")
    if not access_token:
        raise RuntimeError(f"Google OAuth token response missing access_token: {resp}")
    return str(access_token)


def _date_chunks(start_day: date, end_day: date, chunk_days: int = 90) -> Iterable[Tuple[date, date]]:
    current = start_day
    while current <= end_day:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_day)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def _fetch_meta_range(
    meta_token: str, ad_account_id: str, start_day: date, end_day: date
) -> Dict[str, Dict[str, float]]:
    endpoint = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{ad_account_id}/insights"
    by_day: Dict[str, Dict[str, float]] = {}

    for chunk_start, chunk_end in _date_chunks(start_day, end_day, 90):
        params = {
            "fields": "date_start,spend,clicks,conversions,actions,impressions,reach,frequency,ctr,cpc",
            "time_increment": "1",
            "time_range": json.dumps(
                {"since": chunk_start.isoformat(), "until": chunk_end.isoformat()}
            ),
            "access_token": meta_token,
        }
        next_url = f"{endpoint}?{urlencode(params)}"
        while next_url:
            data = _http_json("GET", next_url)
            rows = data.get("data", [])
            for row in rows:
                day = str(row.get("date_start", ""))
                if not day:
                    continue
                conv_selected, conv_raw, conv_lead, conv_fb_pixel_lead = _meta_conversion_value(row)
                by_day[day] = {
                    "spend": _safe_float(row.get("spend")),
                    "clicks": _safe_float(row.get("clicks")),
                    "conversions": conv_selected,
                    "conversions_raw": conv_raw,
                    "conversions_lead": conv_lead,
                    "conversions_fb_pixel_lead": conv_fb_pixel_lead,
                    "impressions": _safe_float(row.get("impressions")),
                    "reach": _safe_float(row.get("reach")),
                    "frequency": _safe_float(row.get("frequency")),
                    "ctr": _safe_float(row.get("ctr")) / 100.0,
                    "cpc": _safe_float(row.get("cpc")),
                }
            next_url = data.get("paging", {}).get("next")
    return by_day


def _fetch_meta_campaign_range(
    meta_token: str, ad_account_id: str, start_day: date, end_day: date
) -> List[Dict[str, Any]]:
    endpoint = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{ad_account_id}/insights"
    out: List[Dict[str, Any]] = []

    for chunk_start, chunk_end in _date_chunks(start_day, end_day, 60):
        params = {
            "fields": (
                "date_start,campaign_id,campaign_name,spend,impressions,reach,frequency,"
                "clicks,ctr,cpc,conversions,actions"
            ),
            "level": "campaign",
            "time_increment": "1",
            "time_range": json.dumps(
                {"since": chunk_start.isoformat(), "until": chunk_end.isoformat()}
            ),
            "access_token": meta_token,
        }
        next_url = f"{endpoint}?{urlencode(params)}"
        while next_url:
            data = _http_json("GET", next_url)
            rows = data.get("data", [])
            for row in rows:
                day = str(row.get("date_start", ""))
                if not day:
                    continue
                conv_selected, conv_raw, conv_lead, conv_fb_pixel_lead = _meta_conversion_value(row)
                out.append(
                    {
                        "date": day,
                        "campaign_id": str(row.get("campaign_id", "")),
                        "campaign_name": str(row.get("campaign_name", "")),
                        "spend": _safe_float(row.get("spend")),
                        "impressions": _safe_float(row.get("impressions")),
                        "reach": _safe_float(row.get("reach")),
                        "frequency": _safe_float(row.get("frequency")),
                        "clicks": _safe_float(row.get("clicks")),
                        "ctr": _safe_float(row.get("ctr")) / 100.0,
                        "cpc": _safe_float(row.get("cpc")),
                        "conversions": conv_selected,
                        "conversions_raw": conv_raw,
                        "conversions_lead": conv_lead,
                        "conversions_fb_pixel_lead": conv_fb_pixel_lead,
                    }
                )
            next_url = data.get("paging", {}).get("next")
    campaign_ids = [
        str(row.get("campaign_id", "")).strip()
        for row in out
        if str(row.get("campaign_id", "")).strip()
    ]
    objective_by_campaign = _fetch_meta_campaign_objectives(meta_token, campaign_ids)
    for row in out:
        campaign_id = str(row.get("campaign_id", "")).strip()
        objective = str(objective_by_campaign.get(campaign_id, "")).strip()
        row["objective"] = objective
        row["campaign_goal"] = objective
    return out


def _fetch_meta_campaign_objectives(
    meta_token: str,
    campaign_ids: List[str],
) -> Dict[str, str]:
    uniq_ids: List[str] = []
    seen: set[str] = set()
    for raw_id in campaign_ids:
        cid = str(raw_id or "").strip()
        if not cid or cid in seen:
            continue
        seen.add(cid)
        uniq_ids.append(cid)
    if not uniq_ids:
        return {}

    endpoint = f"https://graph.facebook.com/{GRAPH_API_VERSION}/"
    objective_by_campaign: Dict[str, str] = {}
    for batch_ids in _chunk_list(uniq_ids, 40):
        params = {
            "ids": ",".join(batch_ids),
            "fields": "objective",
            "access_token": meta_token,
        }
        batch_url = f"{endpoint}?{urlencode(params)}"
        try:
            data = _http_json("GET", batch_url)
        except Exception as exc:
            print(
                f"[warn] Could not read Meta campaign objectives for batch "
                f"{batch_ids[:3]}... ({len(batch_ids)} ids): {exc}",
                file=sys.stderr,
            )
            continue

        if not isinstance(data, dict):
            continue
        for campaign_id in batch_ids:
            raw = data.get(campaign_id, {})
            if not isinstance(raw, dict):
                continue
            objective = str(raw.get("objective", "")).strip()
            if objective:
                objective_by_campaign[campaign_id] = objective
    return objective_by_campaign


def _normalize_paid_device(raw: Any) -> str:
    d = str(raw or "").strip().upper()
    if "DESKTOP" in d:
        return "Desktop"
    if (
        "MOBILE" in d
        or "TABLET" in d
        or "SMARTPHONE" in d
        or "IPHONE" in d
        or "ANDROID" in d
        or "IPAD" in d
    ):
        return "Mobile"
    return "Other"


def _meta_lead_value(row: Dict[str, Any]) -> float:
    action_map = _meta_actions_to_map(row.get("actions"))
    for key in META_LEAD_ACTION_PRIORITY:
        val = _safe_float(action_map.get(key))
        if val > 0:
            return val
    return 0.0


def _normalize_age_range(raw: Any) -> str:
    txt = str(raw or "").strip()
    if not txt:
        return "Unknown"
    up = txt.upper().replace("AGE_RANGE_", "")
    mapped = {
        "18_24": "18-24",
        "18-24": "18-24",
        "25_34": "25-34",
        "25-34": "25-34",
        "35_44": "35-44",
        "35-44": "35-44",
        "45_54": "45-54",
        "45-54": "45-54",
        "55_64": "55-64",
        "55-64": "55-64",
        "65_UP": "65+",
        "65+": "65+",
        "UNKNOWN": "Unknown",
        "UNDETERMINED": "Unknown",
    }.get(up)
    return mapped if mapped else txt


def _normalize_gender(raw: Any) -> str:
    txt = str(raw or "").strip().lower()
    if txt in ("female", "f"):
        return "Female"
    if txt in ("male", "m"):
        return "Male"
    return "Unknown"


def _fetch_meta_lead_demographics_range(
    meta_token: str,
    ad_account_id: str,
    start_day: date,
    end_day: date,
) -> List[Dict[str, Any]]:
    endpoint = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{ad_account_id}/insights"
    acc: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}

    for chunk_start, chunk_end in _date_chunks(start_day, end_day, 30):
        params = {
            "fields": "date_start,spend,impressions,clicks,actions",
            "level": "account",
            "breakdowns": "age,gender",
            "time_increment": "1",
            "time_range": json.dumps(
                {"since": chunk_start.isoformat(), "until": chunk_end.isoformat()}
            ),
            "access_token": meta_token,
        }
        next_url = f"{endpoint}?{urlencode(params)}"
        while next_url:
            data = _http_json("GET", next_url)
            rows = data.get("data", [])
            for row in rows:
                day = str(row.get("date_start", "")).strip()
                if not day:
                    continue
                leads = _meta_lead_value(row)
                age_range = _normalize_age_range(row.get("age"))
                gender = _normalize_gender(row.get("gender"))
                key = (day, "Meta", age_range, gender)
                bucket = acc.setdefault(
                    key,
                    {
                        "date": day,
                        "platform": "Meta",
                        "breakdown": "age_gender",
                        "age_range": age_range,
                        "gender": gender,
                        "spend": 0.0,
                        "impressions": 0.0,
                        "clicks": 0.0,
                        "leads": 0.0,
                    },
                )
                bucket["spend"] += _safe_float(row.get("spend"))
                bucket["impressions"] += _safe_float(row.get("impressions"))
                bucket["clicks"] += _safe_float(row.get("clicks"))
                bucket["leads"] += leads
            next_url = data.get("paging", {}).get("next")
    out = list(acc.values())
    out.sort(
        key=lambda r: (
            str(r.get("date", "")),
            str(r.get("platform", "")),
            str(r.get("age_range", "")),
            str(r.get("gender", "")),
        )
    )
    return out


def _fetch_meta_lead_geo_range(
    meta_token: str,
    ad_account_id: str,
    start_day: date,
    end_day: date,
) -> List[Dict[str, Any]]:
    endpoint = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{ad_account_id}/insights"
    acc: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}

    for chunk_start, chunk_end in _date_chunks(start_day, end_day, 30):
        params = {
            "fields": "date_start,spend,impressions,clicks,actions",
            "level": "account",
            "breakdowns": "country,region",
            "time_increment": "1",
            "time_range": json.dumps(
                {"since": chunk_start.isoformat(), "until": chunk_end.isoformat()}
            ),
            "access_token": meta_token,
        }
        next_url = f"{endpoint}?{urlencode(params)}"
        while next_url:
            data = _http_json("GET", next_url)
            rows = data.get("data", [])
            for row in rows:
                day = str(row.get("date_start", "")).strip()
                if not day:
                    continue
                leads = _meta_lead_value(row)
                country_code = str(row.get("country", "")).strip().upper()
                region = str(row.get("region", "")).strip() or "Unknown"
                key = (day, "Meta", country_code, region)
                bucket = acc.setdefault(
                    key,
                    {
                        "date": day,
                        "platform": "Meta",
                        "country_code": country_code,
                        "region": region,
                        "spend": 0.0,
                        "impressions": 0.0,
                        "clicks": 0.0,
                        "leads": 0.0,
                    },
                )
                bucket["spend"] += _safe_float(row.get("spend"))
                bucket["impressions"] += _safe_float(row.get("impressions"))
                bucket["clicks"] += _safe_float(row.get("clicks"))
                bucket["leads"] += leads
            next_url = data.get("paging", {}).get("next")
    out = list(acc.values())
    out.sort(
        key=lambda r: (
            str(r.get("date", "")),
            str(r.get("platform", "")),
            str(r.get("country_code", "")),
            str(r.get("region", "")),
        )
    )
    return out


def _fetch_meta_device_range(
    meta_token: str, ad_account_id: str, start_day: date, end_day: date
) -> List[Dict[str, Any]]:
    endpoint = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{ad_account_id}/insights"
    acc: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    for chunk_start, chunk_end in _date_chunks(start_day, end_day, 30):
        params = {
            "fields": "date_start,spend,impressions,reach,clicks,conversions,actions",
            "level": "account",
            "breakdowns": "impression_device",
            "time_increment": "1",
            "time_range": json.dumps(
                {"since": chunk_start.isoformat(), "until": chunk_end.isoformat()}
            ),
            "access_token": meta_token,
        }
        next_url = f"{endpoint}?{urlencode(params)}"
        while next_url:
            data = _http_json("GET", next_url)
            rows = data.get("data", [])
            for row in rows:
                day = str(row.get("date_start", "")).strip()
                if not day:
                    continue
                conv_selected, _, _, _ = _meta_conversion_value(row)
                device = _normalize_paid_device(row.get("impression_device"))
                key = (day, "Meta", device)
                bucket = acc.setdefault(
                    key,
                    {
                        "date": day,
                        "platform": "Meta",
                        "device": device,
                        "spend": 0.0,
                        "impressions": 0.0,
                        "reach": 0.0,
                        "clicks": 0.0,
                        "conversions": 0.0,
                    },
                )
                bucket["spend"] += _safe_float(row.get("spend"))
                bucket["impressions"] += _safe_float(row.get("impressions"))
                bucket["reach"] += _safe_float(row.get("reach"))
                bucket["clicks"] += _safe_float(row.get("clicks"))
                bucket["conversions"] += conv_selected
            next_url = data.get("paging", {}).get("next")
    out = list(acc.values())
    out.sort(key=lambda r: (str(r.get("date", "")), str(r.get("platform", "")), str(r.get("device", ""))))
    return out


def _fetch_google_ads_range(
    *,
    access_token: str,
    developer_token: str,
    customer_id: str,
    login_customer_id: str,
    quota_project: str | None,
    start_day: date,
    end_day: date,
) -> Dict[str, Dict[str, float]]:
    endpoint = (
        f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/"
        f"{customer_id}/googleAds:searchStream"
    )
    query = (
        "SELECT segments.date, metrics.cost_micros, metrics.clicks, metrics.impressions, "
        "metrics.ctr, metrics.average_cpc, metrics.conversions "
        "FROM customer "
        f"WHERE segments.date BETWEEN '{start_day.isoformat()}' AND '{end_day.isoformat()}' "
        "ORDER BY segments.date"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
        "login-customer-id": login_customer_id,
    }
    if quota_project:
        headers["x-goog-user-project"] = quota_project

    data = _http_json("POST", endpoint, headers=headers, json_body={"query": query})
    chunks = data if isinstance(data, list) else [data]

    by_day: Dict[str, Dict[str, float]] = {}
    for chunk in chunks:
        for row in chunk.get("results", []):
            segments = row.get("segments", {})
            metrics = row.get("metrics", {})
            day = str(segments.get("date", ""))
            if not day:
                continue
            cost_micros = _safe_int(metrics.get("costMicros"))
            avg_cpc_micros = _safe_float(metrics.get("averageCpc"))
            by_day[day] = {
                "cost_micros": float(cost_micros),
                "cost": float(cost_micros) / 1_000_000.0,
                "clicks": _safe_float(metrics.get("clicks")),
                "impressions": _safe_float(metrics.get("impressions")),
                "ctr": _safe_float(metrics.get("ctr")),
                "average_cpc_micros": avg_cpc_micros,
                "average_cpc": avg_cpc_micros / 1_000_000.0 if avg_cpc_micros else 0.0,
                "conversions": _safe_float(metrics.get("conversions")),
            }
    return by_day


def _fetch_google_device_range(
    *,
    access_token: str,
    developer_token: str,
    customer_id: str,
    login_customer_id: str,
    quota_project: str | None,
    start_day: date,
    end_day: date,
) -> List[Dict[str, Any]]:
    endpoint = (
        f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/"
        f"{customer_id}/googleAds:searchStream"
    )
    query = (
        "SELECT segments.date, segments.device, metrics.cost_micros, "
        "metrics.impressions, metrics.clicks, metrics.conversions "
        "FROM customer "
        f"WHERE segments.date BETWEEN '{start_day.isoformat()}' AND '{end_day.isoformat()}' "
        "ORDER BY segments.date, segments.device"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
        "login-customer-id": login_customer_id,
    }
    if quota_project:
        headers["x-goog-user-project"] = quota_project

    data = _http_json("POST", endpoint, headers=headers, json_body={"query": query})
    chunks = data if isinstance(data, list) else [data]

    acc: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for chunk in chunks:
        for row in chunk.get("results", []):
            segments = row.get("segments", {})
            metrics = row.get("metrics", {})
            day = str(segments.get("date", "")).strip()
            if not day:
                continue
            cost_micros = _safe_float(metrics.get("costMicros"))
            device = _normalize_paid_device(segments.get("device"))
            key = (day, "Google", device)
            bucket = acc.setdefault(
                key,
                {
                    "date": day,
                    "platform": "Google",
                    "device": device,
                    "spend": 0.0,
                    "impressions": 0.0,
                    "reach": 0.0,
                    "clicks": 0.0,
                    "conversions": 0.0,
                },
            )
            bucket["spend"] += cost_micros / 1_000_000.0
            bucket["impressions"] += _safe_float(metrics.get("impressions"))
            bucket["clicks"] += _safe_float(metrics.get("clicks"))
            bucket["conversions"] += _safe_float(metrics.get("conversions"))
    out = list(acc.values())
    out.sort(key=lambda r: (str(r.get("date", "")), str(r.get("platform", "")), str(r.get("device", ""))))
    return out


def _fetch_google_campaign_range(
    *,
    access_token: str,
    developer_token: str,
    customer_id: str,
    login_customer_id: str,
    quota_project: str | None,
    start_day: date,
    end_day: date,
) -> List[Dict[str, Any]]:
    endpoint = (
        f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/"
        f"{customer_id}/googleAds:searchStream"
    )
    query = (
        "SELECT segments.date, campaign.id, campaign.name, "
        "campaign.advertising_channel_type, campaign.advertising_channel_sub_type, "
        "campaign.bidding_strategy_type, metrics.cost_micros, "
        "metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc, "
        "metrics.conversions "
        "FROM campaign "
        f"WHERE segments.date BETWEEN '{start_day.isoformat()}' AND '{end_day.isoformat()}' "
        "ORDER BY segments.date, campaign.id"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
        "login-customer-id": login_customer_id,
    }
    if quota_project:
        headers["x-goog-user-project"] = quota_project

    data = _http_json("POST", endpoint, headers=headers, json_body={"query": query})
    chunks = data if isinstance(data, list) else [data]

    out: List[Dict[str, Any]] = []
    for chunk in chunks:
        for row in chunk.get("results", []):
            segments = row.get("segments", {})
            metrics = row.get("metrics", {})
            campaign = row.get("campaign", {})
            day = str(segments.get("date", ""))
            if not day:
                continue
            cost_micros = _safe_float(metrics.get("costMicros"))
            avg_cpc_micros = _safe_float(metrics.get("averageCpc"))
            advertising_channel_type = _clean_enum(campaign.get("advertisingChannelType"))
            advertising_channel_sub_type = _clean_enum(campaign.get("advertisingChannelSubType"))
            bidding_strategy_type = _clean_enum(campaign.get("biddingStrategyType"))
            campaign_goal = (
                advertising_channel_sub_type
                or advertising_channel_type
                or bidding_strategy_type
            )
            out.append(
                {
                    "date": day,
                    "campaign_id": str(campaign.get("id", "")),
                    "campaign_name": str(campaign.get("name", "")),
                    "advertising_channel_type": advertising_channel_type,
                    "advertising_channel_sub_type": advertising_channel_sub_type,
                    "bidding_strategy_type": bidding_strategy_type,
                    "campaign_goal": campaign_goal,
                    "cost_micros": cost_micros,
                    "cost": cost_micros / 1_000_000.0,
                    "impressions": _safe_float(metrics.get("impressions")),
                    "clicks": _safe_float(metrics.get("clicks")),
                    "ctr": _safe_float(metrics.get("ctr")),
                    "average_cpc_micros": avg_cpc_micros,
                    "average_cpc": avg_cpc_micros / 1_000_000.0 if avg_cpc_micros else 0.0,
                    "conversions": _safe_float(metrics.get("conversions")),
                }
            )
    return out


def _geo_target_constant_id(raw_value: Any) -> str:
    raw = str(raw_value or "").strip()
    if not raw:
        return ""
    if raw.startswith("geoTargetConstants/"):
        return raw.split("/", 1)[1].strip()
    if raw.isdigit():
        return raw
    match = re.search(r"(\d+)$", raw)
    return match.group(1) if match else ""


def _fetch_google_geo_target_constant_map(
    *,
    access_token: str,
    developer_token: str,
    customer_id: str,
    login_customer_id: str,
    quota_project: str | None,
    criterion_ids: List[str],
) -> Dict[str, Dict[str, str]]:
    uniq_ids: List[str] = []
    seen: set[str] = set()
    for raw_id in criterion_ids:
        cid = _geo_target_constant_id(raw_id)
        if not cid or cid in seen:
            continue
        seen.add(cid)
        uniq_ids.append(cid)
    if not uniq_ids:
        return {}

    endpoint = (
        f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/"
        f"{customer_id}/googleAds:searchStream"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
        "login-customer-id": login_customer_id,
    }
    if quota_project:
        headers["x-goog-user-project"] = quota_project

    out: Dict[str, Dict[str, str]] = {}
    for batch in _chunk_list(uniq_ids, 200):
        ids_sql = ", ".join(batch)
        query = (
            "SELECT geo_target_constant.id, geo_target_constant.name, "
            "geo_target_constant.country_code, geo_target_constant.target_type "
            "FROM geo_target_constant "
            f"WHERE geo_target_constant.id IN ({ids_sql})"
        )
        data = _http_json("POST", endpoint, headers=headers, json_body={"query": query})
        chunks = data if isinstance(data, list) else [data]
        for chunk in chunks:
            for row in chunk.get("results", []):
                geo = row.get("geoTargetConstant", {})
                cid = _geo_target_constant_id(geo.get("id"))
                if not cid:
                    continue
                out[cid] = {
                    "name": str(geo.get("name", "")).strip(),
                    "country_code": str(geo.get("countryCode", "")).strip().upper(),
                    "target_type": _clean_enum(geo.get("targetType")),
                }
    return out


def _fetch_google_lead_demographics_range(
    *,
    access_token: str,
    developer_token: str,
    customer_id: str,
    login_customer_id: str,
    quota_project: str | None,
    start_day: date,
    end_day: date,
) -> List[Dict[str, Any]]:
    endpoint = (
        f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/"
        f"{customer_id}/googleAds:searchStream"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
        "login-customer-id": login_customer_id,
    }
    if quota_project:
        headers["x-goog-user-project"] = quota_project

    acc: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}

    for chunk_start, chunk_end in _date_chunks(start_day, end_day, 31):
        age_query = (
            "SELECT segments.date, ad_group_criterion.age_range.type, "
            "metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions "
            "FROM age_range_view "
            f"WHERE segments.date BETWEEN '{chunk_start.isoformat()}' AND '{chunk_end.isoformat()}' "
            "ORDER BY segments.date, ad_group_criterion.age_range.type"
        )
        age_data = _http_json("POST", endpoint, headers=headers, json_body={"query": age_query})
        age_chunks = age_data if isinstance(age_data, list) else [age_data]
        for chunk in age_chunks:
            for row in chunk.get("results", []):
                segments = row.get("segments", {})
                metrics = row.get("metrics", {})
                criterion = row.get("adGroupCriterion", {})
                age_range = _normalize_age_range(
                    criterion.get("ageRange", {}).get("type")
                )
                day = str(segments.get("date", "")).strip()
                if not day:
                    continue
                key = (day, "Google", "age", age_range, "All")
                bucket = acc.setdefault(
                    key,
                    {
                        "date": day,
                        "platform": "Google",
                        "breakdown": "age",
                        "age_range": age_range,
                        "gender": "All",
                        "spend": 0.0,
                        "impressions": 0.0,
                        "clicks": 0.0,
                        "leads": 0.0,
                    },
                )
                bucket["spend"] += _safe_float(metrics.get("costMicros")) / 1_000_000.0
                bucket["impressions"] += _safe_float(metrics.get("impressions"))
                bucket["clicks"] += _safe_float(metrics.get("clicks"))
                bucket["leads"] += _safe_float(metrics.get("conversions"))

        gender_query = (
            "SELECT segments.date, ad_group_criterion.gender.type, "
            "metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions "
            "FROM gender_view "
            f"WHERE segments.date BETWEEN '{chunk_start.isoformat()}' AND '{chunk_end.isoformat()}' "
            "ORDER BY segments.date, ad_group_criterion.gender.type"
        )
        gender_data = _http_json("POST", endpoint, headers=headers, json_body={"query": gender_query})
        gender_chunks = gender_data if isinstance(gender_data, list) else [gender_data]
        for chunk in gender_chunks:
            for row in chunk.get("results", []):
                segments = row.get("segments", {})
                metrics = row.get("metrics", {})
                criterion = row.get("adGroupCriterion", {})
                gender = _normalize_gender(
                    criterion.get("gender", {}).get("type")
                )
                day = str(segments.get("date", "")).strip()
                if not day:
                    continue
                key = (day, "Google", "gender", "All", gender)
                bucket = acc.setdefault(
                    key,
                    {
                        "date": day,
                        "platform": "Google",
                        "breakdown": "gender",
                        "age_range": "All",
                        "gender": gender,
                        "spend": 0.0,
                        "impressions": 0.0,
                        "clicks": 0.0,
                        "leads": 0.0,
                    },
                )
                bucket["spend"] += _safe_float(metrics.get("costMicros")) / 1_000_000.0
                bucket["impressions"] += _safe_float(metrics.get("impressions"))
                bucket["clicks"] += _safe_float(metrics.get("clicks"))
                bucket["leads"] += _safe_float(metrics.get("conversions"))

    out = list(acc.values())
    out.sort(
        key=lambda r: (
            str(r.get("date", "")),
            str(r.get("platform", "")),
            str(r.get("breakdown", "")),
            str(r.get("age_range", "")),
            str(r.get("gender", "")),
        )
    )
    return out


def _fetch_google_lead_geo_range(
    *,
    access_token: str,
    developer_token: str,
    customer_id: str,
    login_customer_id: str,
    quota_project: str | None,
    start_day: date,
    end_day: date,
) -> List[Dict[str, Any]]:
    endpoint = (
        f"https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/"
        f"{customer_id}/googleAds:searchStream"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
        "login-customer-id": login_customer_id,
    }
    if quota_project:
        headers["x-goog-user-project"] = quota_project

    raw_rows: List[Dict[str, Any]] = []
    criterion_ids: set[str] = set()
    for chunk_start, chunk_end in _date_chunks(start_day, end_day, 31):
        query = (
            "SELECT segments.date, geographic_view.country_criterion_id, "
            "segments.geo_target_region, metrics.cost_micros, metrics.impressions, "
            "metrics.clicks, metrics.conversions "
            "FROM geographic_view "
            f"WHERE segments.date BETWEEN '{chunk_start.isoformat()}' AND '{chunk_end.isoformat()}' "
            "AND geographic_view.location_type = LOCATION_OF_PRESENCE "
            "ORDER BY segments.date, geographic_view.country_criterion_id, segments.geo_target_region"
        )
        data = _http_json("POST", endpoint, headers=headers, json_body={"query": query})
        chunks = data if isinstance(data, list) else [data]
        for chunk in chunks:
            for row in chunk.get("results", []):
                segments = row.get("segments", {})
                metrics = row.get("metrics", {})
                geo_view = row.get("geographicView", {})
                day = str(segments.get("date", "")).strip()
                if not day:
                    continue
                country_id = _geo_target_constant_id(geo_view.get("countryCriterionId"))
                region_id = _geo_target_constant_id(segments.get("geoTargetRegion"))
                if country_id:
                    criterion_ids.add(country_id)
                if region_id:
                    criterion_ids.add(region_id)
                raw_rows.append(
                    {
                        "date": day,
                        "country_id": country_id,
                        "region_id": region_id,
                        "spend": _safe_float(metrics.get("costMicros")) / 1_000_000.0,
                        "impressions": _safe_float(metrics.get("impressions")),
                        "clicks": _safe_float(metrics.get("clicks")),
                        "leads": _safe_float(metrics.get("conversions")),
                    }
                )

    geo_lookup = _fetch_google_geo_target_constant_map(
        access_token=access_token,
        developer_token=developer_token,
        customer_id=customer_id,
        login_customer_id=login_customer_id,
        quota_project=quota_project,
        criterion_ids=sorted(criterion_ids),
    )

    acc: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for row in raw_rows:
        country_id = str(row.get("country_id", "")).strip()
        region_id = str(row.get("region_id", "")).strip()
        country_meta = geo_lookup.get(country_id, {})
        region_meta = geo_lookup.get(region_id, {})

        country_code = str(country_meta.get("country_code", "")).strip().upper()
        if not country_code:
            country_code = f"ID:{country_id}" if country_id else "Unknown"
        country_name = str(country_meta.get("name", "")).strip()
        region = str(region_meta.get("name", "")).strip() or "Unknown"

        day = str(row.get("date", "")).strip()
        key = (day, "Google", country_code, region)
        bucket = acc.setdefault(
            key,
            {
                "date": day,
                "platform": "Google",
                "country_code": country_code,
                "country_name": country_name,
                "region": region,
                "spend": 0.0,
                "impressions": 0.0,
                "clicks": 0.0,
                "leads": 0.0,
            },
        )
        bucket["spend"] += _safe_float(row.get("spend"))
        bucket["impressions"] += _safe_float(row.get("impressions"))
        bucket["clicks"] += _safe_float(row.get("clicks"))
        bucket["leads"] += _safe_float(row.get("leads"))

    out = list(acc.values())
    out.sort(
        key=lambda r: (
            str(r.get("date", "")),
            str(r.get("platform", "")),
            str(r.get("country_code", "")),
            str(r.get("region", "")),
        )
    )
    return out


def _load_ga4_oauth_credentials(path: Path) -> Tuple[str, str, str, str | None]:
    if not path.exists():
        raise FileNotFoundError(f"GA4 oauth file not found: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    client_id = obj.get("client_id")
    client_secret = obj.get("client_secret")
    refresh_token = obj.get("refresh_token")
    quota_project_id = obj.get("quota_project_id")
    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError(f"Invalid GA4 oauth file structure: {path}")
    return (
        str(client_id),
        str(client_secret),
        str(refresh_token),
        str(quota_project_id) if quota_project_id else None,
    )


def _ga4_day_key(raw_day: str) -> str:
    if len(raw_day) != 8:
        return raw_day
    return f"{raw_day[0:4]}-{raw_day[4:6]}-{raw_day[6:8]}"


def _source_medium_to_platform(source_medium: str) -> str:
    s = str(source_medium or "").lower()
    if any(k in s for k in ("facebook", "instagram", "meta /", "fb /", "facebook /")):
        return "Meta"
    if any(
        k in s
        for k in (
            "google /",
            "adwords",
            "search,adwords",
            "googleads",
            "google / cpc",
            "google / paid",
        )
    ):
        return "Google"
    return "Other"


def _fetch_ga4_run_report(
    *,
    access_token: str,
    ga4_property_id: str,
    start_day: date,
    end_day: date,
    dimensions: List[str],
    metrics: List[str],
    quota_project: str | None,
    dimension_filter: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    endpoint = (
        f"https://analyticsdata.googleapis.com/{GA4_DATA_API_VERSION}/properties/"
        f"{ga4_property_id}:runReport"
    )
    headers = {"Authorization": f"Bearer {access_token}"}
    if quota_project:
        headers["x-goog-user-project"] = quota_project

    payload = {
        "dateRanges": [{"startDate": start_day.isoformat(), "endDate": end_day.isoformat()}],
        "dimensions": [{"name": d} for d in dimensions],
        "metrics": [{"name": m} for m in metrics],
        "limit": 250000,
    }
    if dimension_filter:
        payload["dimensionFilter"] = dimension_filter
    data = _http_json("POST", endpoint, headers=headers, json_body=payload)
    rows = data.get("rows", [])

    parsed: List[Dict[str, Any]] = []
    for row in rows:
        dim_vals = row.get("dimensionValues", [])
        met_vals = row.get("metricValues", [])
        item: Dict[str, Any] = {}
        for i, d in enumerate(dimensions):
            raw = str(dim_vals[i].get("value", "")) if i < len(dim_vals) else ""
            item[d] = _ga4_day_key(raw) if d == "date" else raw
        for i, m in enumerate(metrics):
            raw = met_vals[i].get("value") if i < len(met_vals) else 0
            item[m] = _safe_float(raw)
        parsed.append(item)
    return parsed


def _fetch_ga4_main_range(
    access_token: str,
    ga4_property_id: str,
    start_day: date,
    end_day: date,
    quota_project: str | None,
) -> Dict[str, Dict[str, float]]:
    rows = _fetch_ga4_run_report(
        access_token=access_token,
        ga4_property_id=ga4_property_id,
        start_day=start_day,
        end_day=end_day,
        dimensions=["date"],
        metrics=[
            "averageSessionDuration",
            "totalUsers",
            "sessions",
            "engagementRate",
            "bounceRate",
        ],
        quota_project=quota_project,
    )
    by_day: Dict[str, Dict[str, float]] = {}
    for row in rows:
        day = str(row.get("date", ""))
        if not day:
            continue
        by_day[day] = {
            "averageSessionDuration": _safe_float(row.get("averageSessionDuration")),
            "totalUsers": _safe_float(row.get("totalUsers")),
            "sessions": _safe_float(row.get("sessions")),
            "engagementRate": _safe_float(row.get("engagementRate")),
            "bounceRate": _safe_float(row.get("bounceRate")),
        }
    return by_day


def _fetch_ga4_device_range(
    access_token: str,
    ga4_property_id: str,
    start_day: date,
    end_day: date,
    quota_project: str | None,
) -> List[Dict[str, Any]]:
    rows = _fetch_ga4_run_report(
        access_token=access_token,
        ga4_property_id=ga4_property_id,
        start_day=start_day,
        end_day=end_day,
        dimensions=["date", "deviceCategory"],
        metrics=["totalUsers"],
        quota_project=quota_project,
    )
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "date": str(row.get("date", "")),
                "deviceCategory": str(row.get("deviceCategory", "")),
                "totalUsers": _safe_float(row.get("totalUsers")),
            }
        )
    return out


def _fetch_ga4_country_range(
    access_token: str,
    ga4_property_id: str,
    start_day: date,
    end_day: date,
    quota_project: str | None,
) -> List[Dict[str, Any]]:
    rows = _fetch_ga4_run_report(
        access_token=access_token,
        ga4_property_id=ga4_property_id,
        start_day=start_day,
        end_day=end_day,
        dimensions=["date", "country"],
        metrics=["totalUsers"],
        quota_project=quota_project,
    )
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "date": str(row.get("date", "")),
                "country": str(row.get("country", "")),
                "totalUsers": _safe_float(row.get("totalUsers")),
            }
        )
    return out


def _fetch_ga4_channel_range(
    access_token: str,
    ga4_property_id: str,
    start_day: date,
    end_day: date,
    quota_project: str | None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for chunk_start, chunk_end in _date_chunks(start_day, end_day, 31):
        rows = _fetch_ga4_run_report(
            access_token=access_token,
            ga4_property_id=ga4_property_id,
            start_day=chunk_start,
            end_day=chunk_end,
            dimensions=["date", "sessionDefaultChannelGroup", "sessionSourceMedium"],
            metrics=[
                "sessions",
                "totalUsers",
                "conversions",
                "averageSessionDuration",
                "engagementRate",
                "bounceRate",
            ],
            quota_project=quota_project,
        )
        for row in rows:
            out.append(
                {
                    "date": str(row.get("date", "")),
                    "sessionDefaultChannelGroup": str(
                        row.get("sessionDefaultChannelGroup", "")
                    ),
                    "sessionSourceMedium": str(row.get("sessionSourceMedium", "")),
                    "sessions": _safe_float(row.get("sessions")),
                    "totalUsers": _safe_float(row.get("totalUsers")),
                    "conversions": _safe_float(row.get("conversions")),
                    "averageSessionDuration": _safe_float(row.get("averageSessionDuration")),
                    "engagementRate": _safe_float(row.get("engagementRate")),
                    "bounceRate": _safe_float(row.get("bounceRate")),
                }
            )
    return out


def _fetch_ga4_top_pages_range(
    access_token: str,
    ga4_property_id: str,
    start_day: date,
    end_day: date,
    quota_project: str | None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for chunk_start, chunk_end in _date_chunks(start_day, end_day, 14):
        rows = _fetch_ga4_run_report(
            access_token=access_token,
            ga4_property_id=ga4_property_id,
            start_day=chunk_start,
            end_day=chunk_end,
            dimensions=["date", "pagePath", "pageTitle"],
            metrics=["screenPageViews", "sessions", "averageSessionDuration"],
            quota_project=quota_project,
        )
        for row in rows:
            out.append(
                {
                    "date": str(row.get("date", "")),
                    "pagePath": str(row.get("pagePath", "")),
                    "pageTitle": str(row.get("pageTitle", "")),
                    "screenPageViews": _safe_float(row.get("screenPageViews")),
                    "sessions": _safe_float(row.get("sessions")),
                    "averageSessionDuration": _safe_float(row.get("averageSessionDuration")),
                }
            )
    return out


def _fetch_ga4_event_range(
    access_token: str,
    ga4_property_id: str,
    start_day: date,
    end_day: date,
    quota_project: str | None,
    *,
    event_name: str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for chunk_start, chunk_end in _date_chunks(start_day, end_day, 31):
        rows = _fetch_ga4_run_report(
            access_token=access_token,
            ga4_property_id=ga4_property_id,
            start_day=chunk_start,
            end_day=chunk_end,
            dimensions=["date", "eventName", "sessionSourceMedium"],
            metrics=["eventCount"],
            quota_project=quota_project,
            dimension_filter={
                "filter": {
                    "fieldName": "eventName",
                    "stringFilter": {"matchType": "EXACT", "value": event_name},
                }
            },
        )
        for row in rows:
            out.append(
                {
                    "date": str(row.get("date", "")),
                    "eventName": str(row.get("eventName", "")),
                    "sessionSourceMedium": str(row.get("sessionSourceMedium", "")),
                    "platform": _source_medium_to_platform(
                        str(row.get("sessionSourceMedium", ""))
                    ),
                    "eventCount": _safe_float(row.get("eventCount")),
                    # Alias to keep dashboard/BI language consistent.
                    "conversions": _safe_float(row.get("eventCount")),
                }
            )
    return out


def _day_keys(start_day: date, end_day: date) -> List[str]:
    keys: List[str] = []
    current = start_day
    while current <= end_day:
        keys.append(current.isoformat())
        current += timedelta(days=1)
    return keys


def _build_daily_rows(
    *,
    start_day: date,
    end_day: date,
    meta_data: Dict[str, Dict[str, float]],
    google_data: Dict[str, Dict[str, float]],
    ga4_data: Dict[str, Dict[str, float]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for day in _day_keys(start_day, end_day):
        meta = meta_data.get(
            day,
            {
                "spend": 0.0,
                "clicks": 0.0,
                "conversions": 0.0,
                "conversions_raw": 0.0,
                "conversions_lead": 0.0,
                "conversions_fb_pixel_lead": 0.0,
                "impressions": 0.0,
                "reach": 0.0,
                "frequency": 0.0,
                "ctr": 0.0,
                "cpc": 0.0,
            },
        )
        google = google_data.get(
            day,
            {
                "cost_micros": 0.0,
                "cost": 0.0,
                "clicks": 0.0,
                "impressions": 0.0,
                "ctr": 0.0,
                "average_cpc_micros": 0.0,
                "average_cpc": 0.0,
                "conversions": 0.0,
            },
        )
        ga4 = ga4_data.get(
            day,
            {
                "averageSessionDuration": 0.0,
                "totalUsers": 0.0,
                "sessions": 0.0,
                "engagementRate": 0.0,
                "bounceRate": 0.0,
            },
        )

        total_spend = _safe_float(meta["spend"]) + _safe_float(google["cost"])
        total_conversions = _safe_float(meta["conversions"]) + _safe_float(google["conversions"])
        total_clicks = _safe_float(meta["clicks"]) + _safe_float(google["clicks"])
        total_impressions = _safe_float(meta["impressions"]) + _safe_float(google["impressions"])

        rows.append(
            {
                "date": day,
                "meta": {
                    "spend": _safe_float(meta["spend"]),
                    "clicks": _safe_float(meta["clicks"]),
                    "conversions": _safe_float(meta["conversions"]),
                    "conversions_raw": _safe_float(meta["conversions_raw"]),
                    "conversions_lead": _safe_float(meta["conversions_lead"]),
                    "conversions_fb_pixel_lead": _safe_float(meta["conversions_fb_pixel_lead"]),
                    "impressions": _safe_float(meta["impressions"]),
                    "reach": _safe_float(meta["reach"]),
                    "frequency": _safe_float(meta["frequency"]),
                    "ctr": _safe_float(meta["ctr"]),
                    "cpc": _safe_float(meta["cpc"]),
                },
                "google_ads": {
                    "cost_micros": _safe_float(google["cost_micros"]),
                    "cost": _safe_float(google["cost"]),
                    "clicks": _safe_float(google["clicks"]),
                    "impressions": _safe_float(google["impressions"]),
                    "ctr": _safe_float(google["ctr"]),
                    "average_cpc_micros": _safe_float(google["average_cpc_micros"]),
                    "average_cpc": _safe_float(google["average_cpc"]),
                    "conversions": _safe_float(google["conversions"]),
                },
                "ga4": {
                    "averageSessionDuration": _safe_float(ga4["averageSessionDuration"]),
                    "totalUsers": _safe_float(ga4["totalUsers"]),
                    "sessions": _safe_float(ga4["sessions"]),
                    "engagementRate": _safe_float(ga4["engagementRate"]),
                    "bounceRate": _safe_float(ga4["bounceRate"]),
                },
                "normalization": {
                    "total_spend": total_spend,
                    "total_conversions": total_conversions,
                    "total_clicks": total_clicks,
                    "total_impressions": total_impressions,
                    "ctr_consolidated": _safe_div(total_clicks, total_impressions),
                    "cpl_consolidated": _calc_cpl(total_spend, total_conversions),
                    "cpl_by_platform": {
                        "meta": _calc_cpl(
                            _safe_float(meta["spend"]), _safe_float(meta["conversions"])
                        ),
                        "google_ads": _calc_cpl(
                            _safe_float(google["cost"]), _safe_float(google["conversions"])
                        ),
                    },
                },
            }
        )
    return rows


def _merge_daily(existing: List[Dict[str, Any]], new_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_date = {str(r["date"]): r for r in existing}
    for row in new_rows:
        by_date[str(row["date"])] = row
    merged = list(by_date.values())
    merged.sort(key=lambda r: str(r["date"]))
    return merged


def _merge_breakdown_rows(
    *,
    existing: List[Dict[str, Any]],
    new_rows: List[Dict[str, Any]],
    start_day: date,
    end_day: date,
    key_fields: Tuple[str, ...],
) -> List[Dict[str, Any]]:
    start_s = start_day.isoformat()
    end_s = end_day.isoformat()

    filtered = [
        row
        for row in existing
        if not (start_s <= str(row.get("date", "")) <= end_s)
    ]
    merged = filtered + new_rows

    dedup: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for row in merged:
        key = tuple(row.get(field) for field in key_fields)
        dedup[key] = row

    out = list(dedup.values())
    out.sort(key=lambda r: tuple(str(r.get(f, "")) for f in key_fields))
    return out


def _summarize(daily_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_spend = sum(_safe_float(r["normalization"]["total_spend"]) for r in daily_rows)
    total_conversions = sum(_safe_float(r["normalization"]["total_conversions"]) for r in daily_rows)
    total_clicks = sum(_safe_float(r["normalization"].get("total_clicks")) for r in daily_rows)
    total_impressions = sum(
        _safe_float(r["normalization"].get("total_impressions")) for r in daily_rows
    )
    meta_spend = sum(_safe_float(r["meta"]["spend"]) for r in daily_rows)
    google_spend = sum(_safe_float(r["google_ads"]["cost"]) for r in daily_rows)

    return {
        "spend_accumulated": total_spend,
        "conversions_accumulated": total_conversions,
        "clicks_accumulated": total_clicks,
        "impressions_accumulated": total_impressions,
        "cpl_average": _calc_cpl(total_spend, total_conversions),
        "ctr_average": _safe_div(total_clicks, total_impressions),
        "spend_by_platform": {"meta": meta_spend, "google_ads": google_spend},
    }


def _load_existing(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_range(
    mode: str,
    output_path: Path,
    bootstrap_start: date,
    anchor_end: date,
    refresh_lookback_days: int = 1,
) -> Tuple[date, date, str]:
    end_day = anchor_end
    if mode == "bootstrap":
        return bootstrap_start, end_day, "bootstrap"
    if mode == "daily":
        return end_day, end_day, "daily"

    # auto
    if not output_path.exists():
        return bootstrap_start, end_day, "bootstrap"

    existing = _load_existing(output_path)
    daily = existing.get("daily", [])
    if not daily:
        return bootstrap_start, end_day, "bootstrap"

    last_date_str = max(str(r.get("date")) for r in daily if r.get("date"))
    last_day = datetime.strptime(last_date_str, "%Y-%m-%d").date()
    lookback = max(int(refresh_lookback_days), 0)
    overlap_start = end_day - timedelta(days=lookback)
    if overlap_start < bootstrap_start:
        overlap_start = bootstrap_start
    start = min(last_day + timedelta(days=1), overlap_start)
    if start > end_day:
        start = end_day
    return start, end_day, "daily"


def _parse_args() -> argparse.Namespace:
    current_year_start = date.today().replace(month=1, day=1).isoformat()
    default_end = date.today().isoformat()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tenant-id",
        default="yap",
        help="Tenant id defined in config/tenants.json (default: yap).",
    )
    parser.add_argument(
        "--tenants-config-path",
        default=str(TENANTS_CONFIG_PATH),
        help="Path to tenants JSON config.",
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "bootstrap", "daily"],
        default="auto",
        help="auto: bootstrap first run, then increment daily.",
    )
    parser.add_argument(
        "--bootstrap-start",
        default=current_year_start,
        help="Start date for bootstrap mode (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Output JSON path. If omitted, uses tenant config path.",
    )
    parser.add_argument(
        "--organic-output-path",
        default=None,
        help="Output JSON path for organic module. If omitted, uses tenant config path.",
    )
    parser.add_argument(
        "--organic-lookback-days",
        type=int,
        default=DEFAULT_ORGANIC_LOOKBACK_DAYS,
        help="Days lookback window for organic extraction.",
    )
    parser.add_argument(
        "--end-date",
        default=default_end,
        help="End date for extraction range (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--refresh-lookback-days",
        type=int,
        default=1,
        help="For mode=auto, reprocess this many days before end-date to absorb late platform adjustments.",
    )
    parser.add_argument(
        "--meta-ad-account-id",
        default=None,
        help="Override Meta ad account id (with or without act_ prefix).",
    )
    parser.add_argument(
        "--google-ads-customer-id",
        default=None,
        help="Override Google Ads customer id for data extraction.",
    )
    parser.add_argument(
        "--google-ads-login-customer-id",
        default=None,
        help="Override Google Ads login customer id (MCC).",
    )
    parser.add_argument(
        "--ga4-property-id",
        default=None,
        help="Override GA4 property id for Data API reports.",
    )
    return parser.parse_args()


def _resolve_meta_app_credentials(cfg: Dict[str, Any]) -> Tuple[str | None, str | None]:
    meta_env = cfg.get("mcp_servers", {}).get("meta-ads-mcp", {}).get("env", {})
    app_id = (
        meta_env.get("META_APP_ID")
        or os.getenv("META_APP_ID")
        or _read_windows_user_env("META_APP_ID")
    )
    app_secret = (
        meta_env.get("META_APP_SECRET")
        or os.getenv("META_APP_SECRET")
        or _read_windows_user_env("META_APP_SECRET")
    )
    return (
        str(app_id) if app_id else None,
        str(app_secret) if app_secret else None,
    )


def main() -> int:
    args = _parse_args()
    tenants_config_path = _resolve_repo_path(args.tenants_config_path)
    tenant_cfg = _resolve_tenant_config(
        tenant_id=str(args.tenant_id),
        tenants_config_path=tenants_config_path,
    )
    tenant_id = str(tenant_cfg.get("id", str(args.tenant_id))).strip().lower()
    tenant_name = str(tenant_cfg.get("name", tenant_id.upper()))
    output_path = (
        _resolve_repo_path(args.output_path)
        if args.output_path
        else _resolve_repo_path(tenant_cfg.get("report_path", DEFAULT_OUTPUT_PATH))
    )
    organic_output_path = (
        _resolve_repo_path(args.organic_output_path)
        if args.organic_output_path
        else _resolve_repo_path(tenant_cfg.get("organic_report_path", DEFAULT_ORGANIC_OUTPUT_PATH))
    )
    tenant_meta_ad_account_id = str(
        args.meta_ad_account_id or tenant_cfg.get("meta_ad_account_id", META_AD_ACCOUNT_ID)
    )
    tenant_google_ads_customer_id = str(
        args.google_ads_customer_id
        or tenant_cfg.get("google_ads_customer_id", GOOGLE_ADS_CUSTOMER_ID)
    )
    tenant_ga4_property_raw = (
        args.ga4_property_id
        if args.ga4_property_id is not None
        else tenant_cfg.get("ga4_property_id", GA4_PROPERTY_ID)
    )
    tenant_ga4_property_id = str(tenant_ga4_property_raw or "").strip()
    tenant_ga4_conversion_event_name = (
        str(tenant_cfg.get("ga4_conversion_event_name", GA4_DEFAULT_CONVERSION_EVENT)).strip()
        or GA4_DEFAULT_CONVERSION_EVENT
    )
    ga4_oauth_path = _resolve_repo_path(tenant_cfg.get("ga4_oauth_path", GA4_OAUTH_PATH))
    bootstrap_start = datetime.strptime(args.bootstrap_start, "%Y-%m-%d").date()
    anchor_end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    organic_lookback_days = max(int(args.organic_lookback_days), 1)
    organic_start_day = anchor_end - timedelta(days=organic_lookback_days - 1)
    organic_end_day = anchor_end
    start_day, end_day, run_kind = _resolve_range(
        args.mode,
        output_path,
        bootstrap_start,
        anchor_end,
        args.refresh_lookback_days,
    )

    cfg = _load_codex_config(CODEX_CONFIG_PATH)
    meta_env = cfg.get("mcp_servers", {}).get("meta-ads-mcp", {}).get("env", {})
    meta_token = meta_env.get("META_ADS_ACCESS_TOKEN")
    meta_ad_account_id = _normalize_meta_ad_account_id(
        tenant_meta_ad_account_id or meta_env.get("META_AD_ACCOUNT_ID", META_AD_ACCOUNT_ID)
    )
    if not meta_token:
        raise RuntimeError("META_ADS_ACCESS_TOKEN not found in ~/.codex/config.toml")
    meta_app_id, meta_app_secret = _resolve_meta_app_credentials(cfg)

    google_env = cfg.get("mcp_servers", {}).get("google-ads-mcp", {}).get("env", {})
    ga_client_id = google_env.get("GOOGLE_ADS_CLIENT_ID")
    ga_client_secret = google_env.get("GOOGLE_ADS_CLIENT_SECRET")
    ga_refresh_token = google_env.get("GOOGLE_ADS_REFRESH_TOKEN")
    ga_developer_token = google_env.get("GOOGLE_ADS_DEVELOPER_TOKEN")
    ga_login_customer_id = (
        args.google_ads_login_customer_id
        or tenant_cfg.get("google_ads_login_customer_id")
        or google_env.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
        or tenant_google_ads_customer_id
    )
    ga_quota_project = google_env.get("GOOGLE_ADS_QUOTA_PROJECT")
    if not all([ga_client_id, ga_client_secret, ga_refresh_token, ga_developer_token]):
        raise RuntimeError("Missing Google Ads OAuth/developer token values in ~/.codex/config.toml")

    meta_data = _fetch_meta_range(str(meta_token), meta_ad_account_id, start_day, end_day)
    meta_campaign_daily = _fetch_meta_campaign_range(
        str(meta_token), meta_ad_account_id, start_day, end_day
    )
    meta_device_daily = _fetch_meta_device_range(
        str(meta_token), meta_ad_account_id, start_day, end_day
    )
    meta_lead_demographics_daily = _fetch_meta_lead_demographics_range(
        str(meta_token), meta_ad_account_id, start_day, end_day
    )
    meta_lead_geo_daily = _fetch_meta_lead_geo_range(
        str(meta_token), meta_ad_account_id, start_day, end_day
    )
    meta_token_status = _fetch_meta_token_status(str(meta_token), meta_app_id, meta_app_secret)

    google_ads_access_token = _google_access_token(
        str(ga_client_id), str(ga_client_secret), str(ga_refresh_token)
    )
    google_data = _fetch_google_ads_range(
        access_token=google_ads_access_token,
        developer_token=str(ga_developer_token),
        customer_id=tenant_google_ads_customer_id,
        login_customer_id=str(ga_login_customer_id),
        quota_project=str(ga_quota_project) if ga_quota_project else None,
        start_day=start_day,
        end_day=end_day,
    )
    google_campaign_daily = _fetch_google_campaign_range(
        access_token=google_ads_access_token,
        developer_token=str(ga_developer_token),
        customer_id=tenant_google_ads_customer_id,
        login_customer_id=str(ga_login_customer_id),
        quota_project=str(ga_quota_project) if ga_quota_project else None,
        start_day=start_day,
        end_day=end_day,
    )
    google_device_daily = _fetch_google_device_range(
        access_token=google_ads_access_token,
        developer_token=str(ga_developer_token),
        customer_id=tenant_google_ads_customer_id,
        login_customer_id=str(ga_login_customer_id),
        quota_project=str(ga_quota_project) if ga_quota_project else None,
        start_day=start_day,
        end_day=end_day,
    )
    google_lead_demographics_daily = _fetch_google_lead_demographics_range(
        access_token=google_ads_access_token,
        developer_token=str(ga_developer_token),
        customer_id=tenant_google_ads_customer_id,
        login_customer_id=str(ga_login_customer_id),
        quota_project=str(ga_quota_project) if ga_quota_project else None,
        start_day=start_day,
        end_day=end_day,
    )
    google_lead_geo_daily = _fetch_google_lead_geo_range(
        access_token=google_ads_access_token,
        developer_token=str(ga_developer_token),
        customer_id=tenant_google_ads_customer_id,
        login_customer_id=str(ga_login_customer_id),
        quota_project=str(ga_quota_project) if ga_quota_project else None,
        start_day=start_day,
        end_day=end_day,
    )

    ga4_main: Dict[str, Dict[str, float]] = {}
    ga4_device: List[Dict[str, Any]] = []
    ga4_country: List[Dict[str, Any]] = []
    ga4_channel: List[Dict[str, Any]] = []
    ga4_top_pages: List[Dict[str, Any]] = []
    ga4_event_daily: List[Dict[str, Any]] = []
    if tenant_ga4_property_id:
        (
            ga4_client_id,
            ga4_client_secret,
            ga4_refresh_token,
            ga4_quota_project,
        ) = _load_ga4_oauth_credentials(ga4_oauth_path)
        ga4_access_token = _google_access_token(
            ga4_client_id, ga4_client_secret, ga4_refresh_token
        )
        ga4_main = _fetch_ga4_main_range(
            ga4_access_token, tenant_ga4_property_id, start_day, end_day, ga4_quota_project
        )
        ga4_device = _fetch_ga4_device_range(
            ga4_access_token, tenant_ga4_property_id, start_day, end_day, ga4_quota_project
        )
        ga4_country = _fetch_ga4_country_range(
            ga4_access_token, tenant_ga4_property_id, start_day, end_day, ga4_quota_project
        )
        ga4_channel = _fetch_ga4_channel_range(
            ga4_access_token, tenant_ga4_property_id, start_day, end_day, ga4_quota_project
        )
        ga4_top_pages = _fetch_ga4_top_pages_range(
            ga4_access_token, tenant_ga4_property_id, start_day, end_day, ga4_quota_project
        )
        ga4_event_daily = _fetch_ga4_event_range(
            ga4_access_token,
            tenant_ga4_property_id,
            start_day,
            end_day,
            ga4_quota_project,
            event_name=tenant_ga4_conversion_event_name,
        )
    else:
        print("GA4 skipped: tenant has no ga4_property_id configured.")

    new_daily = _build_daily_rows(
        start_day=start_day, end_day=end_day, meta_data=meta_data, google_data=google_data, ga4_data=ga4_main
    )

    existing = _load_existing(output_path)
    all_daily = _merge_daily(existing.get("daily", []), new_daily)
    all_device = _merge_breakdown_rows(
        existing=existing.get("ga4_breakdowns", {}).get("device_daily", []),
        new_rows=ga4_device,
        start_day=start_day,
        end_day=end_day,
        key_fields=("date", "deviceCategory"),
    )
    all_country = _merge_breakdown_rows(
        existing=existing.get("ga4_breakdowns", {}).get("country_daily", []),
        new_rows=ga4_country,
        start_day=start_day,
        end_day=end_day,
        key_fields=("date", "country"),
    )
    all_ga4_channel = _merge_breakdown_rows(
        existing=existing.get("traffic_acquisition", {}).get("ga4_channel_daily", []),
        new_rows=ga4_channel,
        start_day=start_day,
        end_day=end_day,
        key_fields=("date", "sessionDefaultChannelGroup", "sessionSourceMedium"),
    )
    all_ga4_top_pages = _merge_breakdown_rows(
        existing=existing.get("traffic_acquisition", {}).get("ga4_top_pages_daily", []),
        new_rows=ga4_top_pages,
        start_day=start_day,
        end_day=end_day,
        key_fields=("date", "pagePath"),
    )
    all_ga4_event_daily = _merge_breakdown_rows(
        existing=existing.get("traffic_acquisition", {}).get("ga4_event_daily", []),
        new_rows=ga4_event_daily,
        start_day=start_day,
        end_day=end_day,
        key_fields=("date", "eventName", "sessionSourceMedium"),
    )
    all_meta_campaign_daily = _merge_breakdown_rows(
        existing=existing.get("traffic_acquisition", {}).get("meta_campaign_daily", []),
        new_rows=meta_campaign_daily,
        start_day=start_day,
        end_day=end_day,
        key_fields=("date", "campaign_id"),
    )
    all_google_campaign_daily = _merge_breakdown_rows(
        existing=existing.get("traffic_acquisition", {}).get("google_campaign_daily", []),
        new_rows=google_campaign_daily,
        start_day=start_day,
        end_day=end_day,
        key_fields=("date", "campaign_id"),
    )
    all_paid_device_daily = _merge_breakdown_rows(
        existing=existing.get("traffic_acquisition", {}).get("paid_device_daily", []),
        new_rows=meta_device_daily + google_device_daily,
        start_day=start_day,
        end_day=end_day,
        key_fields=("date", "platform", "device"),
    )
    all_paid_lead_demographics_daily = _merge_breakdown_rows(
        existing=existing.get("traffic_acquisition", {}).get("paid_lead_demographics_daily", []),
        new_rows=meta_lead_demographics_daily + google_lead_demographics_daily,
        start_day=start_day,
        end_day=end_day,
        key_fields=("date", "platform", "breakdown", "age_range", "gender"),
    )
    all_paid_lead_geo_daily = _merge_breakdown_rows(
        existing=existing.get("traffic_acquisition", {}).get("paid_lead_geo_daily", []),
        new_rows=meta_lead_geo_daily + google_lead_geo_daily,
        start_day=start_day,
        end_day=end_day,
        key_fields=("date", "platform", "country_code", "region"),
    )

    report = {
        "metadata": {
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "run_kind": run_kind,
            "tenant": {"id": tenant_id, "name": tenant_name},
            "updated_range": {"start": start_day.isoformat(), "end": end_day.isoformat()},
            "meta_token_status": meta_token_status,
            "ids": {
                "meta_ad_account_id": meta_ad_account_id,
                "google_ads_customer_id": tenant_google_ads_customer_id,
                "ga4_property_id": tenant_ga4_property_id,
            },
            "ga4_conversion_event_name": tenant_ga4_conversion_event_name,
        },
        "daily": all_daily,
        "ga4_breakdowns": {
            "device_daily": all_device,
            "country_daily": all_country,
        },
        "traffic_acquisition": {
            "ga4_channel_daily": all_ga4_channel,
            "ga4_top_pages_daily": all_ga4_top_pages,
            "ga4_event_daily": all_ga4_event_daily,
            "meta_campaign_daily": all_meta_campaign_daily,
            "google_campaign_daily": all_google_campaign_daily,
            "paid_device_daily": all_paid_device_daily,
            "paid_lead_demographics_daily": all_paid_lead_demographics_daily,
            "paid_lead_geo_daily": all_paid_lead_geo_daily,
        },
        "summary_all_time": _summarize(all_daily),
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(
        f"Report updated: {output_path} | tenant={tenant_id} | run_kind={run_kind} | "
        f"range={start_day.isoformat()}..{end_day.isoformat()}"
    )

    organic_existing = _load_existing(organic_output_path)
    organic_report = _build_organic_report(
        existing=organic_existing,
        cfg=cfg,
        meta_token=str(meta_token),
        meta_ad_account_id=meta_ad_account_id,
        start_day=organic_start_day,
        end_day=organic_end_day,
        run_kind=run_kind,
    )
    organic_output_path.parent.mkdir(parents=True, exist_ok=True)
    organic_output_path.write_text(
        json.dumps(organic_report, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(
        f"Organic report updated: {organic_output_path} | tenant={tenant_id} | "
        f"range={organic_start_day.isoformat()}..{organic_end_day.isoformat()}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
