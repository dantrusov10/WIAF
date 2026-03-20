import os
import re
import json
import hashlib
from datetime import datetime, timezone
from urllib.parse import urljoin
from email.utils import parsedate_to_datetime

import requests
import feedparser
from bs4 import BeautifulSoup, FeatureNotFound
from dateutil import parser as date_parser

from sources import SOURCES

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "public", "data")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; WIAFBot/1.1; +https://wiaf.ru)",
    "Accept-Language": "ru,en;q=0.8",
}
TIMEOUT = 20


def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_existing(filename: str) -> dict:
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_json(filename: str, payload: dict):
    ensure_data_dir()
    with open(os.path.join(DATA_DIR, filename), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def normalize_text(value: str) -> str:
    if not value:
        return ""
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def make_hash(*parts: str) -> str:
    raw = "|".join([normalize_text(p or "") for p in parts])
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def parse_date(value: str) -> str:
    if not value:
        return ""
    try:
        parsed = date_parser.parse(str(value))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc)
        return parsed.replace(microsecond=0).isoformat()
    except Exception:
        try:
            parsed = parsedate_to_datetime(str(value))
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone.utc)
            return parsed.replace(microsecond=0).isoformat()
        except Exception:
            return ""


def sort_key_date(value: str) -> int:
    if not value:
        return 0
    try:
        norm = value if "T" in value else value + "T00:00:00"
        return int(datetime.fromisoformat(norm.replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0


def localize_category(category: str) -> str:
    mapping = {
        "news": "новости",
        "analytics": "аналитика",
        "rates": "ставки",
        "ports": "порты",
        "rail": "железная дорога",
        "road": "автологистика",
        "air": "авиа",
        "sea": "морская логистика",
        "regulation": "регулирование",
        "operator_news": "новости операторов",
        "infrastructure": "инфраструктура",
        "restrictions": "ограничения",
    }
    return mapping.get(category or "news", "новости")


def category_by_text(text: str, default_category: str) -> str:
    t = (text or "").lower()
    if any(word in t for word in ["ставк", "tariff", "rate", "freight", "scfi", "wci"]):
        return "rates"
    if any(word in t for word in ["порт", "port", "terminal", "берег", "пристан"]):
        return "ports"
    if any(word in t for word in ["rail", "жд", "railway", "поезд", "контрейлер"]):
        return "rail"
    if any(word in t for word in ["авто", "truck", "road", "дорог"]):
        return "road"
    if any(word in t for word in ["аналит", "analysis", "review", "обзор"]):
        return "analytics"
    if any(word in t for word in ["тамож", "customs", "фтс", "санкц", "регулир"]):
        return "regulation"
    if any(word in t for word in ["море", "sea", "судоход", "судн", "контейнерн"]):
        return "sea"
    if any(word in t for word in ["мультимод", "transit", "corridor", "коридор"]):
        return "multimodal"
    return default_category or "news"


def transport_by_text(text: str) -> str:
    t = (text or "").lower()
    if any(word in t for word in ["container", "мор", "sea", "vessel", "port", "feu", "teu", "линия", "судно"]):
        return "sea"
    if any(word in t for word in ["rail", "жд", "railway", "train", "вагон"]):
        return "rail"
    if any(word in t for word in ["truck", "авто", "road", "грузов"]):
        return "road"
    if any(word in t for word in ["air", "авиа", "cargo terminal"]):
        return "air"
    if any(word in t for word in ["multimodal", "мультимод"]):
        return "multimodal"
    return "unknown"


def freshness_label(date_str: str) -> str:
    if not date_str:
        return "unknown"
    try:
        norm = date_str if "T" in date_str else date_str + "T00:00:00"
        dt = datetime.fromisoformat(norm.replace("Z", "+00:00"))
    except ValueError:
        return "unknown"
    delta_hours = (datetime.now() - dt).total_seconds() / 3600
    if delta_hours <= 48:
        return "fresh"
    if delta_hours <= 168:
        return "recent"
    return "archive"


def make_soup(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")


def fetch_html(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.text


def item_from_fields(source: dict, title: str, link: str, snippet: str = "", published_at: str = "", image_url: str = "", content_preview: str = "") -> dict | None:
    title = normalize_text(title)
    link = (link or "").strip()
    snippet = normalize_text(snippet)
    if not title or not link or link.startswith("javascript:"):
        return None
    item_hash = make_hash(source["source_name"], title, link)
    category = category_by_text(f"{title} {snippet} {content_preview}", source.get("category_default", "news"))
    return {
        "id": f"news_{item_hash[:12]}",
        "source": source["source_name"],
        "source_id": source["source_id"],
        "source_group": source.get("source_group", "other"),
        "lang": source.get("language", ""),
        "priority": source.get("priority", 0),
        "date": published_at,
        "published_at": published_at,
        "title": title,
        "snippet": snippet[:350],
        "content_preview": normalize_text(content_preview or snippet)[:700],
        "link": link,
        "image_url": (image_url or "").strip(),
        "category": category,
        "category_ru": localize_category(category),
        "transport_type": transport_by_text(f"{title} {snippet}"),
        "region": source.get("country", "unknown"),
        "freshness": freshness_label(published_at),
        "hash": item_hash,
        "status": "published",
    }


def fetch_rss_items(source: dict, url: str | None = None) -> list[dict]:
    url = url or source["list_url"]
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    feed = feedparser.parse(resp.content)
    items = []
    for entry in getattr(feed, "entries", [])[:40]:
        image_url = ""
        media_content = getattr(entry, "media_content", None) or []
        media_thumbnail = getattr(entry, "media_thumbnail", None) or []
        links = getattr(entry, "links", None) or []
        if media_content and isinstance(media_content, list):
            image_url = media_content[0].get("url", "")
        elif media_thumbnail and isinstance(media_thumbnail, list):
            image_url = media_thumbnail[0].get("url", "")
        else:
            for link_obj in links:
                if getattr(link_obj, 'type', '').startswith('image/'):
                    image_url = getattr(link_obj, 'href', '')
                    break
        raw_summary = getattr(entry, "summary", "") or getattr(entry, "description", "")
        item = item_from_fields(
            source,
            getattr(entry, "title", ""),
            getattr(entry, "link", ""),
            raw_summary,
            parse_date(getattr(entry, "published", "") or getattr(entry, "updated", "")),
            image_url=image_url,
            content_preview=raw_summary,
        )
        if item:
            items.append(item)
    return items


def pick_first(node, selector: str):
    try:
        return node.select_one(selector)
    except Exception:
        return None


def extract_generic_links(soup: BeautifulSoup, source: dict, base_url: str) -> list[dict]:
    items = []
    seen = set()
    for a in soup.select("a[href]"):
        href = urljoin(base_url, a.get("href", ""))
        title = normalize_text(a.get_text(" ", strip=True))
        if not title or len(title) < 25:
            continue
        if href in seen:
            continue
        lower = href.lower()
        if not any(token in lower for token in ["news", "press", "article", "novost", "media", "post"]):
            continue
        seen.add(href)
        item = item_from_fields(source, title, href, content_preview=title)
        if item:
            items.append(item)
        if len(items) >= 20:
            break
    return items


def fetch_html_items(source: dict, url: str | None = None) -> list[dict]:
    url = url or source["list_url"]
    html = fetch_html(url)
    soup = make_soup(html)
    items = []
    selector = source.get("article_selector", "article")
    nodes = soup.select(selector)
    for node in nodes[:40]:
        title_node = pick_first(node, source.get("title_selector", "h2 a, h3 a, h2, h3"))
        link_node = pick_first(node, source.get("link_selector", "a[href]"))
        date_node = pick_first(node, source.get("date_selector", "time, .date"))
        snippet_node = pick_first(node, source.get("snippet_selector", "p"))
        title = title_node.get_text(" ", strip=True) if title_node else ""
        href = urljoin(url, link_node.get("href", "") if link_node else "")
        published_at = parse_date(normalize_text(date_node.get_text(" ", strip=True) if date_node else ""))
        snippet = snippet_node.get_text(" ", strip=True) if snippet_node else ""
        image_node = pick_first(node, source.get("image_selector", "img"))
        image_url = urljoin(url, image_node.get("src", "") if image_node else "")
        item = item_from_fields(source, title, href, snippet, published_at, image_url=image_url, content_preview=snippet)
        if item:
            items.append(item)
    if not items:
        items = extract_generic_links(soup, source, url)
    return items


def fetch_source_items(source: dict) -> tuple[list[dict], str, str]:
    attempts = []
    candidates = [(source.get("fetch_method", "rss"), source.get("list_url", ""))]
    for fallback in source.get("fallbacks", []):
        candidates.append((fallback.get("fetch_method", source.get("fetch_method", "html")), fallback.get("list_url", "")))
    errors = []
    for method, url in candidates:
        if not url:
            continue
        attempts.append(f"{method}:{url}")
        try:
            items = fetch_rss_items(source, url) if method == "rss" else fetch_html_items(source, url)
            if items:
                return items, method, url
        except Exception as e:
            errors.append(f"{method}:{url} -> {e}")
    return [], candidates[0][0], candidates[0][1], " | ".join(errors)


def merge_with_existing(new_items: list[dict]) -> tuple[list[dict], bool]:
    existing_payload = load_existing("news.json")
    existing_items = existing_payload.get("items", []) if isinstance(existing_payload, dict) else []
    existing_map = {item.get("hash") or make_hash(item.get("source", ""), item.get("title", ""), item.get("link", "")): item for item in existing_items}
    merged = []
    seen = set()
    for item in sorted(new_items + existing_items, key=lambda x: (sort_key_date(x.get('date') or x.get('published_at')), x.get('priority', 0)), reverse=True):
        key = item.get("hash") or make_hash(item.get("source", ""), item.get("title", ""), item.get("link", ""))
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)
    used_cache = not bool(new_items)
    return merged[:200], used_cache


def main():
    ensure_data_dir()
    all_news = []
    source_status = []
    active_sources = sorted(
        [s for s in SOURCES if s.get("enabled") and s.get("fetch_method") in ("rss", "html")],
        key=lambda x: x.get("priority", 0),
        reverse=True,
    )

    for source in active_sources:
        error_text = ""
        used_method = source.get("fetch_method", "")
        used_url = source.get("list_url", "")
        try:
            result = fetch_source_items(source)
            if len(result) == 4:
                items, used_method, used_url, error_text = result
            else:
                items, used_method, used_url = result
            all_news.extend(items)
            source_status.append({
                "source_id": source["source_id"],
                "source_name": source["source_name"],
                "fetch_method": used_method,
                "used_url": used_url,
                "priority": source.get("priority", 0),
                "enabled": source.get("enabled", False),
                "country": source.get("country", ""),
                "language": source.get("language", ""),
                "items_count": len(items),
                "status": "ok" if items else "empty",
                "checked_at": now_iso(),
                "error": error_text[:300],
            })
            print(f"{source['source_name']}: {len(items)}")
        except Exception as e:
            source_status.append({
                "source_id": source["source_id"],
                "source_name": source["source_name"],
                "fetch_method": source.get("fetch_method", ""),
                "used_url": source.get("list_url", ""),
                "priority": source.get("priority", 0),
                "enabled": source.get("enabled", False),
                "country": source.get("country", ""),
                "language": source.get("language", ""),
                "items_count": 0,
                "status": "error",
                "checked_at": now_iso(),
                "error": str(e)[:300],
            })
            print(f"ERROR in {source['source_name']}: {e}")

    unique = []
    seen = set()
    for item in sorted(all_news, key=lambda x: (sort_key_date(x.get('date') or x.get('published_at')), x.get('priority', 0)), reverse=True):
        key = item.get('hash')
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)

    merged_news, used_cache = merge_with_existing(unique)
    summary = {
        "updated_at": now_iso(),
        "active_sources": len(active_sources),
        "successful_sources": len([x for x in source_status if x["status"] in ("ok", "empty")]),
        "error_sources": len([x for x in source_status if x["status"] == "error"]),
        "fetched_items": len(unique),
        "published_items": len(merged_news),
        "cache_used": used_cache,
    }

    save_json("news.json", {"updated_at": now_iso(), "cache_used": used_cache, "items": merged_news})
    save_json("source_status.json", {"updated_at": now_iso(), "summary": summary, "items": source_status})
    print(f"Saved {len(merged_news)} news items from {len(active_sources)} active sources; fetched now={len(unique)}; cache_used={used_cache}")


if __name__ == "__main__":
    main()
