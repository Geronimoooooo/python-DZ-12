
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_BASE = "https://api.hh.ru"
USER_AGENT = "Mozilla/5.0"


@dataclass
class SalaryValue:
    amount: float
    currency: str


def api_get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    query = f"?{urlencode(params, doseq=True)}" if params else ""
    url = f"{API_BASE}{path}{query}"

    req = Request(url)
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Accept", "application/json")

    try:
        with urlopen(req, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {e.code} for {url}: {body[:500]}") from e
    except URLError as e:
        raise RuntimeError(f"Network error for {url}: {e}") from e


def normalize_skill(name: str) -> str:
    cleaned = re.sub(r"\s+", " ", name.strip())
    return cleaned.lower()


def parse_salary(salary: dict[str, Any] | None) -> SalaryValue | None:
    if not salary:
        return None

    low = salary.get("from")
    high = salary.get("to")
    currency = salary.get("currency")

    if currency is None:
        return None
    if low is None and high is None:
        return None

    if low is not None and high is not None:
        amount = (float(low) + float(high)) / 2
    elif low is not None:
        amount = float(low)
    else:
        amount = float(high)

    return SalaryValue(amount=amount, currency=str(currency))


def strip_html(text: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", unescape(no_tags)).strip()


def extract_fallback_skills(vacancy: dict[str, Any]) -> list[str]:
    # Fallback when key_skills is empty.
    raw = []
    snippet = vacancy.get("snippet") or {}
    for key in ("requirement", "responsibility"):
        value = snippet.get(key)
        if value:
            raw.append(strip_html(value))

    text = " ".join(raw).lower()
    candidates = re.findall(r"[a-zA-Zа-яА-Я0-9+.#-]{2,}", text)

    stopwords = {
        "и", "в", "на", "для", "с", "по", "от", "до", "или", "как",
        "the", "and", "for", "with", "you", "will", "have", "that", "this",
    }

    result = []
    for token in candidates:
        if token in stopwords:
            continue
        if token.isdigit():
            continue
        if len(token) <= 2:
            continue
        result.append(token)

    return result[:15]


def collect_vacancies(
    keyword: str,
    area: int | None,
    per_page: int,
    max_pages: int,
    delay: float,
) -> tuple[list[dict[str, Any]], int]:
    params: dict[str, Any] = {
        "text": keyword,
        "per_page": per_page,
        "page": 0,
    }
    if area is not None:
        params["area"] = area

    first_page = api_get("/vacancies", params=params)
    found = int(first_page.get("found", 0))
    pages_total = int(first_page.get("pages", 0))
    pages_to_read = min(pages_total, max_pages)

    items: list[dict[str, Any]] = list(first_page.get("items", []))

    for page in range(1, pages_to_read):
        params["page"] = page
        data = api_get("/vacancies", params=params)
        items.extend(data.get("items", []))
        if delay > 0:
            time.sleep(delay)

    return items, found


def analyze_keyword(
    keyword: str,
    area: int | None,
    per_page: int,
    max_pages: int,
    delay: float,
    include_fallback_skills: bool,
) -> dict[str, Any]:
    vacancies, found_total = collect_vacancies(
        keyword=keyword,
        area=area,
        per_page=per_page,
        max_pages=max_pages,
        delay=delay,
    )

    skills_counter: Counter[str] = Counter()
    salary_by_currency: dict[str, list[float]] = {}
    employers: Counter[str] = Counter()

    for item in vacancies:
        vac_id = item.get("id")
        if vac_id is None:
            continue

        details = api_get(f"/vacancies/{vac_id}")

        salary = parse_salary(details.get("salary"))
        if salary is not None:
            salary_by_currency.setdefault(salary.currency, []).append(salary.amount)

        employer = (details.get("employer") or {}).get("name")
        if employer:
            employers[str(employer)] += 1

        key_skills = details.get("key_skills") or []
        extracted = [normalize_skill(s.get("name", "")) for s in key_skills if s.get("name")]

        if not extracted and include_fallback_skills:
            extracted = [normalize_skill(s) for s in extract_fallback_skills(details)]

        skills_counter.update([s for s in extracted if s])

        if delay > 0:
            time.sleep(delay)

    total_skills_mentions = sum(skills_counter.values())

    requirements = []
    for skill, count in skills_counter.most_common():
        percent = (count / total_skills_mentions * 100) if total_skills_mentions else 0.0
        requirements.append(
            {
                "name": skill,
                "count": count,
                "percent": round(percent, 2),
            }
        )

    salary_stats: dict[str, float] = {}
    for currency, values in salary_by_currency.items():
        if values:
            salary_stats[currency] = round(sum(values) / len(values), 2)

    report: dict[str, Any] = {
        "keywords": keyword,
        "count": len(vacancies),
        "count_total_found_by_hh": found_total,
        "average_salary_by_currency": salary_stats,
        "requirements": requirements,
        "extra": {
            "top_employers": [
                {"name": name, "count": count}
                for name, count in employers.most_common(10)
            ],
            "vacancies_with_salary": sum(len(v) for v in salary_by_currency.values()),
            "unique_requirements": len(skills_counter),
        },
    }

    if len(salary_stats) == 1:
        only_currency = next(iter(salary_stats))
        report["average_salary"] = salary_stats[only_currency]
        report["salary_currency"] = only_currency

    return report


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HH API vacancy analytics")
    parser.add_argument(
        "--keywords",
        required=True,
        help="Comma-separated search phrases, e.g. 'python developer,data scientist'",
    )
    parser.add_argument(
        "--area",
        type=int,
        default=None,
        help="HH area id, e.g. 1 for Moscow",
    )
    parser.add_argument("--per-page", type=int, default=50, help="Vacancies per page (1..100)")
    parser.add_argument("--max-pages", type=int, default=20, help="Max pages to load")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.05,
        help="Delay between API requests in seconds",
    )
    parser.add_argument(
        "--without-fallback-skills",
        action="store_true",
        help="Use only key_skills from HH and skip snippet-based fallback extraction",
    )
    parser.add_argument("--output", default="hh_report.json", help="Output JSON file")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    if not (1 <= args.per_page <= 100):
        print("Error: --per-page must be in range 1..100", file=sys.stderr)
        return 2
    if args.max_pages < 1:
        print("Error: --max-pages must be >= 1", file=sys.stderr)
        return 2

    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    if not keywords:
        print("Error: at least one keyword is required", file=sys.stderr)
        return 2

    results = []
    for kw in keywords:
        result = analyze_keyword(
            keyword=kw,
            area=args.area,
            per_page=args.per_page,
            max_pages=args.max_pages,
            delay=args.delay,
            include_fallback_skills=not args.without_fallback_skills,
        )
        results.append(result)

    output_path = Path(args.output)
    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved report to: {output_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
