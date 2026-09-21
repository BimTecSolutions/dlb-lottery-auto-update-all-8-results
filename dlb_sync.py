"""
DLB results -> Firestore incremental sync (for GitHub Actions).

Every run:
  1. Reads the latest result of each lottery from dlb.lk
  2. Checks Firestore to see if that draw already exists
  3. If it is new (or its data changed, e.g. a second chance image was
     added later) -> writes it. If there is a gap since the last saved
     draw (missed runs) -> the missing draws are backfilled from the
     history pages.

Firestore layout:
  DLB_Results_All / {lottery_slug} / draws / {date}_{draw_number}

Credentials:
  - GitHub Actions: secret FIREBASE_SERVICE_ACCOUNT (full JSON text)
  - Local testing : ./serviceAccountKey.json
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


# ============================================================
# CONFIGURATION
# ============================================================

BASE_URL = "https://www.dlb.lk"

RESULT_URL = f"{BASE_URL}/result/1/en"

PAGINATION_URL = f"{BASE_URL}/result/pagination_re"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36"
    )
}

# Used only when a lottery has NO documents in Firestore yet
FALLBACK_START_DATE = "2026-03-01"

SERVICE_ACCOUNT_ENV = "FIREBASE_SERVICE_ACCOUNT"
SERVICE_ACCOUNT_PATH = "./serviceAccountKey.json"

FIRESTORE_ROOT_COLLECTION = "DLB_Results_All"
FIRESTORE_SUBCOLLECTION = "draws"
FIRESTORE_BATCH_SIZE = 400

MAX_HISTORY_PAGES = 300
REQUEST_DELAY = 0.3
MAX_RETRIES = 3


# ============================================================
# DLB LOTTERY IDs
# ============================================================

LOTTERIES = {
    11: "Ada Kotipathi",
    1: "Shanida",
    2: "Lagna Wasana",
    17: "Supiri Dhana Sampatha",
    3: "Super Ball",
    12: "Kapruka",
    13: "Sasiri",
    18: "Jaya Sampatha",
}

LAGNA_WASANA_ID = 2


def lottery_slug(name):
    """'Lagna Wasana' -> 'lagna_wasana' (used as Firestore doc id)."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


# ============================================================
# ZODIAC (LAGNA WASANA)
# ============================================================

ZODIAC_KEYWORDS = [
    ("mesha", "Aries"),
    ("mesa", "Aries"),
    ("wushaba", "Taurus"),
    ("rishaba", "Taurus"),
    ("vrushabha", "Taurus"),
    ("mithuna", "Gemini"),
    ("kataka", "Cancer"),
    ("karkataka", "Cancer"),
    ("sinha", "Leo"),
    ("simha", "Leo"),
    ("kanya", "Virgo"),
    ("thula", "Libra"),
    ("tula", "Libra"),
    ("wurshika", "Scorpio"),
    ("vrischika", "Scorpio"),
    ("vruschika", "Scorpio"),
    ("dhanu", "Sagittarius"),
    ("makara", "Capricorn"),
    ("kumbha", "Aquarius"),
    ("meena", "Pisces"),
    ("mina", "Pisces"),
]


# ============================================================
# HELPERS
# ============================================================

IMG_URL_RE = re.compile(
    r"""[^\s'"()<>]+?\.(?:jpe?g|png|gif|webp)""",
    re.IGNORECASE
)


def make_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def normalize_date(text):

    if not text:
        return None

    match = re.search(
        r"(\d{4})-([A-Za-z]{3})-(\d{1,2})",
        text
    )

    if not match:
        return None

    try:

        date = datetime.strptime(
            f"{match.group(1)}-{match.group(2)}-{int(match.group(3))}",
            "%Y-%b-%d"
        )

        return date.strftime("%Y-%m-%d")

    except ValueError:

        return None


def collect_image_urls(elements):

    urls = []

    for element in elements:

        if element is None:
            continue

        for img in element.find_all("img"):

            for attr in ("src", "data-src"):

                value = img.get(attr)

                if value:
                    urls.append(value)

        for tag in element.find_all(True):

            for attr in (
                "onclick",
                "href",
                "data-img",
                "data-image",
                "data-src"
            ):

                value = tag.get(attr)

                if value and isinstance(value, str):

                    urls.extend(
                        IMG_URL_RE.findall(value)
                    )

    unique = []

    for url in urls:

        full = urljoin(BASE_URL + "/", url.strip())

        if full not in unique:
            unique.append(full)

    return unique


def is_special_symbol(url):
    return "symbol_special" in url.lower()


def is_special_image(url):
    lowered = url.lower()
    return "special" in lowered and "symbol_special" not in lowered


def extract_zodiac(elements):

    for url in collect_image_urls(elements):

        if is_special_symbol(url) or is_special_image(url):
            continue

        if "front_img" not in url:
            continue

        file_name = os.path.basename(url.split("?")[0])

        stem = os.path.splitext(file_name)[0]

        key = re.sub(r"^\d+", "", stem).lower()

        name = None

        for keyword, english in ZODIAC_KEYWORDS:

            if keyword in key:
                name = english
                break

        return {
            "zodiac": name,
            "zodiac_key": key or None,
            "zodiac_image": url,
        }

    return {
        "zodiac": None,
        "zodiac_key": None,
        "zodiac_image": None,
    }


def extract_second_chance(elements):

    urls = collect_image_urls(elements)

    has_symbol = any(is_special_symbol(u) for u in urls)

    images = [u for u in urls if is_special_image(u)]

    if not has_symbol and not images:
        return None

    return {
        "available": True,
        "image_url": images[0] if images else None,
    }


# ============================================================
# FETCH MAIN DLB PAGE
# ============================================================

def fetch_main_page(session):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = session.get(
                RESULT_URL,
                timeout=30
            )

        except Exception as e:

            print(f"Request error (attempt {attempt}): {e}")

            time.sleep(2 * attempt)

            continue

        if response.status_code == 200:

            return response.text

        print(
            f"HTTP {response.status_code} (attempt {attempt})"
        )

        time.sleep(2 * attempt)

    return None


# ============================================================
# EXTRACT CURRENT LOTTERY RESULTS
# ============================================================

def extract_current_results(html):

    soup = BeautifulSoup(
        html,
        "html.parser"
    )

    results = {}

    for container in soup.select(".lot_main_result"):

        name_element = container.select_one(
            "h2.lot_m_re_heading"
        )

        date_element = container.select_one(
            "h3.lot_m_re_date"
        )

        if not name_element or not date_element:
            continue

        lottery_name = name_element.get_text(
            " ",
            strip=True
        )

        date_text = date_element.get_text(
            " ",
            strip=True
        )

        draw_match = re.search(
            r"Draw\s+Number\s*-\s*(\d+)",
            date_text,
            re.IGNORECASE
        )

        date_match = re.search(
            r"(\d{4}-[A-Za-z]{3}-\d{1,2})",
            date_text
        )

        if not draw_match or not date_match:
            continue

        date = normalize_date(
            date_match.group(1)
        )

        if not date:
            continue

        # LETTER

        letter = None

        letter_element = container.select_one(
            ".eng_letter"
        )

        if letter_element:

            letter = letter_element.get_text(
                strip=True
            ) or None

        # NUMBERS

        number_elements = container.select(
            ".number_shanida"
        )

        if not number_elements:

            number_elements = container.select(
                ".number_circle"
            )

        numbers = []

        for element in number_elements:

            value = element.get_text(
                strip=True
            )

            if value:

                # keep leading zeros
                numbers.append(value)

        # RESULT ID + LOTTERY ID

        result_id = None

        lottery_id = None

        input_element = container.select_one(
            'input[id^="resultID"]'
        )

        if input_element:

            result_id = input_element.get(
                "value"
            )

            match = re.search(
                r"resultID(\d+)",
                input_element.get("id", "")
            )

            if match:

                lottery_id = int(
                    match.group(1)
                )

        entry = {

            "lottery": lottery_name,

            "lottery_id": lottery_id,

            "draw_number": int(
                draw_match.group(1)
            ),

            "date": date,

            "letter": letter,

            "numbers": numbers,

            "result_id": result_id,

        }

        if "lagna" in lottery_name.lower():

            entry.update(
                extract_zodiac([container])
            )

        second_chance = extract_second_chance(
            [container]
        )

        if second_chance:
            entry["second_chance"] = second_chance

        results[lottery_name] = entry

    return results


# ============================================================
# HISTORY PAGES
# ============================================================

def fetch_history_page(
    session,
    page_id,
    result_id,
    lottery_id
):
    """List of draws, [] if no more pages, None if request failed."""

    data = {

        "pageId": page_id,

        "resultID": result_id,

        "lotteryID": lottery_id,

        "lastsegment": "en",

    }

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = session.post(
                PAGINATION_URL,
                data=data,
                timeout=30
            )

        except Exception as e:

            print(f"Request error (attempt {attempt}): {e}")

            time.sleep(2 * attempt)

            continue

        if response.status_code != 200:

            print(
                f"HTTP {response.status_code} (attempt {attempt})"
            )

            time.sleep(2 * attempt)

            continue

        return parse_history_html(
            response.text,
            lottery_id
        )

    return None


def parse_history_html(
    html,
    lottery_id
):

    # DLB returns <tr>...</tr> fragments

    soup = BeautifulSoup(
        f"<table><tbody>{html}</tbody></table>",
        "html.parser"
    )

    results = []

    for row in soup.find_all("tr"):

        cells = row.find_all("td")

        if len(cells) < 3:
            continue

        first_cell_text = cells[0].get_text(
            " ",
            strip=True
        )

        match = re.search(
            r"(\d+)\s*\|\s*"
            r"(\d{4}-[A-Za-z]{3}-\d{1,2})",
            first_cell_text
        )

        if not match:
            continue

        date = normalize_date(
            match.group(2)
        )

        if not date:
            continue

        result_cell = cells[2]

        letter = None

        letter_element = result_cell.select_one(
            ".res_eng_letter"
        )

        if letter_element:

            letter = letter_element.get_text(
                strip=True
            ) or None

        numbers = []

        for element in result_cell.select(
            ".res_number"
        ):

            value = element.get_text(
                strip=True
            )

            if value:
                numbers.append(value)

        result_id = None

        more_link = row.select_one(
            "a[onclick*='more_result']"
        )

        if more_link:

            result_match = re.search(
                r"more_result\(\s*\d+\s*,\s*(\d+)",
                more_link.get("onclick", "")
            )

            if result_match:

                result_id = result_match.group(1)

        entry = {

            "lottery_id": lottery_id,

            "draw_number": int(
                match.group(1)
            ),

            "date": date,

            "letter": letter,

            "numbers": numbers,

            "result_id": result_id,

        }

        if lottery_id == LAGNA_WASANA_ID:

            zodiac_info = extract_zodiac(
                [cells[1]]
            )

            if not zodiac_info["zodiac_image"]:

                zodiac_info = extract_zodiac(
                    [result_cell]
                )

            entry.update(zodiac_info)

        second_chance = extract_second_chance(
            cells[3:]
        )

        if second_chance:
            entry["second_chance"] = second_chance

        results.append(entry)

    return results


def collect_lottery_draws(
    session,
    lottery_id,
    lottery_name,
    current_result,
    start_date
):
    """
    Current draw + history pages, going back until start_date.
    Returns draws sorted oldest -> newest.
    """

    draws = {}

    def add(result):

        if result["date"] < start_date:
            return

        if not result.get("numbers"):
            return

        key = (result["date"], result["draw_number"])

        if key in draws:
            return

        result["lottery"] = lottery_name

        result["lottery_id"] = lottery_id

        draws[key] = result

    add(current_result)

    result_id = current_result.get("result_id")

    if not result_id:

        print("  No resultID - cannot read history.")

        return sorted(
            draws.values(),
            key=lambda r: (r["date"], r["draw_number"])
        )

    for page_id in range(0, MAX_HISTORY_PAGES):

        history = fetch_history_page(
            session,
            page_id,
            result_id,
            lottery_id
        )

        if history is None:

            raise RuntimeError(
                f"History page {page_id} failed for {lottery_name}"
            )

        if not history:
            break

        for result in history:
            add(result)

        if min(r["date"] for r in history) < start_date:
            break

        time.sleep(REQUEST_DELAY)

    return sorted(
        draws.values(),
        key=lambda r: (r["date"], r["draw_number"])
    )


# ============================================================
# FIRESTORE
# ============================================================

def init_firestore():

    import firebase_admin
    from firebase_admin import credentials, firestore

    if not firebase_admin._apps:

        raw = os.environ.get(SERVICE_ACCOUNT_ENV)

        if raw:

            cred = credentials.Certificate(
                json.loads(raw)
            )

        elif os.path.exists(SERVICE_ACCOUNT_PATH):

            cred = credentials.Certificate(
                SERVICE_ACCOUNT_PATH
            )

        else:

            raise RuntimeError(
                f"No credentials: set {SERVICE_ACCOUNT_ENV} "
                f"or provide {SERVICE_ACCOUNT_PATH}"
            )

        firebase_admin.initialize_app(cred)

    return firestore.client()


def draw_document_id(result):
    """Example: 2026-09-20_5451"""
    return f"{result['date']}_{result['draw_number']}"


def build_firestore_document(result, server_timestamp):

    second = result.get("second_chance") or {}

    draw_date = datetime.strptime(
        result["date"],
        "%Y-%m-%d"
    ).replace(tzinfo=timezone.utc)

    document = {

        "lottery": result["lottery"],

        "lottery_id": result["lottery_id"],

        "draw_number": result["draw_number"],

        "date": result["date"],

        "draw_date": draw_date,

        "letter": result.get("letter"),

        "numbers": result.get("numbers", []),

        "result_id": result.get("result_id"),

        "has_second_chance": bool(second),

        "second_chance": (
            {
                "available": True,
                "image_url": second.get("image_url"),
            }
            if second else None
        ),

        "updated_at": server_timestamp,

    }

    if result["lottery_id"] == LAGNA_WASANA_ID:

        document["zodiac"] = result.get("zodiac")

        document["zodiac_key"] = result.get("zodiac_key")

        document["zodiac_image"] = result.get("zodiac_image")

    return document


def draws_collection(db, lottery_name):

    return (
        db.collection(FIRESTORE_ROOT_COLLECTION)
        .document(lottery_slug(lottery_name))
        .collection(FIRESTORE_SUBCOLLECTION)
    )


def latest_saved_date(collection):
    """Newest 'date' string saved for this lottery, or None."""

    docs = list(
        collection
        .order_by("date", direction="DESCENDING")
        .limit(1)
        .stream()
    )

    if not docs:
        return None

    return docs[0].to_dict().get("date")


def needs_update(existing, current):
    """True if the saved document differs from what the site shows now."""

    if existing.get("numbers") != current.get("numbers"):
        return True

    if existing.get("letter") != current.get("letter"):
        return True

    old_image = (existing.get("second_chance") or {}).get("image_url")

    new_image = (current.get("second_chance") or {}).get("image_url")

    if old_image != new_image:
        return True

    if current.get("lottery_id") == LAGNA_WASANA_ID:

        if existing.get("zodiac_image") != current.get("zodiac_image"):
            return True

    return False


def write_draws(db, lottery_name, draws):

    from firebase_admin import firestore

    root = db.collection(FIRESTORE_ROOT_COLLECTION)

    root.document(lottery_slug(lottery_name)).set(
        {
            "lottery": lottery_name,
            "lottery_id": draws[0]["lottery_id"],
            "slug": lottery_slug(lottery_name),
            "updated_at": firestore.SERVER_TIMESTAMP,
        },
        merge=True
    )

    collection = draws_collection(db, lottery_name)

    batch = db.batch()

    pending = 0

    for draw in draws:

        batch.set(
            collection.document(draw_document_id(draw)),
            build_firestore_document(
                draw,
                firestore.SERVER_TIMESTAMP
            ),
            merge=True
        )

        pending += 1

        if pending >= FIRESTORE_BATCH_SIZE:

            batch.commit()

            batch = db.batch()

            pending = 0

    if pending:
        batch.commit()


# ============================================================
# SYNC ONE LOTTERY
# ============================================================

def sync_lottery(session, db, lottery_id, lottery_name, current):
    """Returns the number of draws written."""

    if not current.get("numbers"):

        print("  Results not published yet - skipped.")

        return 0

    current["lottery_id"] = lottery_id

    current["lottery"] = lottery_name

    collection = draws_collection(db, lottery_name)

    doc_id = draw_document_id(current)

    snapshot = collection.document(doc_id).get()

    label = f"{current['date']} #{current['draw_number']}"

    # ---- Already saved --------------------------------------

    if snapshot.exists:

        if not needs_update(snapshot.to_dict(), current):

            print(f"  Up to date ({label})")

            return 0

        print(f"  Draw {label} changed on the site - updating.")

        write_draws(db, lottery_name, [current])

        return 1

    # ---- New draw: also fill any gap since the last saved one ---

    latest = latest_saved_date(collection)

    if latest:

        print(f"  New draw {label} (last saved: {latest})")

        start_date = latest

    else:

        print(
            f"  New draw {label} (nothing saved yet - "
            f"backfilling from {FALLBACK_START_DATE})"
        )

        start_date = FALLBACK_START_DATE

    draws = collect_lottery_draws(
        session,
        lottery_id,
        lottery_name,
        current,
        start_date
    )

    if latest:

        # the day of the last saved draw is already in Firestore
        draws = [d for d in draws if d["date"] > latest]

    if not draws:
        return 0

    write_draws(db, lottery_name, draws)

    print(f"  Wrote {len(draws)} draw(s).")

    return len(draws)


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "DLB sync started: "
        + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    )

    session = make_session()

    html = fetch_main_page(session)

    if not html:

        print("ERROR: could not fetch the DLB results page.")

        return 1

    current_results = extract_current_results(html)

    if not current_results:

        print("ERROR: no lottery results found on the page.")

        return 1

    db = init_firestore()

    total_written = 0

    errors = []

    for lottery_id, lottery_name in LOTTERIES.items():

        print(f"{lottery_name}:")

        current = current_results.get(lottery_name)

        if not current:

            print("  Not found on the page.")

            continue

        try:

            total_written += sync_lottery(
                session,
                db,
                lottery_id,
                lottery_name,
                current
            )

        except Exception as e:

            print(f"  ERROR: {e}")

            errors.append(lottery_name)

    print(
        f"Done. {total_written} draw(s) written."
    )

    if errors:

        print("Failed lotteries: " + ", ".join(errors))

        return 1

    return 0


if __name__ == "__main__":

    sys.exit(main())
