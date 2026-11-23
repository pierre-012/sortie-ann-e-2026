"""
Filtre les artistes actifs en 2026.

Pour chaque artiste de ta base en ligne (lien ARTISTS_URL), le script ouvre sa discographie
Spotify, regarde ses sorties les plus récentes et vérifie si l'une d'elles
date de l'année cible (2026 par défaut). Si oui, l'artiste est enregistré
dans data/artistes_actifs_2026.json (même format que artistes.json).

Utilisation :
    python artistes_actifs_2026.py            # lance le scan
    python artistes_actifs_2026.py --merge    # fusionne les shards (GitHub Actions)

Variables d'environnement facultatives :
    ARTISTS_URL               : lien vers la base d'artistes (JSON brut)
    SHARD_INDEX / SHARD_TOTAL : répartition entre plusieurs jobs
    TARGET_YEAR               : année à rechercher (2026 par défaut)
"""

import json
import multiprocessing as mp
import os
import random
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.request
from datetime import date, datetime
from pathlib import Path
from queue import Empty
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


ROOT_DIR = Path.cwd()
# Lien direct vers ta base d'artistes (JSON brut, accessible sans connexion).
# Colle ton lien ici, ou passe-le par la variable d'environnement ARTISTS_URL.
# Exemple GitHub : https://raw.githubusercontent.com/TON_PSEUDO/TON_REPO/main/data/artistes.json
# La base est lue UNIQUEMENT depuis ce lien : aucun fichier local n'est utilisé.
ARTISTS_URL = os.environ.get(
    "ARTISTS_URL",
    "https://pierre-012.github.io/spotify-releated-crawler/data/artistes.json",
)

ACTIVE_FILE = ROOT_DIR / "data" / "artistes_actifs_2026.json"
SHARDS_DIR = ROOT_DIR / "data" / "shards"

TARGET_YEAR = int(os.environ.get("TARGET_YEAR", "2026"))

# --- Sharding (répartition entre jobs GitHub Actions) -------------------
SHARD_INDEX = int(os.environ.get("SHARD_INDEX", "0"))
SHARD_TOTAL = int(os.environ.get("SHARD_TOTAL", "1"))

if SHARD_TOTAL < 1:
    raise ValueError("SHARD_TOTAL doit être >= 1.")

if not (0 <= SHARD_INDEX < SHARD_TOTAL):
    raise ValueError(
        f"SHARD_INDEX ({SHARD_INDEX}) doit être compris entre 0 et "
        f"SHARD_TOTAL - 1 ({SHARD_TOTAL - 1})."
    )

SHARD_OUTPUT_FILE = SHARDS_DIR / f"actifs_shard_{SHARD_INDEX}.json"

SPOTIFY_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 "
    "(KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

# --- Réglages de performance -------------------------------------------
MAX_WORKERS = 3
CONTEXT_RECYCLE_EVERY = 40
MIN_DELAY_PER_WORKER = 1.0
MAX_DELAY_PER_WORKER = 2.5
NETWORK_IDLE_TIMEOUT_MS = 15000
# Nombre maximum de sorties récentes examinées par artiste. Utile si la
# toute dernière sortie est une pré-sortie datée après l'année cible.
MAX_ALBUMS_TO_CHECK = 5
# -------------------------------------------------------------------------

INVISIBLE_CHARS_PATTERN = re.compile(
    r"[\u00AD\u061C\u200B\u200C\u200D\u200E\u200F"
    r"\u202A-\u202E\u2066-\u2069\uFEFF]"
)

INTERFACE_LABEL_BLACKLIST = {
    "bibliothèque", "ta bibliothèque", "your library", "library",
    "accueil", "home", "rechercher", "search", "à la une", "populaire",
    "spotify", "premium", "connexion", "log in", "s'inscrire", "sign up",
    "playlists", "artistes", "artists", "albums",
    "podcasts et émissions", "podcasts & shows",
}


# ======================================================================
# Utilitaires
# ======================================================================

def log(message: str, worker_id: Optional[int] = None) -> None:
    prefix = f"[W{worker_id}] " if worker_id is not None else ""
    print(f"{prefix}{message}", flush=True)


def strip_invisible_chars(text: str) -> str:
    return INVISIBLE_CHARS_PATTERN.sub("", text) if text else text


def normalize_url(value: Any) -> str:
    if not isinstance(value, str):
        return ""

    value = value.strip()

    markdown_match = re.fullmatch(r"\[([^\]]+)\]\((https?://[^)]+)\)", value)

    if markdown_match:
        value = markdown_match.group(2).strip()

    if value.startswith("<") and value.endswith(">"):
        value = value[1:-1].strip()

    return value


def is_spotify_artist_url(url: str) -> bool:
    parsed = urlparse(url)

    return (
        parsed.scheme in {"http", "https"}
        and "open.spotify.com" in parsed.netloc
        and "/artist/" in parsed.path
    )


def get_spotify_id(url: str, item_type: str) -> str:
    match = re.search(rf"/{item_type}/([A-Za-z0-9]+)", url, flags=re.IGNORECASE)
    return match.group(1) if match else ""


def build_discography_url(artist_url: str) -> str:
    clean_url = normalize_url(artist_url).rstrip("/")

    clean_url = re.sub(
        r"/discography/(all|albums|singles|compilations|appears-on)$",
        "",
        clean_url,
        flags=re.IGNORECASE,
    )

    return f"{clean_url}/discography/all"


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
        file.write("\n")


def read_active_file(path: Path) -> Dict[str, Dict[str, Any]]:
    """Lit un fichier d'artistes actifs et renvoie le dictionnaire {id: artiste}."""
    if not path.exists():
        return {}

    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except (json.JSONDecodeError, OSError) as error:
        log(f"[WARN] Lecture impossible de {path} : {error}")
        return {}

    artists = data.get("artists") if isinstance(data, dict) else None

    return artists if isinstance(artists, dict) else {}


def save_active_file(path: Path, artists: Dict[str, Dict[str, Any]]) -> None:
    write_json(
        path,
        {
            "year": TARGET_YEAR,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "count": len(artists),
            "artists": artists,
        },
    )


# ======================================================================
# Chargement des artistes
# ======================================================================

def fetch_artists_data() -> Any:
    """Télécharge la base d'artistes depuis ARTISTS_URL (avec 3 tentatives)."""
    url = (ARTISTS_URL or "").strip()

    if not url or "REMPLACE-MOI" in url:
        raise ValueError(
            "ARTISTS_URL est vide ou non renseigné. Mets le lien vers ta base "
            "dans le script, ou définis la variable d'environnement ARTISTS_URL."
        )

    if not url.lower().startswith(("http://", "https://")):
        raise ValueError(
            f"ARTISTS_URL doit commencer par http:// ou https:// (reçu : {url})"
        )

    log(f"[INFO] Téléchargement de la base : {url}")

    last_error: Optional[Exception] = None

    for attempt in range(1, 4):
        try:
            request = urllib.request.Request(
                url,
                headers={"User-Agent": SPOTIFY_USER_AGENT, "Accept": "application/json"},
            )

            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))

        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            last_error = error
            log(f"[WARN] Tentative {attempt}/3 échouée : {error}")
            time.sleep(2 * attempt)

    raise RuntimeError(f"Impossible de récupérer la base d'artistes : {last_error}")


def load_artists() -> List[Dict[str, Any]]:
    data = fetch_artists_data()

    if not isinstance(data, dict):
        raise ValueError("La base d'artistes doit contenir un objet JSON.")

    container = data.get("artists")

    if not isinstance(container, dict):
        raise ValueError("La clé 'artists' est absente ou incorrecte.")

    artists: List[Dict[str, Any]] = []

    for artist_id, artist_data in container.items():
        if not isinstance(artist_data, dict):
            continue

        artist_url = normalize_url(artist_data.get("url", ""))

        if not is_spotify_artist_url(artist_url):
            log(f"[WARN] URL invalide pour {artist_data.get('name', artist_id)}")
            continue

        artist = dict(artist_data)
        artist["id"] = artist_data.get("id") or artist_id
        artist["name"] = artist_data.get("name", "Artiste inconnu")
        artist["url"] = artist_url
        artists.append(artist)

    log(f"[INFO] {len(artists)} artistes chargés.")

    if not artists:
        raise ValueError("Aucun artiste valide trouvé dans artistes.json.")

    return artists


# ======================================================================
# Lecture des dates
# ======================================================================

MONTHS = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7,
    "jul": 7, "august": 8, "aug": 8, "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12,
    "dec": 12, "janvier": 1, "janv": 1, "fevrier": 2, "fevr": 2, "fev": 2,
    "mars": 3, "avril": 4, "avr": 4, "mai": 5, "juin": 6, "juillet": 7,
    "juil": 7, "aout": 8, "septembre": 9, "octobre": 10, "novembre": 11,
    "decembre": 12,
}


def parse_date(value: str) -> Optional[date]:
    if not value:
        return None

    value = " ".join(str(value).strip().split()).replace("\u00a0", " ")

    normalized = unicodedata.normalize("NFD", value)
    normalized = "".join(
        char for char in normalized if unicodedata.category(char) != "Mn"
    )

    match = re.search(
        r"\b((?:19|20)\d{2})[-/](\d{1,2})[-/](\d{1,2})\b", normalized
    )

    if match:
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None

    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass

    # "March 5, 2026"
    match = re.search(
        r"\b([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?[,]?\s+((?:19|20)\d{2})\b",
        normalized,
    )

    if match:
        month = MONTHS.get(match.group(1).lower())

        if month:
            try:
                return date(int(match.group(3)), month, int(match.group(2)))
            except ValueError:
                return None

    # "5 mars 2026"
    match = re.search(
        r"\b(\d{1,2})(?:er)?\s+([A-Za-z]+)\s+((?:19|20)\d{2})\b", normalized
    )

    if match:
        month = MONTHS.get(match.group(2).lower())

        if month:
            try:
                return date(int(match.group(3)), month, int(match.group(1)))
            except ValueError:
                return None

    return None


def parse_date_from_encore_element(element: Any) -> Optional[date]:
    values: List[str] = []

    try:
        values.append(element.get_text(" ", strip=True))
    except Exception:
        pass

    try:
        values.append(str(element))
    except Exception:
        pass

    if hasattr(element, "attrs"):
        for attribute_value in element.attrs.values():
            if isinstance(attribute_value, list):
                values.extend(str(item) for item in attribute_value)
            else:
                values.append(str(attribute_value))

    for value in values:
        parsed = parse_date(value)

        if parsed:
            return parsed

    return None


def extract_release_date_from_html(html: str) -> Optional[date]:
    soup = BeautifulSoup(html, "html.parser")

    # 1) Éléments Encore de Spotify
    for element in soup.select("[data-encore-id]"):
        parsed = parse_date_from_encore_element(element)

        if parsed:
            return parsed

    # 2) Éléments dont les attributs parlent de date / release / métadonnées
    for element in soup.find_all(True):
        attributes_text = " ".join(f"{k}={v}" for k, v in element.attrs.items())

        if not re.search(r"date|release|album|metadata", attributes_text, re.IGNORECASE):
            continue

        parsed = parse_date_from_encore_element(element)

        if parsed:
            return parsed

    # 3) Métadonnées standard
    for selector, attribute in [
        ("time", "datetime"),
        ("meta[property='music:release_date']", "content"),
        ("meta[name='release_date']", "content"),
        ("meta[property='release_date']", "content"),
        ("meta[itemprop='datePublished']", "content"),
        ("meta[itemprop='releaseDate']", "content"),
    ]:
        for element in soup.select(selector):
            parsed = parse_date(element.get(attribute, "")) or parse_date(
                element.get_text(" ", strip=True)
            )

            if parsed:
                return parsed

    # 4) JSON embarqué dans les scripts
    patterns = [
        r'"release_date"\s*:\s*"([^"]+)"',
        r'"releaseDate"\s*:\s*"([^"]+)"',
        r'"datePublished"\s*:\s*"([^"]+)"',
    ]

    for script in soup.find_all("script"):
        raw = script.string or script.get_text()

        if not raw:
            continue

        for pattern in patterns:
            for match in re.finditer(pattern, raw, flags=re.IGNORECASE):
                parsed = parse_date(match.group(1))

                if parsed:
                    return parsed

    # 5) Dernier recours : texte visible
    return parse_date(soup.get_text(" ", strip=True))


def looks_like_interface_label(title: str) -> bool:
    cleaned = strip_invisible_chars(title or "").strip()

    if len(cleaned) < 2 or len(cleaned) > 250:
        return True

    return " ".join(cleaned.lower().split()) in INTERFACE_LABEL_BLACKLIST


def extract_album_title_from_html(html: str) -> str:
    """Titre de l'album (purement informatif, sert à garder une trace)."""
    soup = BeautifulSoup(html, "html.parser")

    def clean(raw: Optional[str]) -> str:
        return " ".join(strip_invisible_chars(raw or "").split())

    entity = soup.select_one("[data-testid='entityTitle']")

    if entity is not None:
        h1 = entity.find("h1")
        title = clean((h1 or entity).get_text(" ", strip=True))

        if title and not looks_like_interface_label(title):
            return title

    og_meta = soup.select_one("meta[property='og:title']")

    if og_meta is not None:
        title = clean(og_meta.get("content", ""))

        if title and not looks_like_interface_label(title):
            return title

    main_el = soup.find("main")

    if main_el is not None and main_el.find("h1") is not None:
        title = clean(main_el.find("h1").get_text(" ", strip=True))

        if title and not looks_like_interface_label(title):
            return title

    return ""


# ======================================================================
# Navigation Spotify
# ======================================================================

def extract_album_links(page) -> List[str]:
    links: List[str] = []

    anchors = page.locator("a[href*='/album/']")
    count = anchors.count()

    for index in range(count):
        try:
            href = anchors.nth(index).get_attribute("href")
        except Exception:
            continue

        if not href:
            continue

        href = normalize_url(href)

        if href.startswith("/"):
            href = f"https://open.spotify.com{href}"

        match = re.search(
            r"https://open\.spotify\.com/(?:intl-[^/]+/)?album/([A-Za-z0-9]+)",
            href,
            flags=re.IGNORECASE,
        )

        if not match:
            continue

        canonical_url = f"https://open.spotify.com/album/{match.group(1)}"

        if canonical_url not in links:
            links.append(canonical_url)

    return links


def wait_for_page_ready(page, worker_id: int) -> None:
    try:
        page.wait_for_selector(
            "main, h1, a[href*='/artist/'], a[href*='/album/']",
            timeout=30000,
        )
    except PlaywrightTimeoutError:
        log("[WARN][SPOTIFY] Élément principal non détecté.", worker_id)

    try:
        page.wait_for_selector("[data-encore-id]", timeout=30000)
    except PlaywrightTimeoutError:
        log("[WARN][SPOTIFY] Aucun élément data-encore-id détecté.", worker_id)

    try:
        page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        pass


def check_artist_activity(
    page,
    artist: Dict[str, Any],
    worker_id: int,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Renvoie (statut, infos) avec statut dans :
      - "active"   : une sortie de l'année cible a été trouvée
      - "inactive" : la sortie la plus récente est antérieure à l'année cible
      - "unknown"  : impossible de déterminer (page vide, date introuvable...)
    """
    artist_name = artist["name"]
    discography_url = build_discography_url(artist["url"])

    log(f"[SPOTIFY] Artiste : {artist_name}", worker_id)

    try:
        page.goto(discography_url, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_selector("a[href*='/album/']", timeout=30000)

        try:
            page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            pass

    except PlaywrightTimeoutError:
        log("[WARN][SPOTIFY] Timeout de chargement de la discographie.", worker_id)
    except Exception as error:
        log(f"[ERREUR][SPOTIFY] {error}", worker_id)
        return "unknown", None

    album_links = extract_album_links(page)

    if not album_links:
        log(f"[SPOTIFY] Aucun projet trouvé pour {artist_name}.", worker_id)
        return "unknown", None

    most_recent_past: Optional[Dict[str, Any]] = None

    # La discographie est triée de la plus récente à la plus ancienne.
    for album_url in album_links[:MAX_ALBUMS_TO_CHECK]:
        album_id = get_spotify_id(album_url, "album")

        try:
            page.goto(album_url, wait_until="domcontentloaded", timeout=120000)
            wait_for_page_ready(page, worker_id)
        except Exception as error:
            log(f"[ERREUR][SPOTIFY] Impossible d'ouvrir {album_url} : {error}", worker_id)
            continue

        html = page.content()
        release_date = extract_release_date_from_html(html)

        if release_date is None:
            log(f"[SKIP][SPOTIFY] Date introuvable pour {album_url}", worker_id)
            continue

        log(f"[DATE] {album_url} -> {release_date.isoformat()}", worker_id)

        release_info = {
            "id": album_id,
            "name": extract_album_title_from_html(html),
            "url": album_url,
            "release_date": release_date.isoformat(),
        }

        if release_date.year == TARGET_YEAR:
            return "active", release_info

        if release_date.year > TARGET_YEAR:
            # Sortie future (pré-sortie) : on regarde la suivante.
            continue

        # Année < année cible : les suivantes seront encore plus anciennes.
        most_recent_past = release_info
        break

    if most_recent_past is not None:
        return "inactive", most_recent_past

    return "unknown", None


def create_browser_context(browser):
    context = browser.new_context(
        locale="fr-FR",
        user_agent=SPOTIFY_USER_AGENT,
        viewport={"width": 1440, "height": 1000},
    )

    context.set_default_timeout(30000)
    context.set_default_navigation_timeout(120000)

    return context


# ======================================================================
# Workers
# ======================================================================

def worker_main(
    worker_id: int,
    artists_chunk: List[Dict[str, Any]],
    result_queue: "mp.Queue",
) -> None:
    log(f"[INFO] Démarrage, {len(artists_chunk)} artiste(s) à traiter.", worker_id)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = create_browser_context(browser)
        page = context.new_page()

        try:
            for index, artist in enumerate(artists_chunk, start=1):
                try:
                    status, release = check_artist_activity(page, artist, worker_id)

                    if status == "active":
                        entry = dict(artist)
                        entry["active_year"] = TARGET_YEAR
                        entry["last_release"] = release
                        result_queue.put(("active", worker_id, entry))

                    elif status == "inactive":
                        last_date = release["release_date"] if release else "?"
                        result_queue.put(
                            ("inactive", worker_id, artist["name"], last_date)
                        )

                    else:
                        result_queue.put(
                            ("unknown", worker_id, artist["name"], "statut indéterminé")
                        )

                except Exception as error:
                    log(f"[ERREUR ARTISTE] {artist['name']} : {error}", worker_id)
                    result_queue.put(("error", worker_id, artist["name"], str(error)))

                if index % CONTEXT_RECYCLE_EVERY == 0 and index != len(artists_chunk):
                    log("[INFO] Recréation du contexte navigateur.", worker_id)

                    for closable in (page, context):
                        try:
                            closable.close()
                        except Exception:
                            pass

                    context = create_browser_context(browser)
                    page = context.new_page()

                time.sleep(random.uniform(MIN_DELAY_PER_WORKER, MAX_DELAY_PER_WORKER))

        finally:
            for closable in (page, context):
                try:
                    closable.close()
                except Exception:
                    pass

            browser.close()

    result_queue.put(("worker_done", worker_id, None))
    log("[INFO] Terminé.", worker_id)


def split_round_robin(
    items: List[Dict[str, Any]],
    n_buckets: int,
) -> List[List[Dict[str, Any]]]:
    buckets: List[List[Dict[str, Any]]] = [[] for _ in range(n_buckets)]

    for index, item in enumerate(items):
        buckets[index % n_buckets].append(item)

    return buckets


# ======================================================================
# Programme principal
# ======================================================================

def merge_shards() -> None:
    """Fusionne tous les shards dans data/artistes_actifs_2026.json."""
    merged = read_active_file(ACTIVE_FILE)
    before = len(merged)

    shard_files = sorted(SHARDS_DIR.glob("actifs_shard_*.json"))

    if not shard_files:
        log("[INFO] Aucun shard à fusionner.")

    for shard_file in shard_files:
        shard_artists = read_active_file(shard_file)
        merged.update(shard_artists)
        log(f"[INFO] {shard_file.name} : {len(shard_artists)} artiste(s)")

    save_active_file(ACTIVE_FILE, merged)
    log(f"[FIN] {ACTIVE_FILE} : {before} -> {len(merged)} artiste(s) actif(s) en {TARGET_YEAR}")


def main() -> None:
    log("=" * 70)
    log(f"[DÉMARRAGE] Recherche des artistes actifs en {TARGET_YEAR}")
    log(f"[DÉMARRAGE] Workers : {MAX_WORKERS} | Shard : {SHARD_INDEX + 1}/{SHARD_TOTAL}")
    log("=" * 70)

    all_artists = load_artists()
    artists = all_artists[SHARD_INDEX::SHARD_TOTAL]

    # Un artiste déjà confirmé actif en 2026 n'a pas besoin d'être revérifié.
    already_active = read_active_file(ACTIVE_FILE)

    if already_active:
        before = len(artists)
        artists = [a for a in artists if a["id"] not in already_active]
        log(
            f"[INFO] {before - len(artists)} artiste(s) déjà confirmé(s) actif(s), ignorés."
        )

    log(f"[INFO] Artistes à vérifier : {len(artists)}")

    if not artists:
        log("[INFO] Rien à faire.")
        return

    # En mode shard on écrit un fichier par shard, sinon directement le fichier final.
    output_file = SHARD_OUTPUT_FILE if SHARD_TOTAL > 1 else ACTIVE_FILE
    found: Dict[str, Dict[str, Any]] = (
        {} if SHARD_TOTAL > 1 else dict(already_active)
    )

    stats = {"traités": 0, "actifs": 0, "inactifs": 0, "inconnus": 0, "erreurs": 0}

    n_workers = min(MAX_WORKERS, len(artists)) or 1
    chunks = split_round_robin(artists, n_workers)

    result_queue: mp.Queue = mp.Queue()
    processes: List[mp.Process] = []
    start_time = time.monotonic()

    for worker_id, chunk in enumerate(chunks, start=1):
        if not chunk:
            continue

        process = mp.Process(
            target=worker_main,
            args=(worker_id, chunk, result_queue),
            daemon=True,
        )
        process.start()
        processes.append(process)

    workers_remaining = len(processes)

    while workers_remaining > 0:
        try:
            message = result_queue.get(timeout=300)
        except Empty:
            log("[WARN] Aucun résultat reçu depuis 5 minutes, vérification des workers...")

            if all(not process.is_alive() for process in processes):
                log("[WARN] Tous les workers sont arrêtés, fin de la boucle.")
                break

            continue

        kind = message[0]

        if kind == "worker_done":
            workers_remaining -= 1
            continue

        if kind == "active":
            _, worker_id, entry = message
            found[entry["id"]] = entry
            save_active_file(output_file, found)
            stats["actifs"] += 1
            log(
                f"[ACTIF] {entry['name']} (dernière sortie : "
                f"{entry['last_release']['release_date']})",
                worker_id,
            )

        elif kind == "inactive":
            _, worker_id, artist_name, last_date = message
            stats["inactifs"] += 1
            log(f"[INACTIF] {artist_name} (dernière sortie : {last_date})", worker_id)

        elif kind == "unknown":
            _, worker_id, artist_name, reason = message
            stats["inconnus"] += 1
            log(f"[INCONNU] {artist_name} : {reason}", worker_id)

        elif kind == "error":
            _, worker_id, artist_name, error_text = message
            stats["erreurs"] += 1

        stats["traités"] += 1

        if stats["traités"] % 25 == 0:
            log(f"[PROGRESSION] {stats['traités']}/{len(artists)} artistes traités.")

    for process in processes:
        process.join(timeout=30)

        if process.is_alive():
            log(f"[WARN] Le processus {process.pid} ne s'est pas terminé proprement, arrêt forcé.")
            process.terminate()

    # Sauvegarde finale (crée le fichier même s'il n'y a aucun artiste actif)
    save_active_file(output_file, found)

    elapsed = time.monotonic() - start_time

    log("")
    log("=" * 70)
    log(f"[FIN] Fichier : {output_file}")
    log(
        f"[FIN] Traités : {stats['traités']} | Actifs {TARGET_YEAR} : {stats['actifs']} | "
        f"Inactifs : {stats['inactifs']} | Inconnus : {stats['inconnus']} | "
        f"Erreurs : {stats['erreurs']}"
    )
    log(f"[FIN] Durée totale : {elapsed:.1f}s ({elapsed / 60:.1f} min)")
    log("=" * 70)


if __name__ == "__main__":
    try:
        if "--merge" in sys.argv:
            merge_shards()
        else:
            main()
    except Exception as error:
        log(f"[ERREUR FATALE] {error}")
        sys.exit(1)
