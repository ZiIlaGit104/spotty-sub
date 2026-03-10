#!/usr/bin/env python3
"""
spotty-dl.py -- Scrape a Spotify playlist (no API key/account required)
                 and download audio via yt-dlp with full metadata tagging.

Metadata sources (in priority order):
  1. Spotify playlist scraper (spotifyscraper) -- title, artist, year, artwork
  2. MusicBrainz reverse Spotify URL lookup    -- album, track#, ISRC
  3. Deezer search API                         -- album fallback (no auth needed)
  4. MusicBrainz title/artist search           -- last resort, compilations excluded

Tracks with no resolvable album are SKIPPED and reported in the Discord summary
so they can be retried later when metadata improves.

Output structure (Plex-friendly):
    <out>/
      Artist Name/
        Album Name/
          Track Title.mp3

Requirements:
    pip install spotifyscraper yt-dlp mutagen requests

Usage:
    python spotty-dl.py "https://open.spotify.com/playlist/..."
    python spotty-dl.py "..." --format flac --out D:/Music --sleep-min 10 --sleep-max 20
"""

import argparse
import os
import sys
import re
import time
import random
import tempfile
import shutil
import requests
import logging

logging.getLogger("spotify_scraper").setLevel(logging.WARNING)

# =============================================================================
# CONFIGURATION
# Edit these defaults or override any of them via CLI arguments at runtime.
# =============================================================================

# Discord webhook URL for run summaries. Set to None or "" to disable.
DISCORD_WEBHOOK_URL  = ""

# yt-dlp: min/max seconds to sleep between downloads (random value chosen each time)
YTDLP_SLEEP_MIN      = 24.6
YTDLP_SLEEP_MAX      = 47.8

# yt-dlp: seconds to sleep between individual internal HTTP requests
YTDLP_SLEEP_REQUESTS = 2.4

# yt-dlp: path to download archive file. Tracks already-downloaded video IDs
# so re-runs skip them automatically. Set to "" to disable.
YTDLP_ARCHIVE        = "./yt-dlp-downloaded.txt"

# Spotify scraper: seconds to sleep between per-track API/page calls
SPOTIFY_TRACK_DELAY  = 0.5

# MusicBrainz: seconds to sleep after each API call (their limit is 1 req/sec)
MB_RATE_LIMIT_DELAY  = 1.1

# Main loop: extra seconds to wait between tracks on top of yt-dlp's own sleep
DOWNLOAD_LOOP_DELAY  = 1.5

# =============================================================================


# -- Dependency check ---------------------------------------------------------
missing = []
try:
    from spotify_scraper import SpotifyClient
except ImportError:
    missing.append("spotifyscraper")
try:
    import yt_dlp
except ImportError:
    missing.append("yt-dlp")
try:
    from mutagen.id3 import (ID3, TIT2, TPE1, TPE2, TALB, TRCK, TDRC,
                              TPOS, APIC, TCON, TSRC, ID3NoHeaderError)
    from mutagen.mp4 import MP4, MP4Cover
    from mutagen.flac import FLAC, Picture
    from mutagen.oggvorbis import OggVorbis
except ImportError:
    missing.append("mutagen")


if missing:
    print(f"[ERROR] Missing packages: {', '.join(missing)}")
    print(f"        Run: pip install {' '.join(missing)}")
    sys.exit(1)


# -- Discord ------------------------------------------------------------------

def discord_notify(webhook_url: str, content: str) -> None:
    """Post a message to a Discord webhook. Silently skips if URL is empty."""
    if not webhook_url:
        return
    try:
        r = requests.post(
            webhook_url,
            json={"content": content},
            timeout=10,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  [warn] Discord notification failed: {e}")


def build_discord_summary(
    playlist_name: str,
    ok: int,
    skipped_existing: int,
    skipped_no_album: int,
    failed: int,
    errors: list[str],
) -> str:
    lines = [
        f"**spotify_dl** finished: **{playlist_name}**",
        f"",
        f"✅  Downloaded:        {ok}",
        f"⏭️  Skipped (exists):  {skipped_existing}",
        f"⚠️  Skipped (no album): {skipped_no_album}",
        f"❌  Failed:            {failed}",
    ]
    if errors:
        lines.append("")
        lines.append("**Unexpected errors:**")
        for e in errors[:10]:   # cap at 10 to avoid hitting Discord's 2000 char limit
            lines.append(f"• {e}")
        if len(errors) > 10:
            lines.append(f"• ... and {len(errors) - 10} more")
    return "\n".join(lines)


# -- Helpers ------------------------------------------------------------------

def sanitize(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", name).strip(". ")


def fetch_bytes(url: str) -> bytes | None:
    if not url:
        return None
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.content
    except Exception as e:
        print(f"  [warn] HTTP fetch failed: {e}")
        return None


def best_image_url(images: list) -> str | None:
    if not images:
        return None
    sorted_imgs = sorted(
        images,
        key=lambda x: x.get("width") or x.get("height") or 0,
        reverse=True,
    )
    return sorted_imgs[0].get("url")


# -- Spotify scraper ----------------------------------------------------------

def scrape_playlist(url: str) -> tuple[str, list[dict]]:
    """
    Scrape playlist metadata via spotifyscraper.
    Uses get_track_info() per track solely to get the best artwork URL —
    album name is never reliably returned by Spotify and is resolved downstream.
    Returns (playlist_name, list of track dicts).
    """
    print(f"[spotify] Scraping playlist ...")
    client = SpotifyClient()
    try:
        playlist = client.get_playlist_info(url)
        name     = playlist.get("name", "Unknown Playlist")
        raw      = playlist.get("tracks", [])
        print(f"[spotify] '{name}' -- {len(raw)} tracks\n")

        tracks = []
        for i, item in enumerate(raw, 1):
            t = item.get("track") or item
            if not t or not t.get("name"):
                continue

            artists  = t.get("artists") or []
            album    = t.get("album") or {}
            images   = album.get("images") or t.get("images") or []
            release  = album.get("release_date") or t.get("release_date") or ""
            ext_ids  = t.get("external_ids") or {}

            track_id  = t.get("id") or (
                t.get("uri", "").split(":")[-1] if t.get("uri") else None
            )
            track_url = f"https://open.spotify.com/track/{track_id}" if track_id else None

            track = {
                "title":        t.get("name", "Unknown Title"),
                "artist":       artists[0].get("name", "Unknown Artist") if artists else "Unknown Artist",
                "all_artists":  ", ".join(a.get("name", "") for a in artists if a.get("name")),
                "album":        album.get("name") or "",
                "year":         release[:4] if release else "",
                "track_number": t.get("track_number"),
                "total_tracks": album.get("total_tracks"),
                "disc_number":  t.get("disc_number"),
                "genre":        ", ".join(t.get("genres") or []),
                "isrc":         ext_ids.get("isrc"),
                "art_url":      best_image_url(images),
                "duration_ms":  t.get("duration_ms", 0),
                "spotify_url":  track_url,
            }

            # Fetch per-track info solely to get the best artwork URL
            if track_url:
                try:
                    full = client.get_track_info(track_url)
                    if full:
                        f_images = (full.get("album") or {}).get("images") or full.get("images") or []
                        if f_images:
                            better = best_image_url(f_images)
                            if better:
                                track["art_url"] = better
                except Exception as e:
                    print(f"  [warn] get_track_info failed for artwork: {e}")

            time.sleep(SPOTIFY_TRACK_DELAY)
            tracks.append(track)
            print(f"  [{i:>3}/{len(raw)}] {track['artist']} - {track['title']}"
                  + (f"  [{track['album']}]" if track["album"] else ""))

    finally:
        client.close()

    print(f"\n[spotify] Done.\n")
    return name, tracks


# -- MusicBrainz fallback -----------------------------------------------------

MB_BASE    = "https://musicbrainz.org/ws/2"
MB_HEADERS = {"User-Agent": "spotify-dl/2.0 (https://github.com/local/spotify-dl)"}
_VIDEO_RE  = re.compile(r'\b(lyric|video|karaoke|instrumental|live|acoustic|demo|remix)\b', re.I)
_COMP_RE   = re.compile(r'\b(compilation|greatest hits|best of|collection|anthology|various)\b', re.I)


def _release_score(release: dict) -> int:
    rg      = release.get("release-group") or {}
    ptype   = rg.get("primary-type", "")
    stypes  = [s.lower() for s in (rg.get("secondary-types") or [])]
    disamb  = release.get("disambiguation", "") or ""
    title   = release.get("title", "") or ""

    # Base score: prefer Album > EP > Single > unknown
    score = {"Album": 30, "EP": 20, "Single": 10}.get(ptype, 0)

    # Heavily penalize compilations and soundtracks
    if "compilation" in stypes:
        score -= 60
    if "soundtrack" in stypes:
        score -= 40
    if "mixtape/street" in stypes or "demo" in stypes:
        score -= 30

    # Penalize if title or disambiguation suggests a compilation/various artists release
    if _COMP_RE.search(title) or _COMP_RE.search(disamb):
        score -= 50

    # Penalize video/live/karaoke releases
    if _VIDEO_RE.search(disamb) or _VIDEO_RE.search(title):
        score -= 50

    return score

def _mb_recording_from_spotify_url(track_url: str) -> dict | None:
    """
    Query MusicBrainz URL relationship index with the Spotify track URL.
    Returns the first matching recording dict, or None.
    """
    try:
        params = {"resource": track_url, "inc": "recording-rels", "fmt": "json"}
        r = requests.get(f"{MB_BASE}/url", params=params, headers=MB_HEADERS, timeout=15)
        time.sleep(MB_RATE_LIMIT_DELAY)
        if r.status_code != 200:
            return None
        data = r.json()
        # Follow relation to recording
        for rel in data.get("relations", []):
            recording = rel.get("recording")
            if recording:
                # Fetch full recording with releases
                rec_id = recording.get("id")
                if not rec_id:
                    continue
                r2 = requests.get(
                    f"{MB_BASE}/recording/{rec_id}",
                    params={"inc": "releases+release-groups+isrcs+release-group-rels", "fmt": "json"},
                    headers=MB_HEADERS, timeout=15,
                )
                time.sleep(MB_RATE_LIMIT_DELAY)
                if r2.status_code == 200:
                    return r2.json()
    except Exception as e:
        print(f"  [warn] MusicBrainz Spotify URL lookup failed: {e}")
    return None


def _mb_lookup_by_spotify_url(track_url: str) -> dict:
    """Pass 1: MusicBrainz reverse Spotify URL lookup. Returns result dict or {}."""
    result = {}
    try:
        recording = _mb_recording_from_spotify_url(track_url)
        if not recording:
            return result

        print(f"           MusicBrainz matched via Spotify URL")
        isrcs = recording.get("isrcs", [])
        if isrcs:
            result["isrc"] = isrcs[0]

        releases = recording.get("releases", [])
        if not releases:
            return result

        best = max(releases, key=_release_score)
        if _release_score(best) < 0:
            print(f"           MB-URL: best release is a compilation, rejecting")
            return result

        rg = best.get("release-group") or {}
        if rg.get("title"):
            result["album"] = rg["title"]

        for media in best.get("media", []):
            trks = media.get("track", [])
            if trks:
                num = trks[0].get("number")
                if num is not None:
                    try:    result["track_number"] = int(num)
                    except: result["track_number"] = num
                if media.get("track-count"):
                    result["total_tracks"] = media["track-count"]
                break

    except Exception as e:
        print(f"  [warn] MB Spotify URL lookup failed: {e}")

    return result


def _mb_lookup_by_search(artist: str, title: str, isrc: str | None) -> dict:
    """Pass 3: MusicBrainz title/artist or ISRC search. Last resort.
    Strictly rejects compilations — only accepts positive-scoring releases."""
    result = {}
    try:
        params = (
            {"query": f"isrc:{isrc}", "inc": "releases+release-groups+isrcs",
             "fmt": "json", "limit": 10}
            if isrc else
            {"query": f'recording:"{title}" AND artist:"{artist}"',
             "inc": "releases+release-groups+isrcs", "fmt": "json", "limit": 10}
        )
        r = requests.get(f"{MB_BASE}/recording", params=params,
                         headers=MB_HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
        time.sleep(MB_RATE_LIMIT_DELAY)

        recordings = data.get("recordings", [])
        if not recordings:
            return result

        recording = max(recordings, key=lambda x: x.get("score", 0))
        isrcs = recording.get("isrcs", [])
        if isrcs:
            result["isrc"] = isrcs[0]

        releases = recording.get("releases", [])
        if not releases:
            return result

        # Strictly exclude compilations
        clean = [rel for rel in releases if _release_score(rel) > 0]
        if not clean:
            print(f"           MB-search: all releases are compilations/junk, rejecting")
            return result

        best = max(clean, key=_release_score)
        rg = best.get("release-group") or {}
        if rg.get("title"):
            result["album"] = rg["title"]

        for media in best.get("media", []):
            trks = media.get("track", [])
            if trks:
                num = trks[0].get("number")
                if num is not None:
                    try:    result["track_number"] = int(num)
                    except: result["track_number"] = num
                if media.get("track-count"):
                    result["total_tracks"] = media["track-count"]
                break

    except Exception as e:
        print(f"  [warn] MB search lookup failed: {e}")

    return result


def deezer_lookup(artist: str, title: str) -> dict:
    """Pass 2: Deezer public API search. No auth required."""
    result = {}
    try:
        r = requests.get(
            "https://api.deezer.com/search/track",
            params={"q": f'artist:"{artist}" track:"{title}"'},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        if not data:
            r = requests.get(
                "https://api.deezer.com/search/track",
                params={"q": f"{artist} {title}"},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json().get("data", [])
        if not data:
            return result

        artist_lower = artist.lower()
        hit = next(
            (t for t in data if t.get("artist", {}).get("name", "").lower() == artist_lower),
            data[0]
        )
        album = hit.get("album", {})
        if album.get("title"):
            result["album"] = album["title"]
        if hit.get("isrc"):
            result["isrc"] = hit["isrc"]

    except Exception as e:
        print(f"  [warn] Deezer lookup failed: {e}")

    return result


def enrich_missing_from_musicbrainz(tracks: list[dict]) -> None:
    needs_enrich = [t for t in tracks if not t["album"] or t["track_number"] is None]
    if not needs_enrich:
        print("[info] All tracks have complete metadata -- skipping enrichment.\n")
        return

    print(f"[enrich] Querying {len(needs_enrich)} track(s) ...\n")
    for t in needs_enrich:
        print(f"  {t['artist']} - {t['title']}")
        changed = []

        def _apply(res: dict, tag: str) -> None:
            if res.get("album") and not t["album"]:
                t["album"] = res["album"];                  changed.append(f"album='{res['album']}' [{tag}]")
            if res.get("track_number") is not None and t["track_number"] is None:
                t["track_number"] = res["track_number"];    changed.append(f"track#={res['track_number']}")
            if res.get("total_tracks") is not None and t["total_tracks"] is None:
                t["total_tracks"] = res["total_tracks"];    changed.append(f"total={res['total_tracks']}")
            if res.get("isrc") and not t.get("isrc"):
                t["isrc"] = res["isrc"];                    changed.append(f"isrc={res['isrc']}")

        def _needs_more() -> bool:
            return not t["album"] or t["track_number"] is None

        # Pass 1: MusicBrainz reverse Spotify URL lookup (exact match)
        # Returns album + track# when available
        if _needs_more() and t.get("spotify_url"):
            _apply(_mb_lookup_by_spotify_url(t["spotify_url"]), "MB-URL")

        # Pass 2: Deezer — good for album name, does not return track#
        if not t["album"]:
            _apply(deezer_lookup(t["artist"], t["title"]), "Deezer")

        # Pass 3: MusicBrainz title/artist search — no compilations
        # Always run if track# is still missing (even if album was found above)
        if _needs_more():
            _apply(_mb_lookup_by_search(t["artist"], t["title"], t.get("isrc")), "MB-search")

        print(f"  -> {', '.join(changed) if changed else 'nothing new found'}\n")


# -- yt-dlp -------------------------------------------------------------------

def build_ytdlp_opts(tmp_dir: str, fmt: str, quality: str, archive: str | None) -> dict:
    codec_map = {
        "mp3":  ("mp3",  quality),
        "m4a":  ("m4a",  quality),
        "flac": ("flac", None),
        "opus": ("opus", None),
    }
    codec, q = codec_map.get(fmt, ("mp3", quality))
    pp = {"key": "FFmpegExtractAudio", "preferredcodec": codec}
    if q:
        pp["preferredquality"] = q

    opts = {
        "format":                  "bestaudio/best",
        "outtmpl":                 os.path.join(tmp_dir, "track.%(ext)s"),
        "postprocessors":          [pp],
        "quiet":                   True,
        "no_warnings":             True,
        "sleep_interval":          YTDLP_SLEEP_MIN,
        "max_sleep_interval":      YTDLP_SLEEP_MAX,
        "sleep_interval_requests": YTDLP_SLEEP_REQUESTS,
    }
    if archive:
        opts["download_archive"] = archive
    return opts


def search_and_download(track: dict, fmt: str, quality: str, archive: str | None) -> str | None:
    base_query  = f"{track['artist']} - {track['title']}"
    album_query = f"{base_query} {track['album']}" if track["album"] else base_query

    tmp_dir = tempfile.mkdtemp()
    opts    = build_ytdlp_opts(tmp_dir, fmt, quality, archive)

    for query in dict.fromkeys([album_query, base_query]):
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([f"ytsearch1:{query}"])
            files = [f for f in os.listdir(tmp_dir) if not f.endswith(".part")]
            if files:
                return os.path.join(tmp_dir, files[0])
        except Exception as e:
            print(f"  [warn] yt-dlp ({query!r}): {e}")

    shutil.rmtree(tmp_dir, ignore_errors=True)
    return None


# -- Metadata tagging ---------------------------------------------------------

def _trck_str(track: dict) -> str:
    n = str(track["track_number"]) if track.get("track_number") is not None else ""
    t = str(track["total_tracks"]) if track.get("total_tracks") is not None else ""
    return f"{n}/{t}" if (n and t) else n


def tag_mp3(path: str, track: dict, art: bytes | None):
    try:    audio = ID3(path)
    except ID3NoHeaderError: audio = ID3()

    def s(frame):
        audio.delall(frame.FrameID); audio.add(frame)

    s(TIT2(encoding=3, text=track["title"]))
    s(TPE1(encoding=3, text=track["artist"]))
    s(TPE2(encoding=3, text=track["all_artists"]))
    if track.get("album"):       s(TALB(encoding=3, text=track["album"]))
    if track.get("year"):        s(TDRC(encoding=3, text=track["year"]))
    trck = _trck_str(track)
    if trck:                     s(TRCK(encoding=3, text=trck))
    if track.get("disc_number"): s(TPOS(encoding=3, text=str(track["disc_number"])))
    if track.get("genre"):       s(TCON(encoding=3, text=track["genre"]))
    if track.get("isrc"):        s(TSRC(encoding=3, text=track["isrc"]))
    if art:
        audio.delall("APIC")
        audio.add(APIC(encoding=3, mime="image/jpeg", type=3, desc="Cover", data=art))
    audio.save(path, v2_version=3)


def tag_m4a(path: str, track: dict, art: bytes | None):
    audio = MP4(path)
    if audio.tags is None: audio.add_tags()
    tags = audio.tags
    tags["\xa9nam"] = [track["title"]]
    tags["\xa9ART"] = [track["artist"]]
    tags["aART"]    = [track["all_artists"]]
    if track.get("album"):       tags["\xa9alb"] = [track["album"]]
    if track.get("year"):        tags["\xa9day"] = [track["year"]]
    if track.get("track_number") is not None:
        total = int(track["total_tracks"]) if track.get("total_tracks") else 0
        tags["trkn"] = [(int(track["track_number"]), total)]
    if track.get("disc_number"): tags["disk"]    = [(int(track["disc_number"]), 0)]
    if track.get("genre"):       tags["\xa9gen"] = [track["genre"]]
    if art:                      tags["covr"]    = [MP4Cover(art, imageformat=MP4Cover.FORMAT_JPEG)]
    audio.save()


def tag_flac(path: str, track: dict, art: bytes | None):
    audio = FLAC(path)
    audio["title"]       = track["title"]
    audio["artist"]      = track["artist"]
    audio["albumartist"] = track["all_artists"]
    if track.get("album"):        audio["album"]       = track["album"]
    if track.get("year"):         audio["date"]        = track["year"]
    if track.get("track_number") is not None:
        audio["tracknumber"] = str(track["track_number"])
        if track.get("total_tracks") is not None:
            audio["totaltracks"] = str(track["total_tracks"])
    if track.get("disc_number"):  audio["discnumber"]  = str(track["disc_number"])
    if track.get("genre"):        audio["genre"]       = track["genre"]
    if track.get("isrc"):         audio["isrc"]        = track["isrc"]
    if art:
        pic = Picture(); pic.type = 3; pic.mime = "image/jpeg"; pic.data = art
        audio.clear_pictures(); audio.add_picture(pic)
    audio.save()


def tag_ogg(path: str, track: dict, art: bytes | None):
    audio = OggVorbis(path)
    audio["title"]       = [track["title"]]
    audio["artist"]      = [track["artist"]]
    audio["albumartist"] = [track["all_artists"]]
    if track.get("album"):        audio["album"]       = [track["album"]]
    if track.get("year"):         audio["date"]        = [track["year"]]
    if track.get("track_number") is not None:
        audio["tracknumber"] = [str(track["track_number"])]
    if track.get("genre"):        audio["genre"]       = [track["genre"]]
    audio.save()


def apply_tags(path: str, track: dict, art: bytes | None):
    ext = os.path.splitext(path)[1].lower()
    if   ext == ".mp3":            tag_mp3(path, track, art)
    elif ext in (".m4a", ".aac"):  tag_m4a(path, track, art)
    elif ext == ".flac":           tag_flac(path, track, art)
    elif ext in (".ogg", ".opus"): tag_ogg(path, track, art)
    else: print(f"  [warn] No tagger for '{ext}', skipping metadata")


# -- Plex folder layout -------------------------------------------------------

def primary_artist(name: str) -> str:
    """Strip featured/contributing artists from a name string for use in folder paths.
    e.g. 'Charlie Puth, Selena Gomez' → 'Charlie Puth'
         'Drake ft. Future'           → 'Drake'
    """
    name = re.split(r'\s*[,&]\s*|\s+f(?:ea)?t\.?\s+|\s+with\s+', name, maxsplit=1)[0]
    return name.strip()


def plex_path(base_dir: str, track: dict, fmt: str) -> str:
    """base_dir / Artist / Album / Track Title.ext"""
    artist   = sanitize(primary_artist(track["artist"] or "Unknown Artist"))
    album    = sanitize(track["album"])   # caller guarantees non-empty
    filename = f"{sanitize(track['title'])}.{fmt}"
    return os.path.join(base_dir, artist, album, filename)


# -- Main ---------------------------------------------------------------------

def main():
    global YTDLP_SLEEP_MIN, YTDLP_SLEEP_MAX, YTDLP_SLEEP_REQUESTS

    parser = argparse.ArgumentParser(
        description="Download a Spotify playlist via YouTube with full Plex-friendly metadata.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("url",
                        help="Spotify playlist URL")
    parser.add_argument("--format", "-f", default="mp3",
                        choices=["mp3", "m4a", "flac", "opus"],
                        help="Output audio format")
    parser.add_argument("--quality", "-q", default="320",
                        choices=["320", "256", "192", "128"],
                        help="Bitrate kbps for mp3/m4a (ignored for flac/opus)")
    parser.add_argument("--out", "-o", default="./downloads",
                        help="Root output directory")
    parser.add_argument("--no-art", action="store_true",
                        help="Skip embedding album art")
    parser.add_argument("--delay", type=float, default=DOWNLOAD_LOOP_DELAY,
                        help="Extra seconds between tracks on top of yt-dlp sleep")
    parser.add_argument("--sleep-min", type=float, default=YTDLP_SLEEP_MIN,
                        help="yt-dlp minimum sleep between downloads (seconds)")
    parser.add_argument("--sleep-max", type=float, default=YTDLP_SLEEP_MAX,
                        help="yt-dlp maximum sleep between downloads (seconds)")
    parser.add_argument("--sleep-requests", type=float, default=YTDLP_SLEEP_REQUESTS,
                        help="yt-dlp sleep between internal HTTP requests (seconds)")
    parser.add_argument("--archive", default=YTDLP_ARCHIVE,
                        help="yt-dlp archive file (empty string to disable)")
    parser.add_argument("--discord", default=DISCORD_WEBHOOK_URL,
                        help="Discord webhook URL for run summary (empty to disable)")
    args = parser.parse_args()

    YTDLP_SLEEP_MIN      = args.sleep_min
    YTDLP_SLEEP_MAX      = args.sleep_max
    YTDLP_SLEEP_REQUESTS = args.sleep_requests

    archive     = args.archive  if args.archive  else None
    discord_url = args.discord  if args.discord  else None

    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)

    print(f"[info] yt-dlp sleep: {YTDLP_SLEEP_MIN}-{YTDLP_SLEEP_MAX}s | "
          f"requests: {YTDLP_SLEEP_REQUESTS}s")
    if archive:
        print(f"[info] Archive: {os.path.abspath(archive)}")
    if discord_url:
        print(f"[info] Discord notifications: enabled")
    print()

    # 1. Scrape Spotify playlist metadata
    playlist_name, tracks = scrape_playlist(args.url)

    # 2. Enrich missing albums via MusicBrainz (Spotify URL) → Deezer → MusicBrainz search
    enrich_missing_from_musicbrainz(tracks)

    # 3. Split tracks: those with album info proceed, those without are deferred
    ready    = [t for t in tracks if t["album"]]
    deferred = [t for t in tracks if not t["album"]]

    if deferred:
        print(f"[warn] {len(deferred)} track(s) have no resolvable album and will be SKIPPED:")
        for t in deferred:
            print(f"       - {t['artist']} - {t['title']}")
        print()

    print(f"[info] Downloading {len(ready)} tracks to: {out_dir}")
    print(f"[info] Structure:  Artist / Album / Title.{args.format}\n")

    ok = 0
    skipped_existing  = 0
    skipped_no_album  = len(deferred)
    failed = 0
    errors: list[str] = []
    art_cache: dict[str, bytes | None] = {}

    for i, track in enumerate(ready, 1):
        final_path = plex_path(out_dir, track, args.format)
        album_dir  = os.path.dirname(final_path)

        trk = (f"{track['track_number']}/{track['total_tracks']}"
               if track.get("total_tracks") else str(track.get("track_number") or "?"))
        print(f"[{i:>3}/{len(ready)}] {track['artist']} - {track['title']}")
        print(f"         {track['album']}  |  track {trk}  |  {track.get('year') or '?'}")

        # Skip if file already exists
        if os.path.exists(final_path):
            print(f"  [skip] Already exists\n")
            skipped_existing += 1
            continue

        # Fetch art (cached per URL)
        art = None
        if not args.no_art and track.get("art_url"):
            key = track["art_url"]
            if key not in art_cache:
                art_cache[key] = fetch_bytes(key)
            art = art_cache[key]
            print(f"         art: {len(art)//1024}KB" if art else "         art: fetch failed")

        # Download audio
        try:
            tmp_file = search_and_download(track, args.format, args.quality, archive)
        except Exception as e:
            msg = f"{track['artist']} - {track['title']}: download exception: {e}"
            print(f"  [ERROR] {msg}\n")
            errors.append(msg)
            failed += 1
            continue

        if not tmp_file:
            msg = f"{track['artist']} - {track['title']}: yt-dlp returned no file"
            print(f"  [FAIL] {msg}\n")
            errors.append(msg)
            failed += 1
            continue

        # Embed metadata + art
        try:
            apply_tags(tmp_file, track, art)
        except Exception as e:
            msg = f"{track['artist']} - {track['title']}: tagging failed: {e}"
            print(f"  [warn] {msg}")
            errors.append(msg)

        # Move to final location
        try:
            os.makedirs(album_dir, exist_ok=True)
            shutil.move(tmp_file, final_path)
            shutil.rmtree(os.path.dirname(tmp_file), ignore_errors=True)
        except Exception as e:
            msg = f"{track['artist']} - {track['title']}: file move failed: {e}"
            print(f"  [ERROR] {msg}\n")
            errors.append(msg)
            failed += 1
            continue

        print(f"  [ ok] {os.path.relpath(final_path, out_dir)}\n")
        ok += 1

        if args.delay > 0 and i < len(ready):
            time.sleep(args.delay)

    # Summary
    print("-" * 55)
    print(f"  Downloaded:         {ok}")
    print(f"  Skipped (exists):   {skipped_existing}")
    print(f"  Skipped (no album): {skipped_no_album}")
    print(f"  Failed:             {failed}")
    print(f"  Root: {out_dir}")
    if archive and os.path.exists(archive):
        with open(archive) as f:
            print(f"  Archive entries: {sum(1 for _ in f)}")

    # Discord notification
    if discord_url:
        summary = build_discord_summary(
            playlist_name, ok, skipped_existing, skipped_no_album, failed, errors
        )
        # Append skipped-no-album track list if any
        if deferred:
            track_list = "\n".join(
                f"  • {t['artist']} - {t['title']}" for t in deferred
            )
            summary += f"\n\n**Skipped (no album found):**\n{track_list}"
        discord_notify(discord_url, summary)
        print("\n[info] Discord summary sent.")


if __name__ == "__main__":
    main()
