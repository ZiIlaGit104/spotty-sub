#!/usr/bin/env python3
"""
spotty-sub.py -- Scrape a Spotify playlist and download audio via yt-dlp
                with clean ID3/Vorbis metadata embedded directly.

Metadata is retrieved directly from Spotify (title, artist, album, year, artwork).

Output structure (Plex-friendly):
    <out>/
      Artist Name/
        Album Name/
          Track Title.ext

Requirements:
    pip install spotifyscraper yt-dlp mutagen requests
    ffmpeg installed in system PATH (Verify with "ffmpeg --version")

Usage:
    python spotty-sub.py "https://open.spotify.com/playlist/..."
    python spotty-sub.py "..." --format flac --out /music --sleep-min 10 --sleep-max 20
    python spotty-sub.py "..." --format mp3 --out /music --overwrite
"""

import argparse
import os
import sys
import re
import time
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
DISCORD_WEBHOOK_URL  = "https://discord.com/api/webhooks/YOUR_WEBHOOK_URL"

# yt-dlp: min/max seconds to sleep between downloads (random value chosen each time)
YTDLP_SLEEP_MIN      = 24.6
YTDLP_SLEEP_MAX      = 47.8

# yt-dlp: seconds to sleep between individual internal HTTP requests
YTDLP_SLEEP_REQUESTS = 2.4

# yt-dlp: path to download archive file. Set to "" to disable.
YTDLP_ARCHIVE        = "./yt-dlp-downloaded.txt"

# Spotify scraper: seconds to sleep between per-track API calls
SPOTIFY_TRACK_DELAY  = 0.5

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
    from mutagen.id3 import (ID3, TIT2, TPE1, TPE2, TALB, TRCK, TDRC, TYER,
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
    failed: int,
    errors: list[str],
) -> str:
    lines = [
        f"**spotify_dl** finished: **{playlist_name}**",
        f"",
        f"✅  Downloaded:        {ok}",
        f"⏭️  Skipped (exists):  {skipped_existing}",
        f"❌  Failed:            {failed}",
    ]
    if errors:
        lines.append("")
        lines.append("**Unexpected errors:**")
        for e in errors[:10]:
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
    """Scrape full playlist metadata via spotifyscraper."""
    print(f"[spotify] Scraping playlist ...")
    client = SpotifyClient()
    try:
        playlist = client.get_playlist(url, max_tracks=None)
        name = getattr(playlist, "name", None) or getattr(playlist, "title", "Unknown Playlist")
        raw = getattr(playlist, "tracks", []) or []
        print(f"[spotify] '{name}' -- {len(raw)} tracks\n")
        tracks = []
        for i, item in enumerate(raw, 1):
            t_obj = getattr(item, "track", item)

            if hasattr(t_obj, "to_dict"):
                t = t_obj.to_dict()
            elif hasattr(t_obj, "__dict__"):
                t = vars(t_obj)
            elif isinstance(t_obj, dict):
                t = t_obj
            else:
                t = {}

            title = t.get("name") or t.get("title") or "Unknown Title"

            album_val = t.get("album")
            if isinstance(album_val, str):
                album_name = album_val
                album_dict = {}
            elif hasattr(album_val, "to_dict"):
                album_dict = album_val.to_dict()
                album_name = album_dict.get("name") or album_dict.get("title") or "Unknown Album"
            elif isinstance(album_val, dict):
                album_dict = album_val
                album_name = album_dict.get("name") or album_dict.get("title") or "Unknown Album"
            else:
                album_dict = {}
                album_name = "Unknown Album"

            release_date = t.get("release_date") or album_dict.get("release_date") or ""

            raw_artists = t.get("artists") or t.get("artist") or album_dict.get("artists") or []
            artist_list = []

            if isinstance(raw_artists, list):
                for a in raw_artists:
                    if isinstance(a, dict):
                        a_name = a.get("name") or a.get("title")
                    elif hasattr(a, "name"):
                        a_name = getattr(a, "name")
                    elif isinstance(a, str):
                        a_name = a
                    else:
                        a_name = None

                    if a_name:
                        artist_list.append(str(a_name).strip())
            elif isinstance(raw_artists, str):
                artist_list = [raw_artists.strip()]

            artist_list = [a for a in artist_list if a]
            main_artist = artist_list[0] if artist_list else "Unknown Artist"
            all_artists = ", ".join(artist_list)

            images = t.get("images") or album_dict.get("images") or []
            formatted_images = []
            if isinstance(images, list):
                for img in images:
                    if isinstance(img, dict):
                        formatted_images.append(img)
                    elif hasattr(img, "to_dict"):
                        formatted_images.append(img.to_dict())
                    elif hasattr(img, "__dict__"):
                        formatted_images.append(vars(img))

            ext_ids = t.get("external_ids") or {}
            if hasattr(ext_ids, "to_dict"):
                ext_ids = ext_ids.to_dict()

            track_id = t.get("id")
            if not track_id and t.get("uri"):
                track_id = str(t.get("uri")).split(":")[-1]
            track_url = f"https://open.spotify.com/track/{track_id}" if track_id else None

            isrc_val = ext_ids.get("isrc") if isinstance(ext_ids, dict) else None

            track = {
                "title":        title,
                "artist":       main_artist,
                "all_artists":  all_artists,
                "album":        album_name,
                "year":         str(release_date)[:4] if release_date else "",
                "track_number": t.get("track_number"),
                "total_tracks": album_dict.get("total_tracks"),
                "disc_number":  t.get("disc_number"),
                "genre":        ", ".join(t.get("genres") or []),
                "isrc":         isrc_val,
                "art_url":      best_image_url(formatted_images),
                "duration_ms":  t.get("duration_ms", 0),
                "spotify_url":  track_url,
            }

            if track_url:
                try:
                    full = client.get_track(track_url)
                    full_dict = full.to_dict() if hasattr(full, "to_dict") else (vars(full) if hasattr(full, "__dict__") else {})
                    f_album = full_dict.get("album") or {}
                    if hasattr(f_album, "to_dict"):
                        f_album = f_album.to_dict()
                    f_imgs = full_dict.get("images") or (f_album.get("images") if isinstance(f_album, dict) else []) or []
                    if f_imgs:
                        fmt_f_imgs = [i.to_dict() if hasattr(i, "to_dict") else (vars(i) if hasattr(i, "__dict__") else i) for i in f_imgs]
                        better = best_image_url(fmt_f_imgs)
                        if better:
                            track["art_url"] = better
                except Exception:
                    pass

            time.sleep(SPOTIFY_TRACK_DELAY)
            tracks.append(track)
            print(f"  [{i:>3}/{len(raw)}] {track['artist']} - {track['title']}  [{track['album']}]")

    finally:
        client.close()

    print(f"\n[spotify] Done.\n")
    return name, tracks


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


_BAD_RESULT_RE  = re.compile(r'\b(live|cover|karaoke|tribute|remix|extended|instrumental|version|parody|reaction|tutorial)\b', re.I)
_AUDIO_RE       = re.compile(r'\b(audio|lyrics?|official audio|full album)\b', re.I)


def _score_result(info: dict, track: dict) -> int:
    """Score a yt-dlp search result — higher is better."""
    title    = info.get("title", "")
    duration = info.get("duration") or 0
    score    = 0

    if _AUDIO_RE.search(title):
        score += 20

    track_title = track.get("title", "")
    if _BAD_RESULT_RE.search(title) and not _BAD_RESULT_RE.search(track_title):
        score -= 30

    expected_ms = track.get("duration_ms", 0)
    if expected_ms and duration:
        diff_sec = abs(duration - expected_ms / 1000)
        if diff_sec < 5:
            score += 20
        elif diff_sec < 15:
            score += 10
        elif diff_sec > 60:
            score -= 20

    return score


def search_and_download(track: dict, fmt: str, quality: str, archive: str | None) -> str | None:
    base_query  = f"{track['artist']} - {track['title']}"
    audio_query = f"{base_query} audio"
    album_query = f"{base_query} {track['album']}" if track["album"] else None

    tmp_dir = tempfile.mkdtemp()
    opts    = build_ytdlp_opts(tmp_dir, fmt, quality, archive)

    queries = dict.fromkeys(q for q in [audio_query, album_query, base_query] if q)

    for query in queries:
        try:
            search_opts = {**opts, "quiet": True, "no_warnings": True, "skip_download": True,
                           "extract_flat": False}
            with yt_dlp.YoutubeDL(search_opts) as ydl:
                results = ydl.extract_info(f"ytsearch5:{query}", download=False)
            candidates = (results or {}).get("entries") or []
            if not candidates:
                continue

            best = max(candidates, key=lambda r: _score_result(r, track))
            video_url = best.get("webpage_url") or best.get("url")
            if not video_url:
                continue

            print(f"         yt: {best.get('title', '?')[:70]}")

            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([video_url])
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
    if track.get("year"):
        year = str(track["year"]).strip()
        s(TDRC(encoding=3, text=year))
        s(TYER(encoding=3, text=year))
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
    if track.get("year"):        tags["\xa9day"] = [str(track["year"]).strip()]
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
    if track.get("year"):
        year = str(track["year"]).strip()
        audio["date"]        = year
        audio["year"]        = year
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
    if track.get("year"):
        year = str(track["year"]).strip()
        audio["date"]        = [year]
        audio["year"]        = [year]
    if track.get("track_number") is not None:
        audio["tracknumber"] = [str(track["track_number"])]
    if track.get("genre"):        audio["genre"]       = [track["genre"]]
    audio.save()


def _purge_archive_entry(archive_path: str, final_path: str) -> None:
    """Remove any archive line whose filename matches the track's final path stem."""
    if not archive_path or not os.path.exists(archive_path):
        return
    stem = os.path.splitext(os.path.basename(final_path))[0].lower()
    try:
        with open(archive_path, "r") as f:
            lines = f.readlines()
        kept = [l for l in lines if stem not in l.lower()]
        if len(kept) < len(lines):
            with open(archive_path, "w") as f:
                f.writelines(kept)
            print(f"         archive: removed {len(lines)-len(kept)} entr{'y' if len(lines)-len(kept)==1 else 'ies'}")
    except Exception as e:
        print(f"  [warn] Could not purge archive: {e}")


def apply_tags(path: str, track: dict, art: bytes | None):
    ext = os.path.splitext(path)[1].lower()
    if   ext == ".mp3":            tag_mp3(path, track, art)
    elif ext in (".m4a", ".aac"):  tag_m4a(path, track, art)
    elif ext == ".flac":           tag_flac(path, track, art)
    elif ext in (".ogg", ".opus"): tag_ogg(path, track, art)
    else: print(f"  [warn] No tagger for '{ext}', skipping metadata")


# -- Plex folder layout -------------------------------------------------------

def primary_artist(name: str) -> str:
    """Strip featured/contributing artists for folder layout.
    e.g. 'Charlie Puth, Selena Gomez' -> 'Charlie Puth'
    """
    name = re.split(r'\s*[,&]\s*|\s+f(?:ea)?t\.?\s+|\s+with\s+', name, maxsplit=1)[0]
    return name.strip()


def plex_path(base_dir: str, track: dict, fmt: str) -> str:
    """base_dir / Artist / Album / Track Title.ext"""
    artist   = sanitize(primary_artist(track["artist"] or "Unknown Artist"))
    album    = sanitize(track["album"] or "Unknown Album")
    filename = f"{sanitize(track['title'])}.{fmt}"
    return os.path.join(base_dir, artist, album, filename)


# -- Main ---------------------------------------------------------------------

def main():
    global YTDLP_SLEEP_MIN, YTDLP_SLEEP_MAX, YTDLP_SLEEP_REQUESTS

    parser = argparse.ArgumentParser(
        description="Download a Spotify playlist via YouTube with direct Spotify metadata.",
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
    parser.add_argument("--overwrite", action="store_true",
                        help="Re-download and overwrite tracks that already exist")
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

    # 1. Scrape Spotify playlist metadata directly
    playlist_name, tracks = scrape_playlist(args.url)

    print(f"[info] Downloading {len(tracks)} tracks to: {out_dir}")
    print(f"[info] Structure:  Artist / Album / Title.{args.format}\n")

    ok = 0
    skipped_existing = 0
    failed = 0
    errors: list[str] = []
    art_cache: dict[str, bytes | None] = {}

    for i, track in enumerate(tracks, 1):
        final_path = plex_path(out_dir, track, args.format)
        album_dir  = os.path.dirname(final_path)

        trk = (f"{track['track_number']}/{track['total_tracks']}"
               if track.get("total_tracks") else str(track.get("track_number") or "?"))
        print(f"[{i:>3}/{len(tracks)}] {track['artist']} - {track['title']}")
        print(f"         {track['album']}  |  track {trk}  |  {track.get('year') or '?'}")

        # Skip if file exists (unless --overwrite)
        if os.path.exists(final_path):
            if not args.overwrite:
                print(f"  [skip] Already exists\n")
                skipped_existing += 1
                continue
            print(f"         overwrite: removing existing file")
            os.remove(final_path)
            _purge_archive_entry(archive, final_path)

        # Fetch art
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

        if args.delay > 0 and i < len(tracks):
            time.sleep(args.delay)

    # Summary
    print("-" * 55)
    print(f"  Downloaded:       {ok}")
    print(f"  Skipped (exists): {skipped_existing}")
    print(f"  Failed:           {failed}")
    print(f"  Root: {out_dir}")
    if archive and os.path.exists(archive):
        with open(archive) as f:
            print(f"  Archive entries: {sum(1 for _ in f)}")

    # Discord notification
    if discord_url:
        summary = build_discord_summary(
            playlist_name, ok, skipped_existing, failed, errors
        )
        discord_notify(discord_url, summary)
        print("\n[info] Discord summary sent.")


if __name__ == "__main__":
    main()
