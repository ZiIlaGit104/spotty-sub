# 🎵 spotty-sub

**Subscribe to Spotify playlists. Download tracks. Keep metadata clean.**

`spotty-sub` scrapes Spotify playlist metadata and enriches it using MusicBrainz and Deezer, then downloads audio from YouTube and embeds full metadata and artwork into properly structured audio files.

---

## 🚀 What It Does

### 1. Subscribe to a Spotify Playlist
Provide one or more Spotify playlist URLs as input.  
The script treats them like subscriptions and keeps your local library in sync.

---

### 2. Scrape Track Metadata

Uses `spotifyscraper` to collect:

- Artist name  
- Track name  
- Album artwork  

---

### 3. Resolve Missing Album Metadata (3-Tier Fallback System)

Spotify’s web UI does not reliably expose album metadata, so `spotty-sub` performs a multi-step enrichment process:

#### Tier 1 — MusicBrainz Direct Lookup
- Uses the Spotify track URL to query the MusicBrainz public API
- Attempts to match the track to a release
- Retrieves:
  - Album name
  - Release year
  - Track number

#### Tier 2 — Deezer API Fallback
- Searches Deezer’s public API using artist + track name
- Attempts to match and extract missing metadata

#### Tier 3 — MusicBrainz Fuzzy Match
- Performs fuzzy matching on MusicBrainz
- Attempts to associate the track with:
  - Debut album
  - Single release
- Avoids compilation albums when possible

---

### 4. Download Audio from YouTube

- Uses `yt-dlp` to search for the track on YouTube
- Downloads the video
- Uses `ffmpeg` to extract audio
- Allows format and quality selection

---

### 5. Embed Metadata & Organize Files

Embeds:

- Artwork  
- Artist  
- Album  
- Track number  
- Year  

Saves files using the structure:

- `<base_path>/<Artist>/<Album>/<Track Name>.<extension>`

---

### 6. Discord Notifications (Optional)

- Sends status updates when tracks are downloaded

---

## 🧠 Smart Features

- API throttling / sleep timers (to respect YouTube, Deezer, MusicBrainz)
- Skips already-downloaded tracks
  - Maintains a YouTube archive file
  - Checks destination path for existing file matches
- Designed for automation
  - Works with Cron (Linux/macOS)
  - Works with Windows Task Scheduler

---

## 📦 Requirements

### System Dependencies

- `ffmpeg`
- Python 3.12 or 3.13  
  *(Should work with most modern Python 3.x versions)*

---

### Python Dependencies

```bash
pip install spotifyscraper yt-dlp mutagen requests
