# 🎵 spotty-sub

**Subscribe to Spotify playlists. Download tracks. Keep metadata clean.**

`spotty-sub` scrapes Spotify playlist metadata then downloads audio from YouTube and embeds full metadata and artwork into properly structured audio files.

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
- Track number
- Album artwork
- Album name 

---

### 3. Download Audio from YouTube

- Uses `yt-dlp` to search for the track on YouTube
- Downloads the video
- Uses `ffmpeg` to extract audio
- Allows format and quality selection

---

### 4. Embed Metadata & Organize Files

Embeds:

- Artwork  
- Artist  
- Album  
- Track number  
- Year  

Saves files using the structure:

- `<base_path>/<Artist>/<Album>/<Track Name>.<extension>`

---

### 5. Discord Notifications (Optional)

- Sends status updates when tracks are downloaded

---

## 🧠 Smart Features

- API throttling / sleep timers (to respect YouTube/Spotify)
- Skips already-downloaded tracks
  - Maintains a YouTube archive file
  - Checks destination path for existing file matches
- Designed for automation
  - Works with CRON (Linux/macOS)
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
