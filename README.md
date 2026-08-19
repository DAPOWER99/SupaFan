# SupaFAN - YouTube Intelligence & OSINT Pipeline

![SupaFAN Banner](https://img.shields.io/badge/SupaFAN-YouTube_OSINT-ff0000?style=for-the-badge&logo=youtube)
![Python](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python) |
![AI Ready](https://img.shields.io/badge/AI_Training-Ready-success?style=for-the-badge)

**SupaFAN** is a highly advanced, automated command-line OSINT (Open Source Intelligence) and data curation pipeline designed to extract, analyze, and download YouTube channel data at scale. 

By unifying the YouTube Data API v3, `yt-dlp`, `static-ffmpeg`, and the OpenRouter AI API, SupaFAN allows researchers, data scientists, and developers to scrape rich datasets from YouTube channels, bypassing strict DRM blocks, and automatically generating intelligence briefings.

---

## 🚀 Core Features

* **Smart Target Resolution:** Automatically resolves simple YouTube handles (e.g., `@MarkRober`) or raw channel IDs into their underlying API upload playlists, mapping the entire channel architecture seamlessly.
* **Deep Metadata & Comment Mining:** Extracts highly detailed video statistics (views, likes, precise upload dates, description tags) alongside top user-engagement comments using the YouTube Data API v3.
* **AI Intelligence Engine:** Integrates with the OpenRouter API to autonomously digest the scraped metadata and comments, calculating audience sentiment, content strategy, and viewer intent, which is then saved as rich text dossiers.
* **High-Fidelity AV Downloader:** Features a robust downloader powered by `yt-dlp` and `static-ffmpeg` to guarantee extraction of the highest-resolution video streams and highest-bitrate audio streams, merging them flawlessly into `.mp4` formats.
* **Anti-DRM (403 Bypass) Integration:** Built-in cookie consumption system that utilizes browser-exported `cookies.txt` files to effortlessly slice through YouTube's aggressive "PO Token" / `403 Forbidden` DRM walls, ensuring unhindered data extraction.

---

## 🧠 Using SupaFAN for AI Model Training

SupaFAN was intentionally designed to output data that is **primed for Artificial Intelligence and Machine Learning pipelines**. The dual-pronged extraction of both raw media and structured textual metadata makes it a powerhouse for dataset curation.

### 1. Multimodal Model Training (Vision & Audio)
The pipeline's integration with `static-ffmpeg` ensures that the downloaded MP4s are high-resolution and artifact-free. This media can be directly fed into vision models (like Sora, Runway, or custom CNNs) for video understanding, temporal action localization, or frame-by-frame generative training. The cleanly merged AAC audio tracks can similarly be extracted for Whisper/speech-to-text fine-tuning or audio-generation models.

### 2. Large Language Model (LLM) Fine-Tuning
The `output/` directory generates highly structured text files that map video concepts to audience reactions. By converting these raw text outputs into JSONL formats, you can fine-tune Llama 3, Mistral, or GPT variants to:
* **Predict Audience Sentiment:** Train an LLM to predict how a specific demographic will react to a specific video title or topic.
* **YouTube Strategy Generation:** Teach a model the correlation between viral metadata (tags, titles, upload timing) and high view counts.

### 3. Retrieval-Augmented Generation (RAG)
SupaFAN's generated OSINT intelligence files serve as perfect vector-database embeddings. You can point a framework like LangChain or LlamaIndex at the `output/` directory, allowing an AI agent to instantly answer questions like *"What was the audience's primary criticism in Mark Rober's egg drop video?"* based entirely on the parsed comments and AI summaries.

### 4. NLP & Behavioral Analysis
The raw comments extracted by the pipeline provide massive, unfiltered corpuses of human internet dialect, slang, and behavioral responses, ideal for training sentiment classifiers, toxicity detectors, or sociological predictive models.

---

## ⚙️ Setup & Installation

### 1. Clone & Environment
Ensure you have Python 3.10+ installed on your system. It is highly recommended to use a virtual environment.
```bash
git clone https://github.com/DAPOWER99/SupaFan.git
cd SupaFAN
python -m venv .venv

# Activate on Windows:
.venv\Scripts\activate
# Activate on Mac/Linux:
source .venv/bin/activate
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. API Keys Configuration
SupaFAN relies on external APIs to gather data and generate intelligence.
1. Copy the `sample.env` file to a new file named `.env`.
2. Fill in your keys:
```env
# Required for extracting metadata and comments
YOUTUBE_API_KEY=your_google_cloud_youtube_api_key

# Required for the AI Intelligence Engine
OPENROUTER_API_KEY=your_openrouter_api_key
```

---

## 🔐 Bypassing the "403 Forbidden" Download Error

YouTube actively combats scraping scripts by rotating DRM protections (PO Tokens) on high-traffic videos. If SupaFAN logs a `403 Forbidden` error during the download phase, it means the IP/request was blocked.

**To permanently bypass this, you must supply your authenticated browser cookies:**

1. Install the **"Get cookies.txt LOCALLY"** extension on Google Chrome, Microsoft Edge, or Brave.
2. Navigate to `youtube.com` and ensure you are logged into a standard account.
3. Click the extension icon in your browser toolbar and click **Export**.
4. A file named `cookies.txt` will download. Move this file directly into your `SupaFAN` root folder (in the same directory as `main.py`).
5. Run SupaFAN again. The downloader engine will detect the `cookies.txt` file and utilize your active session to authorize the stream, bypassing the 403 wall completely!

---

## 💻 CLI Usage Instructions

SupaFAN operates entirely through your terminal. All text-based intelligence summaries are saved to the `output/` directory, and downloaded AV files are saved to `downloads/`.

### Interactive Mode
Run the script without arguments to enter the guided prompt:
```bash
python main.py
```

### Targeted Mode & Video Downloading
Scan a channel's latest 5 uploads, extract all metadata, run AI analysis, AND download the `.mp4` video files simultaneously:
```bash
python main.py -t @MarkRober -d
```

### OSINT Metadata Extraction Only
Scrape the data, mine the comments, and generate AI intelligence summaries, but *skip* the heavy video downloads (Great for fast text-dataset generation):
```bash
python main.py -t @MarkRober
```

### Deep-Scan Architecture (Bypass Limits)
Bypass the default 5-video limit and recursively scan the entire channel's upload history:
```bash
python main.py -t @MarkRober -d --scan-all
```

---

## ⚠️ Important Note regarding Visual Studio Code

If you attempt to preview the downloaded `.mp4` files directly inside **Visual Studio Code's** built-in media player, the video will appear to have **no sound** (the audio icon will be grayed out/crossed out).

**Your speakers are fine and the downloaded file is completely intact.**
This is a known, hardcoded limitation of the VS Code (Electron) architecture, which lacks the proprietary licensing required to decode standard `AAC` audio streams found in high-quality MP4s. 

To watch the video with sound, simply right-click the file in your VS Code explorer, select **Reveal in File Explorer** (or `Shift + Alt + R`), and open it in a standard application like VLC, Windows Media Player, or your web browser.
personally i choose vlc cuz windows media player sucks 🥀

---

## 📝 License
Engineered by DAPOWER99.
---
## Important Notice
![Check Accuracy.md](https://github.com/DAPOWER99/SupaFan/blob/main/instructions.txt)
