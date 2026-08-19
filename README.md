# SupaFAN - YouTube Intelligence & OSINT Pipeline

![SupaFAN Banner](https://img.shields.io/badge/SupaFAN-YouTube_OSINT-ff0000?style=for-the-badge&logo=youtube)
![Python](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python)
![AI Ready](https://img.shields.io/badge/AI_Training-Ready-success?style=for-the-badge)

---

<img align="right" src="logo.svg" alt="SupaFAN Logo" width="320" />

**SupaFAN** is a highly advanced, automated command-line OSINT (Open Source Intelligence) and data curation pipeline designed to extract, analyze, and download YouTube channel data at scale. By unifying the YouTube Data API v3, `yt-dlp`, `static-ffmpeg`, and the OpenRouter AI API, SupaFAN allows researchers, data scientists, and developers to scrape rich datasets from YouTube channels and automatically generate intelligence briefings.
---

## 🚀 Core Features

- **Smart Target Resolution:** Automatically resolves simple YouTube handles (e.g. `@MarkRober`) or raw channel IDs into their underlying API upload playlists.
- **Deep Metadata & Comment Mining:** Extracts detailed video statistics such as views, likes, upload dates, descriptions, tags, and user-engagement comments.
- **AI Intelligence Engine:** Uses the OpenRouter API to analyze scraped metadata and comments, generating audience sentiment, content strategy, and viewer-intent reports.
- **High-Fidelity AV Downloader:** Uses `yt-dlp` and `static-ffmpeg` to download high-resolution video and high-bitrate audio and merge them into `.mp4` files.
- **Cookie-Based Authentication:** Supports authenticated browser cookies for resolving YouTube download restrictions when permitted by the user's session and YouTube's access controls.

---

## 🧠 Using SupaFAN for AI Model Training

SupaFAN was designed to output data that is **primed for Artificial Intelligence and Machine Learning pipelines**. The combination of raw media and structured textual metadata makes it useful for dataset curation.

### 1. Multimodal Model Training
The downloaded MP4 files can be processed by vision and audio models for tasks such as:
- Video understanding
- Temporal action localization
- Frame analysis
- Speech-to-text processing
- Audio analysis

### 2. Large Language Model Fine-Tuning
The `output/` directory generates structured text files containing video concepts, metadata, comments, and AI-generated analysis.

These datasets can be transformed into formats such as JSONL for experimentation with language models.

**Potential applications include:**
- Audience Sentiment Prediction
- YouTube Strategy Analysis
- Content Classification
- Topic Detection
- Comment Analysis

### 3. Retrieval-Augmented Generation (RAG)
SupaFAN's generated intelligence reports can be indexed into a vector database and used with frameworks such as LangChain or LlamaIndex.

This allows an AI system to retrieve information from previously analyzed channels and videos.

### 4. NLP & Behavioral Analysis
The comments extracted by the pipeline provide large collections of real-world internet language, slang, opinions, and reactions that can be useful for:
- Sentiment analysis
- Toxicity classification
- Topic classification
- Community analysis
- Linguistic research

---

## ⚙️ Setup & Installation

### 1. Clone & Environment
Ensure you have Python 3.10+ installed.

```bash
git clone https://github.com/DAPOWER99/SupaFan.git
cd SupaFAN
python -m venv .venv

# Activate on Windows
.venv\Scripts\activate

# Activate on Mac/Linux
source .venv/bin/activate
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. API Keys Configuration
Copy `sample.env` to `.env` and add your API keys:

```env
YOUTUBE_API_KEY=your_google_cloud_youtube_api_key
OPENROUTER_API_KEY=your_openrouter_api_key
```

---

## 🔐 Handling YouTube Download Restrictions

YouTube may return `403 Forbidden` errors when a requested stream requires authentication or when automated requests are restricted.

SupaFAN supports authenticated browser cookies for situations where you are authorized to access the content.

Place your exported `cookies.txt` file in the SupaFAN root directory alongside `main.py`, then run SupaFAN again.

> **⚠️ Important:** Never commit `cookies.txt` or other authentication credentials to GitHub.

Add them to `.gitignore`:

```gitignore
cookies.txt
.env
```

---

## 💻 CLI Usage Instructions

SupaFAN operates through the terminal.

### Interactive Mode
```bash
python main.py
```

### Targeted Mode & Video Downloading
```bash
python main.py -t @MarkRober -d
```

### OSINT Metadata Extraction Only
```bash
python main.py -t @MarkRober
```

### Deep Scan
```bash
python main.py -t @MarkRober -d --scan-all
```

---

## ⚠️ Important Note Regarding Visual Studio Code

If you preview downloaded `.mp4` files using Visual Studio Code's built-in media player, audio may not play correctly depending on the codecs used by the file and the VS Code/Electron environment.

The downloaded file itself may still contain a valid audio stream.

For reliable playback, open the file with a dedicated media player such as **VLC**.

*Personally, I choose VLC because Windows Media Player sucks 🥀*

---

## 📊 Channel Analysis Accuracy

SupaFAN v2.0 was manually evaluated against **13 YouTube channels**.

| Result                |      Count |
| --------------------- | ---------: |
| 100% accuracy         |         10 |
| 90% accuracy          |          1 |
| 80% accuracy          |          1 |
| 70% accuracy          |          1 |
| **Weighted accuracy** | **95.38%** |

### Calculation
```text
(10 × 100 + 1 × 90 + 1 × 80 + 1 × 70) ÷ 13
= 1240 ÷ 13
= 95.384615...
≈ 95%
```

The evaluation represents a **manual report-level accuracy assessment**, rather than a laboratory-grade measurement of every individual generated claim.

**[Check Accuracy](instructions.txt)**

---

## 📝 License

Made With 💖 By **DAPOWER99**.
---

**Happy OSINT Researching! 🔍**
