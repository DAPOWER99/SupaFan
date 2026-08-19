"""
YouTube OSINT & Intelligence Analysis Pipeline V1.0
====================================================================
Production-grade intelligence pipeline that captures full channel
metadata, video telemetry, comment statistics, runs multi-model AI
OSINT analysis through OpenRouter, and exports raw stats, knowledge
web, AI JSON, and an exhaustive, unlimited info.txt report.

Modules:
    1. Configuration & Constants
    2. Utility & Sanitization
    3. YouTube Channel Resolver
    4. Data Extractor Engine
    5. Payload Optimizer
    6. AI Intelligence Engine
    7. Text Writer Engine (info.txt)
    8. Pipeline Orchestrator
    9. CLI Interface
"""

import argparse
import json
import logging
import math
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

try:
    import yt_dlp
    YT_DLP_AVAILABLE = True
except ImportError:
    YT_DLP_AVAILABLE = False


# =====================================================================
# 1. CONFIGURATION & CONSTANTS
# =====================================================================

SCRIPT_VERSION = "1.0.0"
SCRIPT_CODENAME = "SupaFAN"
SCRIPT_AUTHOR = "DAPOWER99"
SCRIPT_REPO = "https://github.com/DAPOWER99/YouTube-OSINT"

DEFAULT_OUTPUT_DIR = Path("output")
DEFAULT_DOWNLOAD_DIR = Path("downloads")
DEFAULT_VIDEOS_TO_SCAN = 25
MAX_COMMENTS_PER_VIDEO = 500
COMMENT_PAGES_MAX = 10
COMMENTS_PER_PAGE = 100
PAYLOAD_TOKEN_BUDGET = 12000
PAYLOAD_MAX_VIDEOS = 20
PAYLOAD_MAX_COMMENTS_PER_VIDEO = 5
PAYLOAD_MAX_DESCRIPTION_CHARS = 300

API_REQUEST_TIMEOUT = 45
AI_BASE_BACKOFF_SECONDS = 2.0
AI_BACKOFF_MULTIPLIER = 1.5
AI_ATTEMPTS_PER_MODEL = 2

# Cascade of free fallback models on OpenRouter.
# openrouter/free is an auto-router that dynamically picks from whatever
# free models are currently available — always the most reliable option.
# Specific model slugs go stale as providers add/remove free access,
# so the auto-router is listed first.
OPENROUTER_FALLBACK_MODELS = [
    "openrouter/free",
    "google/gemma-3-27b-it:free",
    "mistralai/mistral-small-3.1-24b-instruct:free",
    "qwen/qwen3-32b:free",
    "meta-llama/llama-4-scout:free",
]

OPENROUTER_API_ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"

# YouTube URL regex patterns (ordered by specificity)
YT_PATTERNS = {
    "channel_id": re.compile(
        r"(?:https?://)?(?:www\.)?youtube\.com/channel/(UC[a-zA-Z0-9_-]{22})"
    ),
    "custom_url": re.compile(
        r"(?:https?://)?(?:www\.)?youtube\.com/c/([a-zA-Z0-9_.-]+)"
    ),
    "user_url": re.compile(
        r"(?:https?://)?(?:www\.)?youtube\.com/user/([a-zA-Z0-9_.-]+)"
    ),
    "handle_url": re.compile(
        r"(?:https?://)?(?:www\.)?youtube\.com/@([a-zA-Z0-9_.-]+)"
    ),
    "handle_raw": re.compile(
        r"^@([a-zA-Z0-9_.-]+)$"
    ),
    "raw_channel_id": re.compile(
        r"^(UC[a-zA-Z0-9_-]{22})$"
    ),
}

# Exit codes
EXIT_SUCCESS = 0
EXIT_CONFIG_ERROR = 1
EXIT_RESOLUTION_ERROR = 2
EXIT_EXTRACTION_ERROR = 3
EXIT_GENERAL_ERROR = 4

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("YouTubeOSINT")


# =====================================================================
# Pipeline Statistics Tracker
# =====================================================================

@dataclass
class PipelineStatistics:
    """Tracks runtime metrics across the entire pipeline execution."""

    start_time: float = 0.0
    end_time: float = 0.0
    api_calls_youtube: int = 0
    api_calls_openrouter: int = 0
    api_errors_youtube: int = 0
    api_errors_openrouter: int = 0
    videos_scanned: int = 0
    comments_collected: int = 0
    comments_disabled_videos: int = 0
    ai_model_used: str = "none"
    ai_models_attempted: List[str] = field(default_factory=list)
    ai_analysis_time_seconds: float = 0.0
    info_txt_lines: int = 0
    info_txt_bytes: int = 0
    output_files_generated: List[str] = field(default_factory=list)
    partial_failure: bool = False
    error_log: List[str] = field(default_factory=list)

    def start(self):
        """Mark pipeline start."""
        self.start_time = time.time()

    def stop(self):
        """Mark pipeline end."""
        self.end_time = time.time()

    @property
    def elapsed_seconds(self) -> float:
        end = self.end_time if self.end_time else time.time()
        return end - self.start_time

    @property
    def elapsed_display(self) -> str:
        elapsed = self.elapsed_seconds
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        if minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    def record_error(self, source: str, message: str):
        """Log an error event."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = f"[{timestamp}] [{source}] {message}"
        self.error_log.append(entry)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_runtime": self.elapsed_display,
            "total_runtime_seconds": round(self.elapsed_seconds, 2),
            "youtube_api_calls": self.api_calls_youtube,
            "youtube_api_errors": self.api_errors_youtube,
            "openrouter_api_calls": self.api_calls_openrouter,
            "openrouter_api_errors": self.api_errors_openrouter,
            "videos_scanned": self.videos_scanned,
            "comments_collected": self.comments_collected,
            "comments_disabled_videos": self.comments_disabled_videos,
            "ai_model_used": self.ai_model_used,
            "ai_models_attempted": self.ai_models_attempted,
            "ai_analysis_time_seconds": round(self.ai_analysis_time_seconds, 2),
            "info_txt_lines": self.info_txt_lines,
            "info_txt_bytes": self.info_txt_bytes,
            "output_files_count": len(self.output_files_generated),
            "partial_failure": self.partial_failure,
            "errors_encountered": len(self.error_log),
            "error_log": self.error_log,
        }


# =====================================================================
# 2. UTILITY & SANITIZATION MODULE
# =====================================================================

class ConfigManager:
    """Manages environment variables and runtime configurations."""

    def __init__(self):
        load_dotenv()
        self.youtube_api_key: Optional[str] = os.getenv("YOUTUBE_API_KEY")
        self.openrouter_api_key: Optional[str] = os.getenv("OPENROUTER_API_KEY")
        self.openrouter_model: str = os.getenv(
            "OPENROUTER_MODEL", OPENROUTER_FALLBACK_MODELS[0]
        )

    def validate(self) -> bool:
        """Check that all required API keys are present in the environment."""
        missing = []
        if not self.youtube_api_key or self.youtube_api_key.strip() == "":
            missing.append("YOUTUBE_API_KEY")
        if not self.openrouter_api_key or self.openrouter_api_key.strip() == "":
            missing.append("OPENROUTER_API_KEY")

        if missing:
            logger.error(
                "Missing required environment variables in .env file: "
                f"{', '.join(missing)}"
            )
            logger.error(
                "Copy sample.env to .env and fill in your API keys."
            )
            return False

        # Sanity check: keys should not be placeholder values
        placeholders = [
            "your_youtube_api_key_here",
            "your_openrouter_api_key_here",
        ]
        if self.youtube_api_key.strip() in placeholders:
            logger.error("YOUTUBE_API_KEY is still set to the placeholder value.")
            return False
        if self.openrouter_api_key.strip() in placeholders:
            logger.error("OPENROUTER_API_KEY is still set to the placeholder value.")
            return False

        logger.info("Environment variables validated successfully.")
        return True


def clean_text(raw_text: Optional[str]) -> str:
    """
    Strips invisible Unicode directional marks, zero-width characters,
    BOM markers, HTML tags, and normalizes whitespace.
    """
    if not raw_text:
        return ""
    text = re.sub(r'<[^>]+>', '', raw_text)
    # Remove directional marks, zero-width joiners/non-joiners, BOM, etc.
    text = re.sub(
        r'[\u200e\u200f\u200b\u200c\u200d\u202a-\u202e'
        r'\u2066-\u2069\ufeff\u00ad\u034f\u061c'
        r'\u180e\u2060-\u2064\ufff9-\ufffb]',
        '',
        text,
    )
    return " ".join(text.split())


def clean_markdown_json(raw_text: str) -> str:
    """Extracts JSON from LLM responses that may be wrapped in markdown code blocks."""
    cleaned = raw_text.strip()

    # Handle <think>...</think> wrapper from reasoning models
    think_match = re.search(r'</think>\s*(.*)', cleaned, re.DOTALL)
    if think_match:
        cleaned = think_match.group(1).strip()

    # Strip markdown code fences
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()

    # Try to extract JSON object if there is surrounding text
    json_match = re.search(r'(\{[\s\S]*\})', cleaned)
    if json_match:
        cleaned = json_match.group(1)

    return cleaned.strip()


def sanitize_filename(filename: str, max_length: int = 200) -> str:
    """Sanitizes strings for safe filesystem usage with length capping."""
    sanitized = re.sub(r'[\\/*?:"<>|]', "_", filename)
    sanitized = re.sub(r'_+', '_', sanitized)
    sanitized = sanitized.strip('_. ')
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length].rstrip('_. ')
    return sanitized if sanitized else "unnamed_channel"


def format_number(value: Any) -> str:
    """Format a numeric value with comma separators for display."""
    if value is None:
        return "N/A"
    try:
        num = int(value)
        return f"{num:,}"
    except (ValueError, TypeError):
        return str(value)


def format_duration(iso_duration: Optional[str]) -> str:
    """
    Convert ISO 8601 duration (PT1H23M45S) to human-readable format (1h 23m 45s).
    Handles hours, minutes, seconds, and edge cases.
    """
    if not iso_duration:
        return "Unknown"

    match = re.match(
        r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?',
        iso_duration,
    )
    if not match:
        return iso_duration

    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds > 0 or not parts:
        parts.append(f"{seconds}s")

    return " ".join(parts)


def format_duration_seconds(total_seconds: float) -> str:
    """Convert raw seconds to a display string like '2m 15s'."""
    minutes = int(total_seconds // 60)
    seconds = int(total_seconds % 60)
    if minutes > 0:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def estimate_token_count(text: str) -> int:
    """
    Rough token estimation using the ~4 characters per token heuristic.
    Accurate enough for payload budgeting without requiring a tokenizer.
    """
    if not text:
        return 0
    return max(1, len(text) // 4)


def truncate_to_budget(text: str, max_chars: int, suffix: str = "...") -> str:
    """
    Truncate text to a character budget, trying to break at sentence boundaries.
    Falls back to word boundaries, then hard cut.
    """
    if not text or len(text) <= max_chars:
        return text or ""

    budget = max_chars - len(suffix)
    if budget <= 0:
        return suffix

    # Try sentence boundary
    candidate = text[:budget]
    last_period = candidate.rfind('. ')
    if last_period > budget * 0.5:
        return candidate[:last_period + 1] + suffix

    # Try word boundary
    last_space = candidate.rfind(' ')
    if last_space > budget * 0.3:
        return candidate[:last_space] + suffix

    # Hard cut
    return candidate + suffix


def build_section_divider(title: str, width: int = 80) -> str:
    """
    Build an ASCII section header for info.txt reports.

    Example output:
    ╔══════════════════════════════════════════════════════════════════════════════╗
    ║  SECTION TITLE                                                             ║
    ╚══════════════════════════════════════════════════════════════════════════════╝
    """
    inner_width = width - 2
    top = "+" + "=" * inner_width + "+"
    title_line = "| " + title.ljust(inner_width - 2) + " |"
    bottom = "+" + "=" * inner_width + "+"
    return f"\n{top}\n{title_line}\n{bottom}\n"


def build_sub_divider(title: str, width: int = 80) -> str:
    """Build a lighter sub-section divider."""
    return f"\n--- {title} {'-' * max(1, width - len(title) - 6)}\n"


def safe_get(data: Dict[str, Any], *keys, default: Any = "N/A") -> Any:
    """Safely traverse nested dict keys, returning default on miss."""
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key, default)
        else:
            return default
    return current if current is not None else default


# =====================================================================
# 3. YOUTUBE CHANNEL RESOLVER MODULE
# =====================================================================

class YouTubeResolver:
    """
    Resolves various forms of channel inputs to a standard UC-format Channel ID.

    Supported inputs:
        - Direct Channel IDs:  UC...
        - Handle format:       @username
        - Full URLs:           youtube.com/channel/UC...,  youtube.com/@handle,
                               youtube.com/c/CustomName,  youtube.com/user/LegacyName
        - Search queries:      Any freeform text (falls back to YouTube Search)
    """

    def __init__(self, youtube_service: Resource, stats: PipelineStatistics):
        self.yt = youtube_service
        self.stats = stats

    def resolve(self, input_str: str) -> str:
        """
        Master resolution method. Tries patterns in order of specificity,
        falling back to search as the last resort.
        """
        cleaned = input_str.strip()
        if not cleaned:
            raise ValueError("Channel input cannot be empty.")

        logger.info(f"Resolving channel input: '{cleaned}'")

        # 1. Direct Channel ID (UC + 22 chars)
        match = YT_PATTERNS["raw_channel_id"].match(cleaned)
        if match:
            channel_id = match.group(1)
            logger.info(f"[Resolver] Direct Channel ID matched: {channel_id}")
            return channel_id

        # 2. Channel URL: youtube.com/channel/UC...
        match = YT_PATTERNS["channel_id"].search(cleaned)
        if match:
            channel_id = match.group(1)
            logger.info(f"[Resolver] Channel URL pattern matched: {channel_id}")
            return channel_id

        # 3. Handle URL: youtube.com/@handle
        match = YT_PATTERNS["handle_url"].search(cleaned)
        if match:
            handle = "@" + match.group(1)
            logger.info(f"[Resolver] Handle URL detected, resolving: {handle}")
            result = self._resolve_handle(handle)
            if result:
                return result

        # 4. Raw handle: @username
        match = YT_PATTERNS["handle_raw"].match(cleaned)
        if match:
            handle = "@" + match.group(1)
            logger.info(f"[Resolver] Raw handle detected, resolving: {handle}")
            result = self._resolve_handle(handle)
            if result:
                return result

        # 5. Custom URL: youtube.com/c/CustomName
        match = YT_PATTERNS["custom_url"].search(cleaned)
        if match:
            custom_name = match.group(1)
            logger.info(f"[Resolver] Custom URL detected: /c/{custom_name}")
            result = self._resolve_by_search(custom_name)
            if result:
                return result

        # 6. Legacy user URL: youtube.com/user/Username
        match = YT_PATTERNS["user_url"].search(cleaned)
        if match:
            username = match.group(1)
            logger.info(f"[Resolver] Legacy user URL detected: /user/{username}")
            result = self._resolve_legacy_username(username)
            if result:
                return result

        # 7. Check if input contains @ anywhere (partial handle in URL)
        if "@" in cleaned:
            handle_match = re.search(r"@([a-zA-Z0-9_.-]+)", cleaned)
            if handle_match:
                handle = "@" + handle_match.group(1)
                logger.info(f"[Resolver] Extracted handle from input: {handle}")
                result = self._resolve_handle(handle)
                if result:
                    return result

        # 8. Strip URL to get the last path segment and search
        parsed_query = cleaned.rstrip("/").split("/")[-1]
        if parsed_query.startswith("@"):
            parsed_query = parsed_query[1:]

        # 9. Final fallback: YouTube search
        logger.info(f"[Resolver] Falling back to YouTube Search for: '{parsed_query}'")
        result = self._resolve_by_search(parsed_query)
        if result:
            return result

        raise ValueError(
            f"Unable to resolve YouTube Channel ID from input: '{input_str}'. "
            "Try using a direct channel ID (UC...) or a @handle."
        )

    def _resolve_handle(self, handle: str) -> Optional[str]:
        """Resolve a @handle using the YouTube Data API forHandle parameter."""
        try:
            self.stats.api_calls_youtube += 1
            response = self.yt.channels().list(
                part="id",
                forHandle=handle,
            ).execute()

            items = response.get("items", [])
            if items:
                channel_id = items[0]["id"]
                logger.info(f"[Resolver] Handle {handle} -> {channel_id}")
                return channel_id
            else:
                logger.warning(f"[Resolver] Handle '{handle}' returned no results.")
        except HttpError as err:
            self.stats.api_errors_youtube += 1
            logger.warning(f"[Resolver] Handle API error for '{handle}': {err}")
        return None

    def _resolve_legacy_username(self, username: str) -> Optional[str]:
        """Resolve a legacy YouTube username using the forUsername parameter."""
        try:
            self.stats.api_calls_youtube += 1
            response = self.yt.channels().list(
                part="id",
                forUsername=username,
            ).execute()

            items = response.get("items", [])
            if items:
                channel_id = items[0]["id"]
                logger.info(
                    f"[Resolver] Legacy username '{username}' -> {channel_id}"
                )
                return channel_id
            else:
                logger.warning(
                    f"[Resolver] Legacy username '{username}' not found, "
                    "trying search..."
                )
                return self._resolve_by_search(username)
        except HttpError as err:
            self.stats.api_errors_youtube += 1
            logger.warning(
                f"[Resolver] Legacy username API error for '{username}': {err}"
            )
        return None

    def _resolve_by_search(self, query: str) -> Optional[str]:
        """Resolve a channel by searching YouTube."""
        try:
            self.stats.api_calls_youtube += 1
            search_response = self.yt.search().list(
                q=query,
                type="channel",
                part="id,snippet",
                maxResults=1,
            ).execute()

            items = search_response.get("items", [])
            if items:
                channel_id = items[0]["id"]["channelId"]
                channel_title = items[0]["snippet"]["title"]
                logger.info(
                    f"[Resolver] Search resolved '{query}' -> "
                    f"{channel_id} ({channel_title})"
                )
                return channel_id
            else:
                logger.warning(f"[Resolver] Search for '{query}' returned no results.")
        except HttpError as err:
            self.stats.api_errors_youtube += 1
            logger.error(f"[Resolver] Search API error for '{query}': {err}")
        return None

    def resolve_batch(self, inputs: List[str]) -> List[Tuple[str, Optional[str]]]:
        """
        Resolve multiple channel inputs. Returns list of (input, channel_id) tuples.
        channel_id is None if resolution failed.
        """
        results = []
        for inp in inputs:
            try:
                channel_id = self.resolve(inp)
                results.append((inp, channel_id))
            except ValueError as err:
                logger.error(f"[Resolver] Failed to resolve '{inp}': {err}")
                results.append((inp, None))
        return results


# =====================================================================
# 4. DATA EXTRACTOR ENGINE
# =====================================================================

class DownloaderModule:
    """Handles optional video downloading operations using yt-dlp."""

    def __init__(self, download_dir: Path = DEFAULT_DOWNLOAD_DIR):
        self.download_dir = download_dir
        self._ffmpeg_available: Optional[bool] = None
        self._ffmpeg_path: Optional[str] = None
        if YT_DLP_AVAILABLE:
            self.download_dir.mkdir(parents=True, exist_ok=True)

    def _check_ffmpeg(self) -> bool:
        """
        Check if ffmpeg binary is available. Checks in order:
            1. System PATH
            2. static-ffmpeg bundled binaries (pip install static-ffmpeg)
            3. imageio-ffmpeg bundled binary (pip install imageio-ffmpeg)
        """
        if self._ffmpeg_available is not None:
            return self._ffmpeg_available

        import shutil

        # Check system PATH first
        system_ffmpeg = shutil.which("ffmpeg")
        if system_ffmpeg:
            self._ffmpeg_available = True
            self._ffmpeg_path = str(Path(system_ffmpeg).parent)
            logger.info(f"[Downloader] ffmpeg found on PATH: {system_ffmpeg}")
            return True

        # Try static-ffmpeg
        try:
            import static_ffmpeg
            import static_ffmpeg.run
            
            static_ffmpeg.add_paths() # Adds ffmpeg/ffprobe to PATH
            
            # Find the actual directory containing the binaries
            bin_dir = Path(static_ffmpeg.run.get_or_fetch_platform_executables_else_raise()[0]).parent
            
            self._ffmpeg_available = True
            self._ffmpeg_path = str(bin_dir)
            logger.info(f"[Downloader] ffmpeg linked from static-ffmpeg: {self._ffmpeg_path}")
            return True
        except (ImportError, Exception) as e:
            pass

        # Try imageio-ffmpeg bundled binary
        try:
            import imageio_ffmpeg
            bundled = imageio_ffmpeg.get_ffmpeg_exe()
            if bundled and Path(bundled).exists():
                bundled_path = Path(bundled)

                alias_dir = self.download_dir / ".ffmpeg_bin"
                alias_dir.mkdir(parents=True, exist_ok=True)
                alias_exe = alias_dir / "ffmpeg.exe"

                if not alias_exe.exists():
                    try:
                        alias_exe.symlink_to(bundled_path)
                    except (OSError, NotImplementedError):
                        shutil.copy2(str(bundled_path), str(alias_exe))

                alias_dir_str = str(alias_dir)
                current_path = os.environ.get("PATH", "")
                if alias_dir_str not in current_path:
                    os.environ["PATH"] = alias_dir_str + os.pathsep + current_path

                self._ffmpeg_available = True
                self._ffmpeg_path = alias_dir_str
                logger.info(f"[Downloader] ffmpeg linked from imageio-ffmpeg: {alias_exe}")
                return True
        except (ImportError, Exception) as e:
            pass

        self._ffmpeg_available = False
        logger.warning(
            "[Downloader] ffmpeg not found. "
            "Downloads will use single-stream format (may be lower quality). "
            "Install for best quality:  pip install static-ffmpeg"
        )
        return False

    def download(self, video_id: str, enabled: bool = False) -> Dict[str, Any]:
        """Download a video using yt-dlp if enabled and available."""
        if not enabled:
            return {"status": "skipped", "reason": "download_disabled"}

        if not YT_DLP_AVAILABLE:
            logger.warning("yt-dlp package is not installed. Download skipped.")
            return {"status": "failed", "reason": "yt_dlp_missing"}

        video_url = f"https://www.youtube.com/watch?v={video_id}"
        output_template = str(self.download_dir / "%(title)s [%(id)s].%(ext)s")

        # Format selection:
        #   With ffmpeg: merge best separate video + audio streams (highest quality)
        #   Without ffmpeg: pick the best SINGLE file that has BOTH video AND audio
        #     vcodec!=none ensures video is present
        #     acodec!=none ensures audio is present
        if self._check_ffmpeg():
            fmt = (
                "bestvideo[ext=mp4]+bestaudio[ext=m4a]/"
                "bestvideo+bestaudio/"
                "best[vcodec!=none][acodec!=none]/"
                "best"
            )
        else:
            fmt = (
                "best[vcodec!=none][acodec!=none][ext=mp4]/"
                "best[vcodec!=none][acodec!=none]/"
                "best"
            )

        ydl_opts = {
            "format": fmt,
            "outtmpl": output_template,
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "noprogress": True,
            "merge_output_format": "mp4",
            "extractor_args": {"youtube": ["player_client=ios,android"]},
        }

        # Point yt-dlp to ffmpeg if we found it outside system PATH
        if self._ffmpeg_path:
            ydl_opts["ffmpeg_location"] = self._ffmpeg_path
            
        # Support cookies.txt for bypassing DRM/403
        cookie_file = Path("cookies.txt")
        if cookie_file.exists():
            ydl_opts["cookiefile"] = str(cookie_file)
            logger.info("[Downloader] Found cookies.txt, using it to bypass DRM restrictions.")

        browsers_to_try = [None, "edge", "chrome", "firefox", "brave", "opera"]
        
        for browser in browsers_to_try:
            if browser:
                ydl_opts["cookiesfrombrowser"] = (browser,)
                logger.info(f"[Downloader] Retrying {video_id} with {browser} cookies to bypass 403...")

            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(video_url, download=True)
                    filename = ydl.prepare_filename(info)
                    # yt-dlp may change extension after merge
                    if not Path(filename).exists():
                        mp4_name = Path(filename).with_suffix(".mp4")
                        if mp4_name.exists():
                            filename = str(mp4_name)
                    file_size = None
                    try:
                        file_size = Path(filename).stat().st_size
                    except OSError:
                        pass
                    size_display = f" ({format_number(file_size)} bytes)" if file_size else ""
                    logger.info(f"[Downloader] Downloaded: {video_id}{size_display}")
                    return {
                        "status": "success",
                        "file_path": filename,
                        "title": info.get("title"),
                        "duration": info.get("duration"),
                        "filesize_bytes": file_size,
                        "format": info.get("format"),
                        "ffmpeg_used": self._ffmpeg_available,
                    }
            except Exception as e:
                error_msg = str(e)
                if len(error_msg) > 200:
                    error_msg = error_msg[:200] + "..."
                    
                # If we haven't tried all fallbacks yet, keep going
                if browser != browsers_to_try[-1]:
                    logger.warning(f"[Downloader] Attempt with {browser if browser else 'default'} failed ({error_msg}). Trying next...")
                    continue
                
                # If we've exhausted all options
                logger.warning(f"[Downloader] Failed to download {video_id} after all fallbacks: {error_msg}")
                return {"status": "failed", "error": error_msg}


class DataExtractorEngine:
    """
    Extracts all visible metadata, statistics, video details, and comment
    metrics from the YouTube Data API v3.

    Features:
        - Full channel metadata with all API parts
        - Paginated video scanning with progress tracking
        - Paginated comment extraction (up to MAX_COMMENTS_PER_VIDEO)
        - Retry logic on transient API failures
        - Comments-disabled detection
    """

    def __init__(
        self,
        youtube_service: Resource,
        downloader: DownloaderModule,
        stats: PipelineStatistics,
    ):
        self.yt = youtube_service
        self.downloader = downloader
        self.stats = stats

    def _api_call_with_retry(
        self,
        request_func,
        description: str = "API call",
        max_retries: int = 3,
    ) -> Optional[Dict[str, Any]]:
        """
        Execute a YouTube API call with retry logic for transient errors.

        Retries on:
            - HTTP 500, 503 (server errors)
            - HTTP 429 (rate limit)
            - Network timeouts
        """
        for attempt in range(1, max_retries + 1):
            try:
                self.stats.api_calls_youtube += 1
                request = request_func()
                response = request.execute()
                return response
            except HttpError as err:
                status_code = err.resp.status if err.resp else 0
                self.stats.api_errors_youtube += 1

                if status_code in (429, 500, 503) and attempt < max_retries:
                    backoff = 2 ** attempt
                    logger.warning(
                        f"[Extractor] {description} failed (HTTP {status_code}), "
                        f"retry {attempt}/{max_retries} in {backoff}s..."
                    )
                    time.sleep(backoff)
                    continue
                elif status_code == 403:
                    logger.error(
                        f"[Extractor] {description} returned 403 Forbidden. "
                        "Check API key and quota."
                    )
                    self.stats.record_error("Extractor", f"{description}: 403 Forbidden")
                    return None
                else:
                    logger.error(f"[Extractor] {description} failed: {err}")
                    self.stats.record_error("Extractor", f"{description}: {err}")
                    return None
            except Exception as err:
                self.stats.api_errors_youtube += 1
                if attempt < max_retries:
                    backoff = 2 ** attempt
                    logger.warning(
                        f"[Extractor] {description} error: {err}, "
                        f"retry {attempt}/{max_retries} in {backoff}s..."
                    )
                    time.sleep(backoff)
                    continue
                logger.error(f"[Extractor] {description} failed after retries: {err}")
                self.stats.record_error("Extractor", f"{description}: {err}")
                return None
        return None

    def fetch_channel_metadata(self, channel_id: str) -> Dict[str, Any]:
        """
        Fetch complete channel metadata including snippet, statistics,
        content details, branding, topic details, and status.
        """
        logger.info(f"[Extractor] Fetching channel metadata for: {channel_id}")

        response = self._api_call_with_retry(
            lambda: self.yt.channels().list(
                part="snippet,statistics,contentDetails,brandingSettings,"
                     "topicDetails,status",
                id=channel_id,
            ),
            description=f"Channel metadata for {channel_id}",
        )

        if not response:
            raise ValueError(
                f"Failed to fetch channel metadata for ID: {channel_id}. "
                "The API request failed."
            )

        items = response.get("items", [])
        if not items:
            raise ValueError(f"No channel found with ID: {channel_id}")

        info = items[0]
        snippet = info.get("snippet", {})
        stats = info.get("statistics", {})
        branding = info.get("brandingSettings", {}).get("channel", {})
        topics = info.get("topicDetails", {})
        status = info.get("status", {})
        content_details = info.get("contentDetails", {})
        uploads_playlist = content_details.get("relatedPlaylists", {}).get(
            "uploads", ""
        )

        thumbnails = snippet.get("thumbnails", {})
        thumbnail_url = (
            thumbnails.get("high", {}).get("url")
            or thumbnails.get("medium", {}).get("url")
            or thumbnails.get("default", {}).get("url")
            or ""
        )

        channel_meta = {
            "channel_id": channel_id,
            "title": clean_text(snippet.get("title", "")),
            "description": clean_text(snippet.get("description", "")),
            "description_raw": snippet.get("description", ""),
            "published_at": snippet.get("publishedAt", ""),
            "country": snippet.get("country", "Unknown"),
            "custom_url": snippet.get("customUrl", ""),
            "default_language": snippet.get("defaultLanguage", "Not specified"),
            "subscriber_count": stats.get("subscriberCount", "Hidden"),
            "view_count": stats.get("viewCount", "0"),
            "video_count": stats.get("videoCount", "0"),
            "hidden_subscriber_count": stats.get("hiddenSubscriberCount", False),
            "keywords": clean_text(branding.get("keywords", "")),
            "topic_categories": topics.get("topicCategories", []),
            "topic_ids": topics.get("topicIds", []),
            "uploads_playlist_id": uploads_playlist,
            "privacy_status": status.get("privacyStatus", "Unknown"),
            "is_linked": status.get("isLinked", False),
            "long_uploads_status": status.get("longUploadsStatus", "Unknown"),
            "made_for_kids": status.get("madeForKids", False),
            "thumbnail_url": thumbnail_url,
            "branding_title": branding.get("title", ""),
            "branding_description": clean_text(branding.get("description", "")),
            "unsubscribed_trailer": branding.get("unsubscribedTrailer", ""),
        }

        logger.info(
            f"[Extractor] Channel: {channel_meta['title']} | "
            f"Subscribers: {format_number(channel_meta['subscriber_count'])} | "
            f"Videos: {format_number(channel_meta['video_count'])} | "
            f"Views: {format_number(channel_meta['view_count'])}"
        )

        return channel_meta

    def fetch_comments_for_video(
        self,
        video_id: str,
        max_comments: int = MAX_COMMENTS_PER_VIDEO,
        max_pages: int = COMMENT_PAGES_MAX,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Fetch comments for a video with full pagination support.

        Returns:
            Tuple of (comments_list, comments_disabled_flag)
        """
        comments: List[Dict[str, Any]] = []
        next_page_token: Optional[str] = None
        pages_fetched = 0
        comments_disabled = False

        while pages_fetched < max_pages and len(comments) < max_comments:
            per_page = min(COMMENTS_PER_PAGE, max_comments - len(comments))
            if per_page <= 0:
                break

            try:
                self.stats.api_calls_youtube += 1
                request_params = {
                    "part": "snippet",
                    "videoId": video_id,
                    "maxResults": per_page,
                    "order": "relevance",
                    "textFormat": "plainText",
                }
                if next_page_token:
                    request_params["pageToken"] = next_page_token

                response = self.yt.commentThreads().list(
                    **request_params
                ).execute()

                for item in response.get("items", []):
                    top_comment = item["snippet"]["topLevelComment"]
                    snippet = top_comment["snippet"]

                    comments.append({
                        "comment_id": top_comment.get("id", item.get("id", "")),
                        "author": clean_text(snippet.get("authorDisplayName")),
                        "author_channel_id": snippet.get(
                            "authorChannelId", {}
                        ).get("value", "Unknown"),
                        "author_profile_url": snippet.get(
                            "authorChannelUrl", ""
                        ),
                        "text": clean_text(snippet.get("textDisplay")),
                        "text_original": clean_text(snippet.get("textOriginal", "")),
                        "likes": snippet.get("likeCount", 0),
                        "published_at": snippet.get("publishedAt", ""),
                        "updated_at": snippet.get("updatedAt", ""),
                        "viewer_rating": snippet.get("viewerRating", "none"),
                        "total_reply_count": item.get("snippet", {}).get(
                            "totalReplyCount", 0
                        ),
                    })

                next_page_token = response.get("nextPageToken")
                pages_fetched += 1

                if not next_page_token:
                    break

            except HttpError as err:
                self.stats.api_errors_youtube += 1
                status_code = err.resp.status if err.resp else 0

                if status_code == 403:
                    # Comments are disabled on this video
                    comments_disabled = True
                    logger.debug(
                        f"[Extractor] Comments disabled for video: {video_id}"
                    )
                elif status_code == 404:
                    logger.debug(
                        f"[Extractor] Video not found for comments: {video_id}"
                    )
                else:
                    logger.debug(
                        f"[Extractor] Comment fetch error for {video_id}: {err}"
                    )
                break
            except Exception as err:
                logger.debug(
                    f"[Extractor] Unexpected error fetching comments for "
                    f"{video_id}: {err}"
                )
                break

        self.stats.comments_collected += len(comments)
        if comments_disabled:
            self.stats.comments_disabled_videos += 1

        return comments, comments_disabled

    def scan_recent_uploads(
        self,
        uploads_playlist_id: str,
        scan_all: bool = False,
        max_videos: int = DEFAULT_VIDEOS_TO_SCAN,
        download_enabled: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Scan video uploads with full pagination, detailed metadata extraction,
        and progress logging.
        """
        logger.info(
            f"[Extractor] Scanning uploads playlist: {uploads_playlist_id} "
            f"({'ALL' if scan_all else f'max {max_videos}'} videos)"
        )

        videos: List[Dict[str, Any]] = []
        next_page_token: Optional[str] = None
        total_target = "ALL" if scan_all else str(max_videos)

        while True:
            # Calculate how many to fetch this page
            if scan_all:
                fetch_limit = 50
            else:
                remaining = max_videos - len(videos)
                if remaining <= 0:
                    break
                fetch_limit = min(50, remaining)

            # Fetch playlist page
            response = self._api_call_with_retry(
                lambda: self.yt.playlistItems().list(
                    playlistId=uploads_playlist_id,
                    part="snippet,contentDetails",
                    maxResults=fetch_limit,
                    pageToken=next_page_token,
                ),
                description="Playlist items fetch",
            )

            if not response:
                logger.error(
                    "[Extractor] Failed to fetch playlist items. "
                    "Returning partial results."
                )
                self.stats.partial_failure = True
                break

            items = response.get("items", [])
            if not items:
                break

            # Collect video IDs for batch detail fetch
            video_ids = [
                item.get("contentDetails", {}).get("videoId")
                for item in items
                if item.get("contentDetails", {}).get("videoId")
            ]

            if not video_ids:
                break

            # Batch fetch video details (statistics, content details, snippet)
            details_response = self._api_call_with_retry(
                lambda: self.yt.videos().list(
                    part="snippet,statistics,contentDetails,status",
                    id=",".join(video_ids),
                ),
                description="Video details batch fetch",
            )

            if not details_response:
                logger.warning(
                    "[Extractor] Could not fetch video details for batch. "
                    "Skipping batch."
                )
                self.stats.partial_failure = True
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
                continue

            video_details_map = {
                v["id"]: v for v in details_response.get("items", [])
            }

            # Process each video
            for item in items:
                vid_id = item.get("contentDetails", {}).get("videoId")
                if not vid_id or vid_id not in video_details_map:
                    continue

                v_detail = video_details_map[vid_id]
                v_stats = v_detail.get("statistics", {})
                v_snippet = v_detail.get("snippet", {})
                v_content = v_detail.get("contentDetails", {})
                v_status = v_detail.get("status", {})

                cleaned_title = clean_text(
                    v_snippet.get("title", item.get("snippet", {}).get("title", ""))
                )

                logger.info(
                    f"[Extractor] [{len(videos) + 1}/{total_target}] "
                    f"Processing: {cleaned_title} ({vid_id})"
                )

                # Fetch comments with pagination
                comments, comments_disabled = self.fetch_comments_for_video(vid_id)

                # Optional download
                dl_result = self.downloader.download(vid_id, enabled=download_enabled)

                # Build video record
                video_record = {
                    "video_id": vid_id,
                    "title": cleaned_title,
                    "description": clean_text(v_snippet.get("description", "")),
                    "description_raw": v_snippet.get("description", ""),
                    "published_at": v_snippet.get("publishedAt", ""),
                    "tags": v_snippet.get("tags", []),
                    "category_id": v_snippet.get("categoryId", ""),
                    "channel_id": v_snippet.get("channelId", ""),
                    "default_language": v_snippet.get("defaultLanguage", ""),
                    "default_audio_language": v_snippet.get(
                        "defaultAudioLanguage", ""
                    ),
                    "live_broadcast_content": v_snippet.get(
                        "liveBroadcastContent", "none"
                    ),
                    "views": v_stats.get("viewCount", "0"),
                    "likes": v_stats.get("likeCount", "0"),
                    "comment_count": v_stats.get("commentCount", "0"),
                    "favorite_count": v_stats.get("favoriteCount", "0"),
                    "duration": v_content.get("duration", ""),
                    "duration_human": format_duration(v_content.get("duration")),
                    "dimension": v_content.get("dimension", ""),
                    "definition": v_content.get("definition", ""),
                    "caption": v_content.get("caption", "false"),
                    "licensed_content": v_content.get("licensedContent", False),
                    "projection": v_content.get("projection", ""),
                    "upload_status": v_status.get("uploadStatus", ""),
                    "privacy_status": v_status.get("privacyStatus", ""),
                    "embeddable": v_status.get("embeddable", False),
                    "made_for_kids": v_status.get("madeForKids", False),
                    "comments_disabled": comments_disabled,
                    "sample_comments": comments,
                    "comment_sample_count": len(comments),
                    "download_status": dl_result,
                }

                videos.append(video_record)
                self.stats.videos_scanned += 1

                if not scan_all and len(videos) >= max_videos:
                    break

            # Pagination
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
            if not scan_all and len(videos) >= max_videos:
                break

        logger.info(
            f"[Extractor] Video scan complete: {len(videos)} videos processed, "
            f"{self.stats.comments_collected} comments collected."
        )
        return videos


# =====================================================================
# 5. PAYLOAD OPTIMIZER MODULE
# =====================================================================

class PayloadOptimizer:
    """
    Downsample and trim extracted data specifically for AI prompt payloads
    while preserving full raw data for the info.txt report.

    Strategy:
        1. Select top N videos by engagement (views + likes + comments)
        2. Trim descriptions to budget
        3. Select top comments per video
        4. Iteratively reduce until under token budget
    """

    def __init__(
        self,
        token_budget: int = PAYLOAD_TOKEN_BUDGET,
        max_videos: int = PAYLOAD_MAX_VIDEOS,
        max_comments_per_video: int = PAYLOAD_MAX_COMMENTS_PER_VIDEO,
        max_description_chars: int = PAYLOAD_MAX_DESCRIPTION_CHARS,
    ):
        self.token_budget = token_budget
        self.max_videos = max_videos
        self.max_comments_per_video = max_comments_per_video
        self.max_description_chars = max_description_chars

    def _engagement_score(self, video: Dict[str, Any]) -> int:
        """Calculate a simple engagement score for ranking videos."""
        try:
            views = int(video.get("views", 0))
            likes = int(video.get("likes", 0))
            comments = int(video.get("comment_count", 0))
            return views + (likes * 10) + (comments * 50)
        except (ValueError, TypeError):
            return 0

    def optimize(
        self,
        videos: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Create an optimized payload for the AI prompt.

        Returns:
            Tuple of (compressed_videos, optimization_report)
        """
        if not videos:
            return [], {"status": "empty", "original_count": 0, "optimized_count": 0}

        # Sort by engagement and take top N
        sorted_videos = sorted(
            videos, key=self._engagement_score, reverse=True
        )

        # Start with max budget and iteratively reduce
        candidate_count = min(len(sorted_videos), self.max_videos)
        max_desc = self.max_description_chars
        max_comments = self.max_comments_per_video

        for reduction_pass in range(5):
            compressed = []
            for v in sorted_videos[:candidate_count]:
                compressed_video = {
                    "video_id": v.get("video_id"),
                    "title": v.get("title"),
                    "published_at": v.get("published_at", "")[:10],
                    "views": v.get("views"),
                    "likes": v.get("likes"),
                    "comment_count": v.get("comment_count"),
                    "duration": v.get("duration_human", v.get("duration")),
                    "tags": v.get("tags", [])[:10],
                    "description": truncate_to_budget(
                        v.get("description", ""), max_desc
                    ),
                    "top_comments": [
                        {
                            "author": c.get("author", ""),
                            "text": truncate_to_budget(
                                c.get("text", ""), 200
                            ),
                            "likes": c.get("likes", 0),
                        }
                        for c in v.get("sample_comments", [])[:max_comments]
                    ],
                }
                compressed.append(compressed_video)

            # Check token budget
            payload_text = json.dumps(compressed, ensure_ascii=False)
            token_estimate = estimate_token_count(payload_text)

            if token_estimate <= self.token_budget:
                break

            # Reduce parameters for next pass
            candidate_count = max(3, candidate_count - 3)
            max_desc = max(100, max_desc - 50)
            max_comments = max(2, max_comments - 1)

        report = {
            "status": "optimized",
            "original_video_count": len(videos),
            "optimized_video_count": len(compressed),
            "estimated_tokens": estimate_token_count(
                json.dumps(compressed, ensure_ascii=False)
            ),
            "token_budget": self.token_budget,
            "reduction_passes": reduction_pass + 1,
            "max_description_chars": max_desc,
            "max_comments_per_video": max_comments,
        }

        logger.info(
            f"[Optimizer] Payload optimized: {report['original_video_count']} -> "
            f"{report['optimized_video_count']} videos, "
            f"~{report['estimated_tokens']} tokens "
            f"(budget: {self.token_budget})"
        )

        return compressed, report

    def generate_channel_summary_for_prompt(
        self, meta: Dict[str, Any]
    ) -> str:
        """Build a compact channel summary string for the AI prompt header."""
        lines = [
            f"Title: {meta.get('title', 'Unknown')}",
            f"Channel ID: {meta.get('channel_id', 'Unknown')}",
            f"Country: {meta.get('country', 'Unknown')}",
            f"Subscribers: {format_number(meta.get('subscriber_count'))}",
            f"Total Views: {format_number(meta.get('view_count'))}",
            f"Total Videos: {format_number(meta.get('video_count'))}",
            f"Created: {meta.get('published_at', 'Unknown')[:10]}",
            f"Language: {meta.get('default_language', 'Not specified')}",
            f"Keywords: {meta.get('keywords', 'None')}",
        ]
        description = truncate_to_budget(
            meta.get("description", ""), 500
        )
        lines.append(f"Description:\n{description}")
        return "\n".join(lines)


# =====================================================================
# 6. AI INTELLIGENCE ENGINE
# =====================================================================

class AIIntelligenceEngine:
    """
    OpenRouter-based OSINT analysis engine with multi-model waterfall
    fallback, error classification, and response validation.

    Fallback order:
        1. Primary model (from config)
        2-5. OPENROUTER_FALLBACK_MODELS cascade

    Error handling:
        - Rate limit (429): Backoff and retry
        - Server error (5xx): Backoff and retry
        - Timeout: Retry with extended timeout
        - Malformed JSON response: Retry with same model
        - All failures: Return graceful degradation placeholder
    """

    EXPECTED_JSON_KEYS = {
        "inferred_topic",
        "content_themes",
        "knowledge_web",
        "extracted_hidden_data",
        "audience_and_community",
        "sentiment_and_tone",
        "osint_summary_assessment",
    }

    def __init__(
        self,
        api_key: str,
        primary_model: str,
        stats: PipelineStatistics,
    ):
        self.api_key = api_key
        self.primary_model = primary_model
        self.stats = stats
        self.optimizer = PayloadOptimizer()

    def _build_model_queue(self) -> List[str]:
        """Build the ordered model queue with primary model first."""
        queue = [self.primary_model]
        for model in OPENROUTER_FALLBACK_MODELS:
            if model not in queue:
                queue.append(model)
        return queue

    def _build_prompt(
        self,
        channel_summary: str,
        compressed_videos: List[Dict[str, Any]],
    ) -> str:
        """Construct the OSINT analysis prompt for the LLM."""
        video_corpus = json.dumps(compressed_videos, indent=2, ensure_ascii=False)

        return f"""You are an expert digital forensics investigator and Open Source Intelligence (OSINT) analyst.
Conduct an in-depth intelligence synthesis on the provided YouTube channel data.

--- CHANNEL METADATA ---
{channel_summary}

--- SCRAPED VIDEOS AND USER COMMENTS CORPUS ---
{video_corpus}

--- INSTRUCTIONS & OUTPUT FORMAT ---
Perform a detailed intelligence assessment based strictly on the channel data and text corpus provided.
You MUST respond ONLY with a raw, valid JSON object containing the exact structure below.
Do NOT include any markdown formatting, code blocks, or explanatory text outside the JSON.

Expected JSON Structure:
{{
    "inferred_topic": "Detailed description of the primary niche, sub-niches, and overarching domain.",
    "content_themes": ["Theme 1", "Theme 2", "Theme 3", "..."],
    "knowledge_web": {{
        "key_entities": ["People, organizations, tools, software, or platforms referenced"],
        "technical_concepts": ["Core technologies, methodologies, software, or systems discussed"],
        "recurring_motifs": ["Recurring jokes, tropes, concepts, or project names"]
    }},
    "extracted_hidden_data": {{
        "contact_emails": ["Extracted email addresses"],
        "social_links": ["Extracted URLs, Twitter/X handles, GitHub profiles, Discord links"],
        "external_domains": ["Mentioned websites or domain names"],
        "potential_names_or_aliases": ["Real names, pseudonyms, or aliases inferred"]
    }},
    "audience_and_community": {{
        "audience_demographic_estimate": "Estimated target demographic and user persona",
        "community_engagement_level": "Assessment of user comments, engagement style, and community response",
        "common_feedback_or_questions": ["Key questions or requests repeatedly raised in comments"]
    }},
    "sentiment_and_tone": "Evaluation of creator communication style, tone, and audience sentiment.",
    "osint_summary_assessment": "Executive brief summarizing key findings, potential operational security (OPSEC) leaks, and channel profiling insight."
}}"""

    def _validate_response(self, data: Dict[str, Any]) -> bool:
        """Check that the AI response contains the expected top-level keys."""
        if not isinstance(data, dict):
            return False
        present_keys = set(data.keys())
        required_minimum = {"inferred_topic", "osint_summary_assessment"}
        return required_minimum.issubset(present_keys)

    def _classify_error(
        self, status_code: int, response_text: str
    ) -> str:
        """Classify an HTTP error for logging and decision-making."""
        if status_code == 429:
            return "RATE_LIMITED"
        elif status_code in (500, 502, 503, 504):
            return "SERVER_ERROR"
        elif status_code == 401:
            return "UNAUTHORIZED"
        elif status_code == 413:
            return "PAYLOAD_TOO_LARGE"
        elif status_code == 400:
            if "context_length" in response_text.lower():
                return "CONTEXT_OVERFLOW"
            return "BAD_REQUEST"
        elif status_code == 0:
            return "NETWORK_ERROR"
        return f"HTTP_{status_code}"

    def _generate_fallback_report(
        self, channel_meta: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a graceful degradation placeholder when all AI models fail.
        Includes basic automated analysis from raw data.
        """
        logger.warning(
            "[AI Engine] All models failed. Generating fallback report."
        )
        return {
            "error": "All OpenRouter models failed to respond.",
            "fallback_mode": True,
            "inferred_topic": (
                f"Channel '{channel_meta.get('title', 'Unknown')}' — "
                "AI analysis unavailable. Manual review recommended."
            ),
            "content_themes": ["Analysis unavailable — all AI models failed"],
            "knowledge_web": {
                "key_entities": ["Manual extraction required"],
                "technical_concepts": ["Manual extraction required"],
                "recurring_motifs": ["Manual extraction required"],
            },
            "extracted_hidden_data": {
                "contact_emails": [],
                "social_links": [],
                "external_domains": [],
                "potential_names_or_aliases": [],
                "note": (
                    "AI extraction failed. Review channel description and "
                    "video descriptions manually for contact information."
                ),
            },
            "audience_and_community": {
                "audience_demographic_estimate": "Analysis unavailable",
                "community_engagement_level": "Analysis unavailable",
                "common_feedback_or_questions": [],
            },
            "sentiment_and_tone": "Analysis unavailable — manual review recommended.",
            "osint_summary_assessment": (
                "AUTOMATED FALLBACK: All AI analysis models were unreachable. "
                "Raw telemetry data has been preserved in full below. "
                "Conduct manual OSINT review using the video and comment data."
            ),
        }

    def analyze(
        self,
        channel_meta: Dict[str, Any],
        videos: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Run the full AI OSINT analysis with multi-model waterfall fallback.

        Flow:
            1. Optimize payload
            2. Build prompt
            3. Try each model with AI_ATTEMPTS_PER_MODEL retries
            4. Validate response structure
            5. Fall back to next model on failure
            6. Return fallback report if all models exhausted
        """
        analysis_start = time.time()

        # Optimize payload
        compressed_videos, opt_report = self.optimizer.optimize(videos)
        channel_summary = self.optimizer.generate_channel_summary_for_prompt(
            channel_meta
        )

        # Build prompt
        prompt = self._build_prompt(channel_summary, compressed_videos)
        prompt_tokens = estimate_token_count(prompt)
        logger.info(
            f"[AI Engine] Prompt built: ~{prompt_tokens} estimated tokens"
        )

        # Build model queue
        model_queue = self._build_model_queue()
        cumulative_backoff = AI_BASE_BACKOFF_SECONDS

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": SCRIPT_REPO,
            "X-Title": f"YouTube OSINT Pipeline v{SCRIPT_VERSION}",
        }

        for model_index, model in enumerate(model_queue):
            logger.info(
                f"[AI Engine] Attempting model [{model_index + 1}/"
                f"{len(model_queue)}]: {model}"
            )
            self.stats.ai_models_attempted.append(model)

            payload = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.2,
            }

            for attempt in range(1, AI_ATTEMPTS_PER_MODEL + 1):
                attempt_start = time.time()
                try:
                    self.stats.api_calls_openrouter += 1

                    response = requests.post(
                        OPENROUTER_API_ENDPOINT,
                        headers=headers,
                        json=payload,
                        timeout=API_REQUEST_TIMEOUT,
                    )

                    attempt_time = time.time() - attempt_start

                    if response.status_code == 200:
                        data = response.json()
                        raw_text = (
                            data.get("choices", [{}])[0]
                            .get("message", {})
                            .get("content", "")
                            or ""
                        )

                        if not raw_text.strip():
                            logger.warning(
                                f"[AI Engine] Model {model} returned empty "
                                f"response (attempt {attempt})"
                            )
                            continue

                        # Parse JSON from response
                        cleaned_json = clean_markdown_json(raw_text)
                        try:
                            analysis_result = json.loads(cleaned_json)
                        except json.JSONDecodeError as json_err:
                            logger.warning(
                                f"[AI Engine] Model {model} returned invalid "
                                f"JSON (attempt {attempt}): {json_err}"
                            )
                            self.stats.record_error(
                                "AI Engine",
                                f"JSON parse error from {model}: {json_err}",
                            )
                            continue

                        # Validate structure
                        if not self._validate_response(analysis_result):
                            logger.warning(
                                f"[AI Engine] Model {model} response missing "
                                f"required keys (attempt {attempt})"
                            )
                            continue

                        # Success
                        self.stats.ai_model_used = model
                        self.stats.ai_analysis_time_seconds = (
                            time.time() - analysis_start
                        )

                        # Inject metadata
                        analysis_result["_meta"] = {
                            "model_used": model,
                            "attempt": attempt,
                            "response_time_seconds": round(attempt_time, 2),
                            "payload_optimization": opt_report,
                        }

                        logger.info(
                            f"[AI Engine] Analysis complete using {model} "
                            f"(attempt {attempt}, {attempt_time:.1f}s)"
                        )
                        return analysis_result

                    else:
                        # Non-200 response
                        error_class = self._classify_error(
                            response.status_code, response.text
                        )
                        self.stats.api_errors_openrouter += 1

                        logger.warning(
                            f"[AI Engine] Model {model} returned "
                            f"HTTP {response.status_code} [{error_class}] "
                            f"(attempt {attempt}/{AI_ATTEMPTS_PER_MODEL}): "
                            f"{response.text[:200]}"
                        )

                        self.stats.record_error(
                            "AI Engine",
                            f"{model}: HTTP {response.status_code} [{error_class}]",
                        )

                        if error_class == "CONTEXT_OVERFLOW":
                            logger.warning(
                                "[AI Engine] Context overflow — skipping "
                                "remaining attempts for this model."
                            )
                            break
                        elif error_class == "UNAUTHORIZED":
                            logger.error(
                                "[AI Engine] API key unauthorized. "
                                "Check OPENROUTER_API_KEY."
                            )
                            break

                except requests.exceptions.Timeout:
                    self.stats.api_errors_openrouter += 1
                    logger.warning(
                        f"[AI Engine] Model {model} timed out after "
                        f"{API_REQUEST_TIMEOUT}s (attempt {attempt})"
                    )
                    self.stats.record_error(
                        "AI Engine", f"{model}: Request timeout"
                    )

                except requests.exceptions.ConnectionError as conn_err:
                    self.stats.api_errors_openrouter += 1
                    logger.warning(
                        f"[AI Engine] Network error for {model} "
                        f"(attempt {attempt}): {conn_err}"
                    )
                    self.stats.record_error(
                        "AI Engine", f"{model}: Connection error"
                    )

                except Exception as e:
                    self.stats.api_errors_openrouter += 1
                    logger.warning(
                        f"[AI Engine] Unexpected error for {model} "
                        f"(attempt {attempt}): {e}"
                    )
                    self.stats.record_error(
                        "AI Engine", f"{model}: {type(e).__name__}: {e}"
                    )

                # Backoff between attempts
                if attempt < AI_ATTEMPTS_PER_MODEL:
                    logger.info(
                        f"[AI Engine] Backing off {cumulative_backoff:.1f}s "
                        f"before retry..."
                    )
                    time.sleep(cumulative_backoff)

            # Escalate backoff between models
            cumulative_backoff *= AI_BACKOFF_MULTIPLIER
            if model_index < len(model_queue) - 1:
                logger.info(
                    f"[AI Engine] Escalating to next model. "
                    f"Backoff: {cumulative_backoff:.1f}s"
                )
                time.sleep(cumulative_backoff)

        # All models exhausted
        self.stats.ai_analysis_time_seconds = time.time() - analysis_start
        self.stats.record_error(
            "AI Engine", "All candidate models failed to produce valid analysis"
        )
        return self._generate_fallback_report(channel_meta)


# =====================================================================
# 7. TEXT WRITER ENGINE (info.txt)
# =====================================================================

class TextWriterEngine:
    """
    Generates the exhaustive, un-summarized, unlimited-length info.txt report.

    Sections:
        Header:     Generation timestamp, script version, target channel
        Section 1:  Complete Channel Telemetry
        Section 2:  AI OSINT & Pattern Analysis (JSON)
        Section 3:  Granular Video & Comment Log
        Section 4:  Pipeline Execution Summary
        Footer:     End-of-report marker with metrics
    """

    def __init__(self, file_path: Path, stats: PipelineStatistics):
        self.file_path = file_path
        self.stats = stats
        self._line_count = 0

    def _write(self, f, text: str):
        """Write text and track line count."""
        f.write(text)
        self._line_count += text.count("\n")

    def generate(
        self,
        meta: Dict[str, Any],
        videos: List[Dict[str, Any]],
        ai_report: Dict[str, Any],
    ):
        """Generate the complete info.txt report."""
        logger.info(f"[Writer] Generating exhaustive info.txt: {self.file_path}")

        with open(self.file_path, "w", encoding="utf-8") as f:
            self._write_header(f, meta)
            self._write_channel_telemetry(f, meta)
            self._write_ai_analysis(f, ai_report)
            self._write_video_comment_log(f, videos)
            self._write_pipeline_summary(f)
            self._write_footer(f, meta)

        # Record file metrics
        file_size = self.file_path.stat().st_size
        self.stats.info_txt_lines = self._line_count
        self.stats.info_txt_bytes = file_size

        logger.info(
            f"[Writer] info.txt complete: {self._line_count:,} lines, "
            f"{file_size:,} bytes"
        )

    def _write_header(self, f, meta: Dict[str, Any]):
        """Write the report header block."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        local_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self._write(f, "=" * 80 + "\n")
        self._write(f, " " * 10 + "EXHAUSTIVE OSINT & CHANNEL TELEMETRY REPORT\n")
        self._write(f, "=" * 80 + "\n\n")
        self._write(f, f"  Target Channel    : {meta.get('title', 'Unknown')}\n")
        self._write(f, f"  Channel ID        : {meta.get('channel_id', 'Unknown')}\n")
        self._write(f, f"  Generation Time   : {now}\n")
        self._write(f, f"  Local Time        : {local_now}\n")
        self._write(f, f"  Script Version    : {SCRIPT_VERSION} ({SCRIPT_CODENAME})\n")
        self._write(f, f"  Script Author     : {SCRIPT_AUTHOR}\n")
        self._write(f, f"  Python Version    : {sys.version.split()[0]}\n")
        self._write(f, f"  Platform          : {sys.platform}\n")
        self._write(f, "\n" + "=" * 80 + "\n\n")

    def _write_channel_telemetry(self, f, meta: Dict[str, Any]):
        """Write Section 1: Complete Channel Telemetry."""
        self._write(f, build_section_divider(
            "SECTION 1: COMPLETE CHANNEL TELEMETRY"
        ))

        fields = [
            ("Channel Title", meta.get("title")),
            ("Channel ID", meta.get("channel_id")),
            ("Custom URL", meta.get("custom_url", "N/A")),
            ("Subscriber Count", format_number(meta.get("subscriber_count"))),
            ("Total View Count", format_number(meta.get("view_count"))),
            ("Total Video Count", format_number(meta.get("video_count"))),
            ("Hidden Subscriber Count", str(meta.get("hidden_subscriber_count", False))),
            ("Country", meta.get("country")),
            ("Creation Date", meta.get("published_at")),
            ("Default Language", meta.get("default_language")),
            ("Privacy Status", meta.get("privacy_status", "N/A")),
            ("Is Linked", str(meta.get("is_linked", "N/A"))),
            ("Long Uploads Status", meta.get("long_uploads_status", "N/A")),
            ("Made For Kids", str(meta.get("made_for_kids", False))),
            ("Uploads Playlist ID", meta.get("uploads_playlist_id")),
            ("Thumbnail URL", meta.get("thumbnail_url", "N/A")),
            ("Unsubscribed Trailer", meta.get("unsubscribed_trailer", "N/A")),
        ]

        max_label_len = max(len(label) for label, _ in fields)
        for label, value in fields:
            padded_label = label.ljust(max_label_len + 2)
            self._write(f, f"  {padded_label}: {value}\n")

        # Keywords
        self._write(f, f"\n  Channel Keywords / Tags:\n")
        keywords = meta.get("keywords", "")
        if keywords:
            self._write(f, f"    {keywords}\n")
        else:
            self._write(f, "    (none)\n")

        # Topic categories
        self._write(f, f"\n  Topic Categories:\n")
        topics = meta.get("topic_categories", [])
        if topics:
            for topic in topics:
                self._write(f, f"    - {topic}\n")
        else:
            self._write(f, "    (none)\n")

        # Full description
        self._write(f, f"\n  Full Channel Description (unedited):\n")
        self._write(f, "  " + "-" * 60 + "\n")
        description = meta.get("description_raw", meta.get("description", ""))
        if description:
            for line in description.split("\n"):
                self._write(f, f"    {line}\n")
        else:
            self._write(f, "    (no description)\n")
        self._write(f, "  " + "-" * 60 + "\n")

        # Branding description if different
        branding_desc = meta.get("branding_description", "")
        if branding_desc and branding_desc != description:
            self._write(f, f"\n  Branding Settings Description:\n")
            self._write(f, "  " + "-" * 60 + "\n")
            for line in branding_desc.split("\n"):
                self._write(f, f"    {line}\n")
            self._write(f, "  " + "-" * 60 + "\n")

        self._write(f, "\n")

    def _write_ai_analysis(self, f, ai_report: Dict[str, Any]):
        """Write Section 2: AI OSINT & Pattern Analysis."""
        self._write(f, build_section_divider(
            "SECTION 2: AI OSINT & PATTERN ANALYSIS"
        ))

        if ai_report.get("fallback_mode"):
            self._write(f, "  *** WARNING: AI analysis fallback mode ***\n")
            self._write(f, "  All AI models failed. Results below are automated\n")
            self._write(f, "  placeholders. Manual analysis is recommended.\n\n")

        # Write the full JSON report
        formatted_json = json.dumps(ai_report, indent=4, ensure_ascii=False)
        self._write(f, "  " + "-" * 60 + "\n")
        for line in formatted_json.split("\n"):
            self._write(f, f"  {line}\n")
        self._write(f, "  " + "-" * 60 + "\n")

        # Also write key findings in readable format if available
        if not ai_report.get("error"):
            self._write(f, "\n  --- Key Findings Summary ---\n\n")

            inferred = ai_report.get("inferred_topic", "")
            if inferred:
                self._write(f, f"  Inferred Topic:\n    {inferred}\n\n")

            themes = ai_report.get("content_themes", [])
            if themes:
                self._write(f, "  Content Themes:\n")
                for theme in themes:
                    self._write(f, f"    - {theme}\n")
                self._write(f, "\n")

            knowledge = ai_report.get("knowledge_web", {})
            if knowledge:
                self._write(f, "  Knowledge Web:\n")
                for key, values in knowledge.items():
                    display_key = key.replace("_", " ").title()
                    self._write(f, f"    {display_key}:\n")
                    if isinstance(values, list):
                        for v in values:
                            self._write(f, f"      - {v}\n")
                    else:
                        self._write(f, f"      {values}\n")
                self._write(f, "\n")

            hidden = ai_report.get("extracted_hidden_data", {})
            if hidden:
                self._write(f, "  Extracted Hidden Data:\n")
                for key, values in hidden.items():
                    display_key = key.replace("_", " ").title()
                    self._write(f, f"    {display_key}:\n")
                    if isinstance(values, list):
                        if values:
                            for v in values:
                                self._write(f, f"      - {v}\n")
                        else:
                            self._write(f, "      (none found)\n")
                    else:
                        self._write(f, f"      {values}\n")
                self._write(f, "\n")

            audience = ai_report.get("audience_and_community", {})
            if audience:
                self._write(f, "  Audience & Community:\n")
                for key, value in audience.items():
                    display_key = key.replace("_", " ").title()
                    if isinstance(value, list):
                        self._write(f, f"    {display_key}:\n")
                        for v in value:
                            self._write(f, f"      - {v}\n")
                    else:
                        self._write(f, f"    {display_key}: {value}\n")
                self._write(f, "\n")

            sentiment = ai_report.get("sentiment_and_tone", "")
            if sentiment:
                self._write(f, f"  Sentiment & Tone:\n    {sentiment}\n\n")

            osint_summary = ai_report.get("osint_summary_assessment", "")
            if osint_summary:
                self._write(f, f"  OSINT Summary Assessment:\n    {osint_summary}\n\n")

    def _write_video_comment_log(self, f, videos: List[Dict[str, Any]]):
        """Write Section 3: Granular Video & Comment Log."""
        self._write(f, build_section_divider(
            "SECTION 3: GRANULAR VIDEO & COMMENT LOG"
        ))

        self._write(f, f"  Total Videos Scanned: {len(videos)}\n")
        total_comments = sum(
            len(v.get("sample_comments", [])) for v in videos
        )
        self._write(f, f"  Total Comments Captured: {total_comments}\n\n")

        for idx, vid in enumerate(videos, 1):
            self._write(f, "=" * 80 + "\n")
            self._write(
                f,
                f"  VIDEO [{idx}/{len(videos)}]: {vid.get('title', 'Unknown')}\n",
            )
            self._write(f, "=" * 80 + "\n\n")

            # Video metadata block
            video_fields = [
                ("Video ID", vid.get("video_id")),
                ("Published At", vid.get("published_at")),
                ("Duration (ISO)", vid.get("duration")),
                ("Duration (Human)", vid.get("duration_human", format_duration(vid.get("duration")))),
                ("Category ID", vid.get("category_id")),
                ("View Count", format_number(vid.get("views"))),
                ("Like Count", format_number(vid.get("likes"))),
                ("Comment Count", format_number(vid.get("comment_count"))),
                ("Favorite Count", format_number(vid.get("favorite_count", "0"))),
                ("Dimension", vid.get("dimension", "N/A")),
                ("Definition", vid.get("definition", "N/A")),
                ("Has Captions", vid.get("caption", "false")),
                ("Licensed Content", str(vid.get("licensed_content", False))),
                ("Projection", vid.get("projection", "N/A")),
                ("Upload Status", vid.get("upload_status", "N/A")),
                ("Privacy Status", vid.get("privacy_status", "N/A")),
                ("Embeddable", str(vid.get("embeddable", False))),
                ("Made For Kids", str(vid.get("made_for_kids", False))),
                ("Comments Disabled", str(vid.get("comments_disabled", False))),
                ("Default Language", vid.get("default_language", "N/A")),
                ("Default Audio Language", vid.get("default_audio_language", "N/A")),
                ("Live Broadcast Content", vid.get("live_broadcast_content", "none")),
            ]

            max_label = max(len(label) for label, _ in video_fields)
            for label, value in video_fields:
                padded = label.ljust(max_label + 2)
                self._write(f, f"    {padded}: {value}\n")

            # Tags
            tags = vid.get("tags", [])
            self._write(f, f"\n    Tags ({len(tags)}):\n")
            if tags:
                # Write tags in a wrapped format
                tag_line = "      "
                for i, tag in enumerate(tags):
                    tag_str = f'"{tag}"'
                    if i < len(tags) - 1:
                        tag_str += ", "
                    if len(tag_line) + len(tag_str) > 78:
                        self._write(f, tag_line + "\n")
                        tag_line = "      " + tag_str
                    else:
                        tag_line += tag_str
                if tag_line.strip():
                    self._write(f, tag_line + "\n")
            else:
                self._write(f, "      (no tags)\n")

            # Download status
            dl_status = vid.get("download_status", {})
            self._write(f, f"\n    Download Status: {json.dumps(dl_status)}\n")

            # Full description
            self._write(f, f"\n    Full Video Description:\n")
            self._write(f, "    " + "-" * 50 + "\n")
            description = vid.get(
                "description_raw", vid.get("description", "")
            )
            if description:
                for line in description.split("\n"):
                    self._write(f, f"      {line}\n")
            else:
                self._write(f, "      (no description)\n")
            self._write(f, "    " + "-" * 50 + "\n")

            # Comments
            comments = vid.get("sample_comments", [])
            self._write(f, f"\n    Comments Captured ({len(comments)}):\n")

            if vid.get("comments_disabled"):
                self._write(f, "      ** Comments are DISABLED on this video **\n")
            elif not comments:
                self._write(f, "      (no comments captured)\n")
            else:
                for c_idx, comment in enumerate(comments, 1):
                    self._write(f, f"\n      Comment [{c_idx}/{len(comments)}]:\n")
                    self._write(
                        f,
                        f"        Comment ID       : {comment.get('comment_id', 'N/A')}\n",
                    )
                    self._write(
                        f,
                        f"        Author           : {comment.get('author', 'Unknown')}\n",
                    )
                    self._write(
                        f,
                        f"        Author Channel ID: {comment.get('author_channel_id', 'Unknown')}\n",
                    )
                    self._write(
                        f,
                        f"        Profile URL      : {comment.get('author_profile_url', 'N/A')}\n",
                    )
                    self._write(
                        f,
                        f"        Published At     : {comment.get('published_at', 'N/A')}\n",
                    )
                    self._write(
                        f,
                        f"        Last Updated At  : {comment.get('updated_at', 'N/A')}\n",
                    )
                    self._write(
                        f,
                        f"        Like Count       : {format_number(comment.get('likes', 0))}\n",
                    )
                    self._write(
                        f,
                        f"        Reply Count      : {format_number(comment.get('total_reply_count', 0))}\n",
                    )
                    self._write(f, f"        Full Text:\n")
                    comment_text = comment.get("text", "")
                    if comment_text:
                        for line in comment_text.split("\n"):
                            self._write(f, f"          {line}\n")
                    else:
                        self._write(f, "          (empty comment)\n")

            self._write(f, "\n" + "-" * 80 + "\n\n")

    def _write_pipeline_summary(self, f):
        """Write Section 4: Pipeline Execution Summary."""
        self._write(f, build_section_divider(
            "SECTION 4: PIPELINE EXECUTION SUMMARY"
        ))

        summary = self.stats.to_dict()

        summary_fields = [
            ("Total Runtime", summary.get("total_runtime")),
            ("YouTube API Calls", str(summary.get("youtube_api_calls"))),
            ("YouTube API Errors", str(summary.get("youtube_api_errors"))),
            ("OpenRouter API Calls", str(summary.get("openrouter_api_calls"))),
            ("OpenRouter API Errors", str(summary.get("openrouter_api_errors"))),
            ("Videos Scanned", str(summary.get("videos_scanned"))),
            ("Comments Collected", str(summary.get("comments_collected"))),
            ("Comments Disabled Videos", str(summary.get("comments_disabled_videos"))),
            ("AI Model Used", summary.get("ai_model_used")),
            (
                "AI Analysis Time",
                f"{summary.get('ai_analysis_time_seconds', 0)}s",
            ),
            ("Output Files Generated", str(summary.get("output_files_count"))),
            ("Partial Failure", str(summary.get("partial_failure"))),
            ("Total Errors", str(summary.get("errors_encountered"))),
        ]

        max_label = max(len(label) for label, _ in summary_fields)
        for label, value in summary_fields:
            padded = label.ljust(max_label + 2)
            self._write(f, f"  {padded}: {value}\n")

        # AI models attempted
        models = summary.get("ai_models_attempted", [])
        if models:
            self._write(f, "\n  AI Models Attempted:\n")
            for i, model in enumerate(models, 1):
                status = "USED" if model == summary.get("ai_model_used") else "FAILED"
                self._write(f, f"    {i}. {model} [{status}]\n")

        # Error log
        errors = summary.get("error_log", [])
        if errors:
            self._write(f, "\n  Error Log:\n")
            for error in errors:
                self._write(f, f"    {error}\n")

        self._write(f, "\n")

    def _write_footer(self, f, meta: Dict[str, Any]):
        """Write the end-of-report footer."""
        self._write(f, "=" * 80 + "\n")
        self._write(f, "  END OF REPORT\n")
        self._write(f, f"  Channel: {meta.get('title', 'Unknown')}\n")
        self._write(f, f"  Report Lines: ~{self._line_count:,}\n")
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        self._write(f, f"  Completed At: {now}\n")
        self._write(f, "=" * 80 + "\n")
        self._write(f, f"\n[Generated by {SCRIPT_CODENAME} v{SCRIPT_VERSION}]\n")


# =====================================================================
# 8. PIPELINE ORCHESTRATOR
# =====================================================================

class PipelineOrchestrator:
    """
    Coordinates execution across all pipeline stages:
        1. Input resolution
        2. Channel metadata extraction
        3. Video & comment scanning
        4. AI OSINT analysis
        5. Report generation (JSON + info.txt)

    Features:
        - Pre-flight API key validation
        - Progress tracking via PipelineStatistics
        - Partial-result saving on mid-pipeline failures
        - Post-run summary with file sizes and paths
    """

    def __init__(
        self,
        config: ConfigManager,
        output_dir: Path = DEFAULT_OUTPUT_DIR,
    ):
        self.config = config
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.stats = PipelineStatistics()

        # Build YouTube service
        self.youtube_service = build(
            "youtube", "v3", developerKey=config.youtube_api_key
        )

        # Initialize modules
        self.resolver = YouTubeResolver(self.youtube_service, self.stats)
        self.downloader = DownloaderModule()
        self.extractor = DataExtractorEngine(
            self.youtube_service, self.downloader, self.stats
        )
        self.ai_engine = AIIntelligenceEngine(
            config.openrouter_api_key, config.openrouter_model, self.stats
        )

    def _preflight_check(self) -> bool:
        """
        Run a minimal YouTube API call to verify the API key works
        before committing to a full pipeline run.
        """
        logger.info("[Pipeline] Running pre-flight API key check...")
        try:
            self.stats.api_calls_youtube += 1
            response = (
                self.youtube_service.channels()
                .list(part="id", id="UC_x5XG1OV2P6uZZ5FSM9Ttw")
                .execute()
            )
            if response.get("items"):
                logger.info("[Pipeline] Pre-flight check passed.")
                return True
            else:
                logger.warning(
                    "[Pipeline] Pre-flight check returned empty. "
                    "API key may have limited access."
                )
                return True  # Allow to continue, might still work
        except HttpError as err:
            self.stats.api_errors_youtube += 1
            status_code = err.resp.status if err.resp else 0
            if status_code == 403:
                logger.error(
                    "[Pipeline] Pre-flight FAILED: API key is invalid or "
                    "YouTube Data API is not enabled in Google Cloud Console."
                )
            elif status_code == 400:
                logger.error(
                    f"[Pipeline] Pre-flight FAILED: Bad request — {err}"
                )
            else:
                logger.error(
                    f"[Pipeline] Pre-flight FAILED: {err}"
                )
            return False
        except Exception as err:
            logger.error(f"[Pipeline] Pre-flight FAILED: {err}")
            return False

    def _save_json_output(
        self,
        data: Any,
        filename: str,
        description: str,
    ) -> Path:
        """Save a JSON output file and track it in stats."""
        file_path = self.output_dir / filename
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        self.stats.output_files_generated.append(str(file_path))
        size = file_path.stat().st_size
        logger.info(
            f"[Pipeline] Saved {description}: {file_path} "
            f"({size:,} bytes)"
        )
        return file_path

    def run(
        self,
        target_input: str,
        scan_all: bool = False,
        max_videos: int = DEFAULT_VIDEOS_TO_SCAN,
        download_enabled: bool = False,
    ):
        """
        Execute the full intelligence pipeline.

        Steps:
            1. Pre-flight API check
            2. Resolve channel input to Channel ID
            3. Fetch channel metadata
            4. Scan videos and comments
            5. Run AI OSINT analysis
            6. Generate output files
        """
        self.stats.start()
        logger.info("=" * 60)
        logger.info("  YouTube Intelligence Pipeline — Starting")
        logger.info("=" * 60)
        logger.info(f"  Target: {target_input}")
        logger.info(f"  Scan mode: {'Full channel' if scan_all else f'Max {max_videos} videos'}")
        logger.info(f"  Downloads: {'Enabled' if download_enabled else 'Disabled'}")
        logger.info("")

        # Step 1: Pre-flight
        if not self._preflight_check():
            logger.error(
                "[Pipeline] Pre-flight check failed. "
                "Fix your API key and try again."
            )
            self.stats.stop()
            sys.exit(EXIT_CONFIG_ERROR)

        # Step 2: Resolve channel
        try:
            channel_id = self.resolver.resolve(target_input)
        except ValueError as err:
            logger.error(f"[Pipeline] Channel resolution failed: {err}")
            self.stats.stop()
            sys.exit(EXIT_RESOLUTION_ERROR)

        # Step 3: Fetch channel metadata
        try:
            meta = self.extractor.fetch_channel_metadata(channel_id)
        except ValueError as err:
            logger.error(f"[Pipeline] Channel metadata fetch failed: {err}")
            self.stats.stop()
            sys.exit(EXIT_EXTRACTION_ERROR)

        # Step 4: Scan videos and comments
        videos = []
        try:
            videos = self.extractor.scan_recent_uploads(
                meta["uploads_playlist_id"],
                scan_all=scan_all,
                max_videos=max_videos,
                download_enabled=download_enabled,
            )
        except Exception as err:
            logger.error(
                f"[Pipeline] Video scanning encountered an error: {err}. "
                "Saving partial results."
            )
            self.stats.partial_failure = True
            self.stats.record_error("Pipeline", f"Video scan error: {err}")

        # Step 5: AI Analysis
        ai_report = {}
        try:
            ai_report = self.ai_engine.analyze(meta, videos)
        except Exception as err:
            logger.error(
                f"[Pipeline] AI analysis encountered an error: {err}. "
                "Generating fallback report."
            )
            self.stats.record_error("Pipeline", f"AI analysis error: {err}")
            ai_report = self.ai_engine._generate_fallback_report(meta)

        # Step 6: Generate outputs
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_title = sanitize_filename(meta["title"])

        # 6a. Channel stats JSON
        stats_data = {
            "script_version": SCRIPT_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "channel_metadata": meta,
            "scanned_videos_count": len(videos),
            "total_comments_collected": self.stats.comments_collected,
        }
        stats_file = self._save_json_output(
            stats_data,
            f"{safe_title}_stats_{timestamp}.json",
            "Channel stats",
        )

        # 6b. Knowledge web JSON (full data dump)
        knowledge_data = {
            "script_version": SCRIPT_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "channel_metadata": meta,
            "scanned_videos": videos,
        }
        knowledge_file = self._save_json_output(
            knowledge_data,
            f"{safe_title}_knowledge_web_{timestamp}.json",
            "Knowledge web",
        )

        # 6c. AI analysis JSON
        ai_data = {
            "script_version": SCRIPT_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "channel_title": meta["title"],
            "channel_id": meta["channel_id"],
            "openrouter_analysis": ai_report,
        }
        ai_file = self._save_json_output(
            ai_data,
            f"{safe_title}_info_{timestamp}.json",
            "AI analysis",
        )

        # 6d. Exhaustive info.txt
        info_txt_path = self.output_dir / f"{safe_title}_info_{timestamp}.txt"
        writer = TextWriterEngine(info_txt_path, self.stats)
        writer.generate(meta, videos, ai_report)
        self.stats.output_files_generated.append(str(info_txt_path))

        # Pipeline complete
        self.stats.stop()

        # Print summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("  YouTube Intelligence Pipeline — Complete")
        logger.info("=" * 60)
        logger.info(f"  Channel        : {meta['title']}")
        logger.info(f"  Videos Scanned : {self.stats.videos_scanned}")
        logger.info(f"  Comments       : {self.stats.comments_collected}")
        logger.info(f"  AI Model       : {self.stats.ai_model_used}")
        logger.info(f"  Runtime        : {self.stats.elapsed_display}")
        logger.info("")
        logger.info("  Output Files:")
        logger.info(f"    [STATS]      {stats_file}")
        logger.info(f"    [KNOWLEDGE]  {knowledge_file}")
        logger.info(f"    [AI JSON]    {ai_file}")
        logger.info(f"    [INFO TXT]   {info_txt_path}")
        logger.info(
            f"    info.txt     {self.stats.info_txt_lines:,} lines | "
            f"{self.stats.info_txt_bytes:,} bytes"
        )
        if self.stats.partial_failure:
            logger.warning(
                "  ⚠ Pipeline completed with partial failures. "
                "Check error log above."
            )
        if self.stats.error_log:
            logger.info(f"  Errors: {len(self.stats.error_log)}")
        logger.info("=" * 60)


# =====================================================================
# 9. CLI INTERFACE
# =====================================================================

def print_banner():
    """Print the application banner (ASCII-safe for Windows console)."""
    banner = r"""
    +===========================================================+
    |                                                           |
    |   ____                    _____ _    _   _                 |
    |  / ___| _   _ _ __   __ |  ___/ \  | \ | |               |
    |  \___ \| | | | '_ \ / _`| |_ / _ \ |  \| |               |
    |   ___) | |_| | |_) | (_|| _|/ ___ \| |\  |               |
    |  |____/ \__,_| .__/ \__,|_|/_/   \_\_| \_|               |
    |              |_|                                          |
    |                                                           |
    |        YouTube Intelligence & OSINT Pipeline              |
    |                                                           |
    +===========================================================+
    """
    print(banner)
    print(f"    Version {SCRIPT_VERSION} ({SCRIPT_CODENAME}) by {SCRIPT_AUTHOR}")
    print(f"    Repository: {SCRIPT_REPO}")
    print()


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for non-interactive mode."""
    parser = argparse.ArgumentParser(
        prog="SupaFAN",
        description=(
            "YouTube OSINT & Intelligence Analysis Pipeline — "
            "Captures full channel metadata, video telemetry, comment "
            "statistics, runs AI OSINT analysis, and generates exhaustive "
            "info.txt reports."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python main.py --target @MrBeast\n"
            "  python main.py --target UCX6OQ3DkcsbYNE6H8uQQuVA --scan-all\n"
            "  python main.py --target \"https://youtube.com/@pewdiepie\" "
            "--max-videos 50\n"
            "  python main.py  (interactive mode)\n"
        ),
    )

    parser.add_argument(
        "--target", "-t",
        type=str,
        default=None,
        help=(
            "YouTube channel URL, handle (@username), or Channel ID (UC...). "
            "If not provided, the script runs in interactive mode."
        ),
    )
    parser.add_argument(
        "--scan-all", "-a",
        action="store_true",
        default=False,
        help="Scan ALL videos in the channel uploads playlist.",
    )
    parser.add_argument(
        "--max-videos", "-m",
        type=int,
        default=DEFAULT_VIDEOS_TO_SCAN,
        help=f"Maximum number of videos to scan (default: {DEFAULT_VIDEOS_TO_SCAN}).",
    )
    parser.add_argument(
        "--download", "-d",
        action="store_true",
        default=False,
        help="Enable local video downloading via yt-dlp.",
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Output directory for generated files (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--version", "-v",
        action="version",
        version=f"%(prog)s {SCRIPT_VERSION} ({SCRIPT_CODENAME})",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        default=False,
        help="Reduce console output to warnings and errors only.",
    )

    return parser


def run_interactive_mode() -> Tuple[str, bool, int, bool, str]:
    """
    Run the interactive CLI prompts and return user choices.

    Returns:
        Tuple of (target, scan_all, max_videos, download_enabled, output_dir)
    """
    print("-" * 55)
    print("  Interactive Mode")
    print("-" * 55)
    print()

    # Target input
    while True:
        target = input(
            "  Enter YouTube channel URL, handle (@username), "
            "or Channel ID:\n  > "
        ).strip()
        if target:
            break
        print("  [!] Target cannot be empty. Please try again.\n")

    # Scan all?
    print()
    scan_all_choice = input(
        "  Scan ALL videos in channel uploads? (y/N): "
    ).strip().lower()
    scan_all = scan_all_choice in ["y", "yes"]

    # Max videos
    max_videos = DEFAULT_VIDEOS_TO_SCAN
    if not scan_all:
        custom_limit = input(
            f"  Enter max videos to scan (default {DEFAULT_VIDEOS_TO_SCAN}): "
        ).strip()
        if custom_limit.isdigit() and int(custom_limit) > 0:
            max_videos = int(custom_limit)
        elif custom_limit:
            print(f"  [!] Invalid number. Using default: {DEFAULT_VIDEOS_TO_SCAN}")

    # Download?
    print()
    dl_choice = input(
        "  Enable local video downloading via yt-dlp? (y/N): "
    ).strip().lower()
    download_enabled = dl_choice in ["y", "yes"]
    if download_enabled and not YT_DLP_AVAILABLE:
        print("  [!] yt-dlp is not installed. Downloads will be skipped.")
        print("  [!] Install with: pip install yt-dlp")

    # Output directory
    print()
    output_dir_input = input(
        f"  Output directory (default: {DEFAULT_OUTPUT_DIR}): "
    ).strip()
    output_dir = output_dir_input if output_dir_input else str(DEFAULT_OUTPUT_DIR)

    print()
    print("-" * 55)
    print(f"  Target       : {target}")
    print(f"  Scan All     : {scan_all}")
    print(f"  Max Videos   : {'ALL' if scan_all else max_videos}")
    print(f"  Downloads    : {'Enabled' if download_enabled else 'Disabled'}")
    print(f"  Output Dir   : {output_dir}")
    print("-" * 55)
    print()

    confirm = input("  Proceed with these settings? (Y/n): ").strip().lower()
    if confirm in ["n", "no"]:
        print("  [!] Aborted by user.")
        sys.exit(EXIT_SUCCESS)

    print()
    return target, scan_all, max_videos, download_enabled, output_dir


def main():
    """Main entry point — handles both CLI and interactive modes."""
    print_banner()

    # Parse CLI arguments
    parser = build_argument_parser()
    args = parser.parse_args()

    # Set log level
    if args.quiet:
        logging.getLogger("YouTubeOSINT").setLevel(logging.WARNING)

    # Validate configuration
    config = ConfigManager()
    if not config.validate():
        sys.exit(EXIT_CONFIG_ERROR)

    # Determine mode: CLI or Interactive
    if args.target:
        # CLI mode
        target = args.target
        scan_all = args.scan_all
        max_videos = args.max_videos
        download_enabled = args.download
        output_dir = args.output_dir
    else:
        # Interactive mode
        target, scan_all, max_videos, download_enabled, output_dir = (
            run_interactive_mode()
        )

    # Run pipeline
    orchestrator = PipelineOrchestrator(
        config, output_dir=Path(output_dir)
    )
    try:
        orchestrator.run(
            target,
            scan_all=scan_all,
            max_videos=max_videos,
            download_enabled=download_enabled,
        )
    except KeyboardInterrupt:
        print("\n\n  [!] Pipeline interrupted by user (Ctrl+C).")
        orchestrator.stats.stop()
        orchestrator.stats.record_error("Pipeline", "Interrupted by user")
        logger.info(
            f"  Runtime before interrupt: {orchestrator.stats.elapsed_display}"
        )
        sys.exit(EXIT_SUCCESS)
    except SystemExit:
        raise
    except Exception as e:
        logger.critical(
            f"[Pipeline] Fatal error: {e}", exc_info=True
        )
        sys.exit(EXIT_GENERAL_ERROR)


if __name__ == "__main__":
    main()
