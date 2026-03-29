"""
scrapers/video_downloader.py — Download Meta CDN videos, transcribe with
faster-whisper, and extract frames with ffmpeg.

This is the only Playwright component remaining after migration to Apify.
Playwright is used *solely* as an authenticated HTTP client for CDN URLs
that reject plain httpx requests — no page navigation or DOM parsing.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Optional

import httpx

from config import RAW_DIR, SCRAPER_USER_AGENTS
from scrapers.utils import random_user_agent

logger = logging.getLogger(__name__)

_MAX_FILE_BYTES = 10 * 1024 * 1024  # 10 MB cap


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def process_video(
    video_url: str,
    ad_library_id: str,
    brand_slug: str,
) -> dict:
    """
    Download *video_url*, transcribe speech, extract key frames.

    Parameters
    ----------
    video_url : str
        Meta CDN URL for the ad video.
    ad_library_id : str
        Unique ad identifier (used as subdirectory name).
    brand_slug : str
        Already-slugified brand name (caller runs ``safe_brand_slug()``).

    Returns
    -------
    dict
        ``{"transcript", "transcript_language", "frames_path", "image_path"}``
        — all ``None`` on any failure.
    """
    empty = {
        "transcript": None,
        "transcript_language": None,
        "frames_path": None,
        "image_path": None,
    }

    if not video_url:
        logger.warning("No video_url provided — skipping")
        return empty

    ad_dir = RAW_DIR / brand_slug / ad_library_id
    video_path = ad_dir / "video.mp4"
    frames_dir = ad_dir / "frames"

    # ── Download ──────────────────────────────────────────────────────────
    downloaded = _download_direct(video_url, video_path)
    if not downloaded:
        downloaded = _download_via_playwright(video_url, video_path)
    if not downloaded:
        logger.warning("Both download methods failed for ad %s", ad_library_id)
        return empty

    # ── Transcribe ────────────────────────────────────────────────────────
    transcript, language = _transcribe_video(video_path)

    # ── Extract frames ────────────────────────────────────────────────────
    frames_ok = _extract_frames(video_path, frames_dir)

    # ── Clean up video file ───────────────────────────────────────────────
    try:
        video_path.unlink(missing_ok=True)
        logger.debug("Deleted video file %s", video_path)
    except OSError as exc:
        logger.warning("Could not delete video file: %s", exc)

    # ── Build result ──────────────────────────────────────────────────────
    first_frame = frames_dir / "frame_0.0s.jpg"
    image_path = str(first_frame) if first_frame.exists() else None

    return {
        "transcript": transcript,
        "transcript_language": language,
        "frames_path": str(frames_dir) if frames_ok else None,
        "image_path": image_path,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Download — direct httpx
# ══════════════════════════════════════════════════════════════════════════════

def _download_direct(url: str, dest: Path) -> bool:
    """
    Attempt a plain httpx download.  Works ~30% of the time when the
    CDN URL has not expired.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        r = httpx.get(
            url,
            timeout=30,
            follow_redirects=True,
            headers={"User-Agent": random_user_agent()},
        )
        content_type = r.headers.get("content-type", "")
        if r.status_code == 200 and content_type.startswith("video/"):
            if len(r.content) > _MAX_FILE_BYTES:
                logger.warning("Direct download exceeds 10MB limit, skipping: %s", url[:80])
                return False
            dest.write_bytes(r.content)
            logger.info("Direct httpx download OK (%d bytes) → %s", len(r.content), dest)
            return True

        logger.debug(
            "Direct download returned HTTP %d, content-type=%s — will try Playwright",
            r.status_code, content_type,
        )
        return False

    except Exception as exc:
        logger.debug("Direct httpx download failed: %s — will try Playwright", exc)
        return False


# ══════════════════════════════════════════════════════════════════════════════
# Download — Playwright authenticated request
# ══════════════════════════════════════════════════════════════════════════════

def _download_via_playwright(url: str, dest: Path) -> bool:
    """
    Download using Playwright's API request context (preserves session cookies).
    Meta CDN URLs often require browser session auth — regular HTTP returns 403.

    No page navigation or DOM parsing — purely an authenticated HTTP GET.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("Playwright not installed — cannot attempt authenticated download")
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=random_user_agent(),
            )
            try:
                response = ctx.request.get(url, timeout=60_000)
                if response.ok:
                    content = response.body()
                    if len(content) > _MAX_FILE_BYTES:
                        logger.warning(
                            "Playwright download exceeds 10MB limit, skipping: %s",
                            url[:80],
                        )
                        return False
                    dest.write_bytes(content)
                    logger.info(
                        "Playwright download OK (%d bytes) → %s",
                        len(content), dest,
                    )
                    return True
                else:
                    logger.warning(
                        "Playwright download failed (HTTP %d): %s",
                        response.status, url[:80],
                    )
                    return False
            finally:
                ctx.close()
                browser.close()

    except Exception as exc:
        logger.warning("Playwright download error: %s — %s", exc, url[:80])
        return False


# ══════════════════════════════════════════════════════════════════════════════
# Whisper transcription
# ══════════════════════════════════════════════════════════════════════════════

def _transcribe_video(video_path: Path) -> tuple[Optional[str], Optional[str]]:
    """Transcribe speech from video using faster-whisper. Auto-detects language.

    Returns (transcript_text, detected_language) — e.g. ("hello world", "en").
    """
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        logger.warning("faster-whisper not installed — skipping transcription")
        return None, None

    if not video_path.exists():
        return None, None

    try:
        logger.info("Transcribing %s with faster-whisper...", video_path.name)
        model = WhisperModel("base", device="cpu", compute_type="int8")
        segments, info = model.transcribe(str(video_path), beam_size=5)

        detected_lang = info.language
        logger.info("Detected language: %s (prob=%.2f)", detected_lang, info.language_probability)

        transcript_parts = []
        for segment in segments:
            transcript_parts.append(segment.text.strip())

        transcript = " ".join(transcript_parts)
        logger.info("Transcript (%d chars): %s", len(transcript), transcript[:200])
        return (transcript if transcript else None, detected_lang)

    except Exception as exc:
        logger.warning("Transcription failed: %s", exc)
        return None, None


# ══════════════════════════════════════════════════════════════════════════════
# Frame extraction (ffmpeg)
# ══════════════════════════════════════════════════════════════════════════════

def _extract_frames(video_path: Path, frames_dir: Path) -> bool:
    """Extract 7 frames from the video: 0s, 0.5s, 1s, 1.5s, 2s, 3s, and midpoint."""
    if not video_path.exists():
        return False

    # Get video duration for midpoint calculation
    duration = _get_video_duration(video_path)
    midpoint = duration / 2 if duration else 5.0

    timestamps = [0, 0.5, 1.0, 1.5, 2.0, 3.0, midpoint]

    frames_dir.mkdir(parents=True, exist_ok=True)
    extracted = 0

    for ts in timestamps:
        # Skip timestamps beyond video duration
        if duration and ts > duration:
            continue

        frame_name = f"frame_{ts:.1f}s.jpg"
        frame_path = frames_dir / frame_name

        try:
            result = subprocess.run(
                [
                    "ffmpeg", "-y",
                    "-ss", f"{ts:.2f}",
                    "-i", str(video_path),
                    "-frames:v", "1",
                    "-q:v", "2",
                    str(frame_path),
                ],
                capture_output=True,
                timeout=30,
            )
            if frame_path.exists() and frame_path.stat().st_size > 0:
                extracted += 1
            else:
                logger.debug("Frame at %.1fs: ffmpeg produced no output", ts)
        except FileNotFoundError:
            logger.warning("ffmpeg not found in PATH — cannot extract frames. "
                           "Install ffmpeg and add to PATH.")
            return False
        except subprocess.TimeoutExpired:
            logger.warning("ffmpeg timeout extracting frame at %.1fs", ts)
        except Exception as exc:
            logger.warning("Frame extraction error at %.1fs: %s", ts, exc)

    logger.info("Extracted %d/%d frames → %s", extracted, len(timestamps), frames_dir)
    return extracted > 0


def _get_video_duration(video_path: Path) -> Optional[float]:
    """Get video duration in seconds using ffprobe."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(video_path),
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _cli() -> None:
    import argparse

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(name)s -- %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        prog="python -m scrapers.video_downloader",
        description="Download a Meta CDN video, transcribe, and extract frames.",
    )
    parser.add_argument("--url", required=True, help="Meta CDN video URL")
    parser.add_argument("--ad-id", required=True, help="Ad library ID (used as directory name)")
    parser.add_argument("--brand-slug", required=True,
                        help="Slugified brand name (output of safe_brand_slug)")
    args = parser.parse_args()

    result = process_video(
        video_url=args.url,
        ad_library_id=args.ad_id,
        brand_slug=args.brand_slug,
    )

    logger.info("Result: %s", result)


if __name__ == "__main__":
    _cli()
