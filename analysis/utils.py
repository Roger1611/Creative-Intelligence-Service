"""
analysis/utils.py — Shared utilities for analysis modules.
"""

from __future__ import annotations

import re


def classify_hook_structure(hook: str) -> str:
    """Classify a hook into a structural category."""
    hook_lower = hook.lower().strip()

    if hook_lower.endswith("?"):
        return "question"
    if re.match(r"^\d", hook_lower):
        return "number_lead"
    if any(hook_lower.startswith(w) for w in
           ("stop", "wait", "don't", "never", "warning")):
        return "pattern_interrupt"
    if any(w in hook_lower for w in ("you ", "your ", "you're")):
        return "direct_address"
    if any(w in hook_lower for w in
           ("secret", "nobody", "hidden", "truth", "real reason")):
        return "curiosity_gap"
    if any(w in hook_lower for w in
           ("before", "after", "transformation", "results", "changed")):
        return "transformation"
    if any(w in hook_lower for w in
           ("review", "testimonial", "said", "told me")):
        return "social_proof_lead"
    if any(w in hook_lower for w in
           ("last chance", "limited", "hurry", "only", "ending")):
        return "urgency_lead"
    if any(w in hook_lower for w in
           ("doctor", "expert", "dermatologist", "study", "research")):
        return "authority_lead"
    return "bold_claim"
