"""Width policy for normalized convenience text, never raw evidence or identities."""

from __future__ import annotations


def bounded_normalized_text(value: str | None, max_length: int) -> str | None:
    """Keep a deterministic Unicode code-point prefix fitting VARCHAR(max_length).

    None, empty strings, whitespace and ordinary values are unchanged. Do not
    use this for addresses, provenance locators, raw payloads or digest inputs.
    Full values must remain recoverable through the row's source evidence.
    """
    if max_length < 1:
        raise ValueError("max_length must be positive")
    return value[:max_length] if value is not None else None
