"""Explicit checks for JSON strings PostgreSQL cannot represent losslessly.

This is a provider-boundary predicate, not a database serializer or sanitizer.
Check object keys as well as values, including unknown nested provider extras.
"""

from __future__ import annotations


def postgres_json_string_failure(payload: object) -> str | None:
    """Return a fixed diagnostic code for NUL or unpaired UTF-16 surrogates.

    Inputs are decoded JSON trees. Iteration avoids adding a recursive walk to
    the parser. Other schema/type/numeric errors are deliberately not handled.
    """
    pending = [payload]
    surrogate_found = False
    while pending:
        value = pending.pop()
        if isinstance(value, str):
            if "\x00" in value:
                return "postgresql_nul_string"
            if any("\ud800" <= character <= "\udfff" for character in value):
                surrogate_found = True
        elif isinstance(value, dict):
            pending.extend(value.keys())
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)
    return "postgresql_unpaired_surrogate" if surrogate_found else None
