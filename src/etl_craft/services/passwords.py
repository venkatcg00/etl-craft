"""Fixed-cost scrypt password verification without retaining cleartext passwords."""

import hashlib
import hmac
import secrets

from etl_craft.core.errors import UsageError


def hash_password(password: str) -> str:
    """Store a random salt and a bounded, versioned scrypt digest."""
    if not 12 <= len(password) <= 1024:
        raise UsageError("password length must be 12 to 1024 characters; choose a longer password")
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=16384, r=8, p=5)
    return f"scrypt$16384$8$5${salt.hex()}${digest.hex()}"


def verify_password(password: str, encoded: str | None) -> bool:
    """Refuse malformed or unsupported hashes without trusting stored cost parameters."""
    if not encoded or not 1 <= len(password) <= 1024:
        return False
    pieces = encoded.split("$")
    if len(pieces) != 6 or pieces[:4] != ["scrypt", "16384", "8", "5"]:
        return False
    try:
        salt, expected = bytes.fromhex(pieces[4]), bytes.fromhex(pieces[5])
    except ValueError:
        return False
    if len(salt) != 16 or len(expected) != 64:
        return False
    actual = hashlib.scrypt(password.encode(), salt=salt, n=16384, r=8, p=5)
    return hmac.compare_digest(actual, expected)
