"""Print a bcrypt hash for moderators.json (pip install bcrypt)."""

from __future__ import annotations

import argparse
import getpass

import bcrypt


def main() -> None:
    p = argparse.ArgumentParser(description="bcrypt hash for data/moderators.json")
    p.add_argument(
        "-p",
        "--password",
        default=None,
        help="Plain password (omit to prompt securely)",
    )
    args = p.parse_args()
    plain = args.password
    if plain is None:
        plain = getpass.getpass("Password: ")
    h = bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode()
    print(h)


if __name__ == "__main__":
    main()
