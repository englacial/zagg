"""``python -m zagg.config --schema [--block <name>]``: print the template grammar's JSON schema."""

import argparse
import sys

from zagg.config.schema import BLOCKS, schema_json


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m zagg.config",
        description="Export the zagg template grammar (issue #677) as a JSON schema.",
    )
    parser.add_argument("--schema", action="store_true", help="print the JSON schema and exit")
    parser.add_argument(
        "--block",
        choices=sorted(BLOCKS),
        default=None,
        help="export one block instead of the whole config",
    )
    parser.add_argument(
        "--no-registry-hints",
        action="store_true",
        help="omit the registered capability names from the capability-named fields",
    )
    args = parser.parse_args(argv)
    if not args.schema:
        parser.error("nothing to do: pass --schema")
    sys.stdout.write(schema_json(args.block, registry_hints=not args.no_registry_hints) + "\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
