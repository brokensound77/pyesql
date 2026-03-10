import sys
import tomllib
from pathlib import Path

from pyesql import parse


def load_elastic_rules(repo_path: Path, esql_only: bool = True) -> list[dict]:
    rules = [tomllib.loads(r.read_text()) for r in repo_path.rglob("rules/**/*.toml")]
    bb_rules = repo_path.rglob("rules_building_block/**/*.toml")
    rules += [tomllib.loads(r.read_text()) for r in bb_rules]
    if esql_only:
        return [r for r in rules if r["rule"].get("language", None) == "esql"]
    return rules


def load_elastic_hunts(repo_path: Path, esql_only: bool = True) -> list[dict]:
    rules = [tomllib.loads(r.read_text()) for r in repo_path.rglob("hunting/**/*.toml")]
    if esql_only:
        return [r for r in rules if r["hunt"].get("language", None) == "ES|QL"]
    return rules


if __name__ == "__main__":
    repo = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("detection-rules")

    loaded_rules = load_elastic_rules(repo)
    loaded_hunts = load_elastic_hunts(repo)

    errors = []

    for rule in loaded_rules:
        name = rule["rule"]["name"]
        query = rule["rule"]["query"]
        try:
            parse(query)
        except Exception as e:
            errors.append(f"[rule] {name}: {e}")

    for hunt in loaded_hunts:
        name = hunt["hunt"]["name"]
        for query in hunt["hunt"]["query"]:
            try:
                parse(query)
            except Exception as e:
                errors.append(f"[hunt] {name}: {e}")

    total = len(loaded_rules) + sum(len(h["hunt"]["query"]) for h in loaded_hunts)
    print(
        f"\nParsed {total} queries ({len(loaded_rules)} rules, "
        f"{sum(len(h['hunt']['query']) for h in loaded_hunts)} hunts)"
    )

    if errors:
        print(f"\n{len(errors)} error(s):\n")
        for err in errors:
            print(f"  {err}")
        sys.exit(1)
    else:
        print("All queries parsed successfully.")
