"""Apply the same money-history predicate to the pinned production handler.

The live handler has deployed batching/FOF fixes absent from main. This guarded
build-time patch changes only its money verifier, leaving every other AST node
identical. Never use it with an unreviewed or mutable base image.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
from pathlib import Path


OLD_MASK = """            mask = [
                (code, day) in ours and not pd.isna(nav)
                and abs(float(nav) - float(ours[(code, day)]))
                    > MONEY_NAV_TOLERANCE
                for day, nav in zip(df.trade_date, df.unit_nav)
            ]"""
NEW_MASK = """            if not df.trade_date.between(window_start, target).all():
                raise ValueError("Upstream returned dates outside the verified window")
            mask = [
                not pd.isna(nav) and (
                    (code, day) not in ours
                    or abs(float(nav) - float(ours[(code, day)]))
                        > MONEY_NAV_TOLERANCE
                )
                for day, nav in zip(df.trade_date, df.unit_nav)
            ]"""


def patch_source(source: str, expected_sha: str) -> str:
    if hashlib.sha256(source.encode()).hexdigest() != expected_sha:
        raise ValueError("Production handler SHA differs from the reviewed baseline")
    tree = ast.parse(source)
    functions = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                 and n.name == "_verify_money_funds"]
    if len(functions) != 1:
        raise ValueError("Expected exactly one money verifier")
    function = functions[0]
    lines = source.splitlines(keepends=True)
    original = "".join(lines[function.lineno - 1:function.end_lineno])
    updated = original
    replacements = [
        (OLD_MASK, NEW_MASK),
        ("checked = corrected = errors = 0", "checked = corrected = errors = added = 0"),
        ("            corrected += len(merged)",
         "            new_rows = sum((r.fund_code, r.trade_date) not in ours\n"
         "                           for r in merged.itertuples())\n"
         "            added += new_rows\n"
         "            corrected += len(merged) - new_rows"),
        ('f"{corrected} corrected, {errors} errors"',
         'f"{added} added, {corrected} corrected, {errors} errors"'),
        ('"corrected": corrected, "codes": codes,',
         '"corrected": corrected, "added": added, "codes": codes,'),
    ]
    for old, new in replacements:
        if updated.count(old) != 1:
            raise ValueError(f"Production verifier layout changed: {old[:70]}")
        updated = updated.replace(old, new, 1)
    lines[function.lineno - 1:function.end_lineno] = [updated]
    result = "".join(lines)
    patched_tree = ast.parse(result)
    before = [ast.dump(n) for n in tree.body if n is not function]
    after = [ast.dump(n) for n in patched_tree.body
             if not (isinstance(n, ast.FunctionDef) and n.name == function.name)]
    if before != after:
        raise ValueError("Patch changed code outside the money verifier")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("handler", type=Path)
    parser.add_argument("--expected-sha", required=True)
    args = parser.parse_args()
    original = args.handler.read_text()
    result = patch_source(original, args.expected_sha)
    args.handler.write_text(result)
    print("Verified: only _verify_money_funds changed")


if __name__ == "__main__":
    main()
