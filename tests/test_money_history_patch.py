"""The image overlay must fail closed on drift and preserve other live fixes."""
import ast
import hashlib
import importlib.util
from pathlib import Path

import pytest


path = Path(__file__).parents[1] / "lambda/fund-fallback-fetcher/patch_money_history.py"
spec = importlib.util.spec_from_file_location("money_history_patch", path)
patch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(patch)

SOURCE = '''def _fill_gaps():
    return "existing FOF/budget/batch behavior"

def _verify_money_funds():
    checked = corrected = errors = 0
    for results in batches:
        for code, df, err in results:
''' + patch.OLD_MASK + '''
        if frames:
            _commit(writer, merged, target)
            corrected += len(merged)
            for row in merged.itertuples():
                ours[(row.fund_code, row.trade_date)] = row.unit_nav
        logger.info(f"{corrected} corrected, {errors} errors")
    return {
        "corrected": corrected, "codes": codes,
    }

def lambda_handler(event, context):
    return "existing success accounting"
'''


def digest(source):
    return hashlib.sha256(source.encode()).hexdigest()


def test_only_money_verifier_ast_changes():
    result = patch.patch_source(SOURCE, digest(SOURCE))
    before = ast.parse(SOURCE).body
    after = ast.parse(result).body
    assert ast.dump(before[0]) == ast.dump(after[0])
    assert ast.dump(before[2]) == ast.dump(after[2])
    assert ast.dump(before[1]) != ast.dump(after[1])
    assert '"added": added' in result
    assert "(code, day) not in ours" in result


def test_wrong_handler_digest_is_rejected():
    with pytest.raises(ValueError, match="SHA"):
        patch.patch_source(SOURCE, "0" * 64)


def test_unexpected_mask_layout_is_rejected():
    changed = SOURCE.replace("(code, day) in ours", "(code, day) in inventory")
    with pytest.raises(ValueError, match="layout"):
        patch.patch_source(changed, digest(changed))


def test_patch_cannot_be_applied_twice():
    result = patch.patch_source(SOURCE, digest(SOURCE))
    with pytest.raises(ValueError, match="layout"):
        patch.patch_source(result, digest(result))
