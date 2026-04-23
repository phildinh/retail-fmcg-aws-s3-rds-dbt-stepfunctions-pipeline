import sys
import os
import importlib.util

# `lambda` is a Python keyword — dotted import won't parse.
# Use importlib to load handler.py directly from its file path.
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

_spec = importlib.util.spec_from_file_location(
    "handler",
    os.path.join(_root, "lambda", "lambda_1_generator", "handler.py")
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
handler = _mod.handler


def test_lambda_1_local():
    """
    Test Lambda 1 handler locally before deploying to AWS.
    Simulates a Step Functions event payload.
    """
    event = {
        "run_date": "2026-04-18",
        "run_timestamp": "2026-04-18 06:00:00"
    }

    result = handler(event, context=None)

    assert result["status"] == "success"
    assert result["run_date"] == "2026-04-18"
    assert "s3_uris" in result
    assert len(result["s3_uris"]) == 4
    print("\nAll assertions passed!")
    print(f"S3 URIs: {result['s3_uris']}")


if __name__ == "__main__":
    test_lambda_1_local()
