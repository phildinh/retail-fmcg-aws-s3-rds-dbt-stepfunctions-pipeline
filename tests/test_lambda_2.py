import sys
import os
import importlib.util

sys.path.insert(0, os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
))


def load_handler(relative_path):
    abs_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        relative_path
    )
    spec   = importlib.util.spec_from_file_location(
                 "handler", abs_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_lambda_2_local():
    """
    Test Lambda 2 locally using the output from Lambda 1 test.
    Make sure you ran test_lambda_1.py first so S3 has files.
    """
    handler_module = load_handler(
        "lambda/lambda_2_staging/handler.py"
    )

    event = {
        "run_date":      "2026-04-18",
        "run_timestamp": "2026-04-18 06:00:00",
        "s3_uris": {
            "products":  "s3://retail-fmcg-raw-data/products/...",
            "stores":    "s3://retail-fmcg-raw-data/stores/...",
            "customers": "s3://retail-fmcg-raw-data/customers/...",
            "fact_sales": "s3://retail-fmcg-raw-data/fact_sales/..."
        }
    }

    result = handler_module.handler(event, context=None)

    assert result["status"] == "success"
    assert result["rows_loaded"] > 0
    assert "run_id" in result
    print("\nAll assertions passed!")
    print(f"Rows loaded: {result['rows_loaded']}")
    print(f"Run ID: {result['run_id']}")


if __name__ == "__main__":
    test_lambda_2_local()
