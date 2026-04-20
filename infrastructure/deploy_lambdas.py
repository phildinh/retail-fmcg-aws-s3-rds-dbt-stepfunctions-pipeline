import boto3
import zipfile
import os
import sys
sys.path.insert(0, os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
))
from utils.config import get_config


def create_zip(source_dirs: list, zip_path: str):
    """
    Package Lambda function code and dependencies into a zip file.
    Includes the handler, utils, and data_generator folders.
    """
    with zipfile.ZipFile(zip_path, 'w',
                         zipfile.ZIP_DEFLATED) as zf:
        for source_dir, prefix in source_dirs:
            for root, dirs, files in os.walk(source_dir):
                # Skip cache and output folders
                dirs[:] = [d for d in dirs
                           if d not in ['__pycache__',
                                        'output', '.git']]
                for file in files:
                    if file.endswith('.pyc'):
                        continue
                    filepath = os.path.join(root, file)
                    arcname  = os.path.join(
                        prefix,
                        os.path.relpath(filepath, source_dir)
                    )
                    zf.write(filepath, arcname)
    print(f"  Created zip: {zip_path}")


def deploy_lambda(function_name: str,
                  zip_path: str,
                  handler: str,
                  role_arn: str,
                  env_vars: dict,
                  config: dict):
    """
    Create or update a Lambda function in AWS.
    If function exists — update code and config.
    If not — create it fresh.
    """
    client = boto3.client(
        "lambda",
        region_name=config["aws_region"]
    )

    with open(zip_path, "rb") as f:
        zip_bytes = f.read()

    try:
        # Try update first
        client.update_function_code(
            FunctionName = function_name,
            ZipFile      = zip_bytes
        )
        client.update_function_configuration(
            FunctionName  = function_name,
            Handler       = handler,
            Environment   = {"Variables": env_vars},
            Timeout       = 300,
            MemorySize    = 256
        )
        print(f"  Updated: {function_name}")

    except client.exceptions.ResourceNotFoundException:
        # Create if doesn't exist
        client.create_function(
            FunctionName  = function_name,
            Runtime       = "python3.11",
            Role          = role_arn,
            Handler       = handler,
            Code          = {"ZipFile": zip_bytes},
            Environment   = {"Variables": env_vars},
            Timeout       = 300,
            MemorySize    = 256,
            Tags          = {
                "Project":     "retail-fmcg-aws-pipeline",
                "Environment": "dev",
                "Owner":       "phil",
                "Purpose":     "portfolio"
            }
        )
        print(f"  Created: {function_name}")


def main():
    config = get_config()

    # Get Lambda role ARN
    iam    = boto3.client("iam")
    role   = iam.get_role(RoleName="retail-fmcg-lambda-role")
    role_arn = role["Role"]["Arn"]
    print(f"Lambda role ARN: {role_arn}")

    # Environment variables passed to both Lambdas
    env_vars = {
        "AWS_REGION":      config["aws_region"],
        "S3_BUCKET":       config["s3_bucket"],
        "DB_HOST":         config["db_host"],
        "DB_NAME":         config["db_name"],
        "DB_USER":         config["db_user"],
        "DB_PASSWORD":     config["db_password"],
        "DB_PORT":         str(config["db_port"]),
        "SNS_TOPIC_ARN":   config["sns_topic_arn"],
        "NUM_TRANSACTIONS": str(config["num_transactions"]),
        "OUTPUT_DIR":      "/tmp/output",
        "PIPELINE_ENV":    config["pipeline_env"]
    }

    os.makedirs("infrastructure/packages", exist_ok=True)

    # ── Deploy Lambda 1 ───────────────────────────────────
    print("\nPackaging Lambda 1...")
    create_zip(
        source_dirs=[
            ("lambda/lambda_1_generator", ""),
            ("data_generator", "data_generator"),
            ("utils", "utils")
        ],
        zip_path="infrastructure/packages/lambda_1.zip"
    )
    print("Deploying Lambda 1...")
    deploy_lambda(
        function_name = "retail-fmcg-lambda-1-generator",
        zip_path      = "infrastructure/packages/lambda_1.zip",
        handler       = "handler.handler",
        role_arn      = role_arn,
        env_vars      = env_vars,
        config        = config
    )

    # ── Deploy Lambda 2 ───────────────────────────────────
    print("\nPackaging Lambda 2...")
    create_zip(
        source_dirs=[
            ("lambda/lambda_2_staging", ""),
            ("data_generator", "data_generator"),
            ("utils", "utils")
        ],
        zip_path="infrastructure/packages/lambda_2.zip"
    )
    print("Deploying Lambda 2...")
    deploy_lambda(
        function_name = "retail-fmcg-lambda-2-staging",
        zip_path      = "infrastructure/packages/lambda_2.zip",
        handler       = "handler.handler",
        role_arn      = role_arn,
        env_vars      = env_vars,
        config        = config
    )

    print("\nBoth Lambdas deployed successfully!")
    print("Check AWS Console -> Lambda to verify.")


if __name__ == "__main__":
    main()