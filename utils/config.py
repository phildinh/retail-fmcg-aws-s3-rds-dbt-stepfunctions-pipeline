import os
from dotenv import load_dotenv

load_dotenv()


def get_config() -> dict:
    """
    Load all environment variables in one place.
    Every Lambda and script imports from here — no scattered
    os.getenv() calls across the codebase.
    Raises clearly if a required variable is missing.
    """
    required = [
        "AWS_REGION",
        "S3_BUCKET",
        "DB_HOST",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
        "DB_PORT",
        "SNS_TOPIC_ARN"
    ]

    config = {
        # AWS
        "aws_region":     os.getenv("AWS_REGION", "ap-southeast-2"),
        "s3_bucket":      os.getenv("S3_BUCKET"),

        # RDS
        "db_host":        os.getenv("DB_HOST"),
        "db_name":        os.getenv("DB_NAME", "fmcg_db"),
        "db_user":        os.getenv("DB_USER"),
        "db_password":    os.getenv("DB_PASSWORD"),
        "db_port":        int(os.getenv("DB_PORT", 5432)),

        # SNS
        "sns_topic_arn":  os.getenv("SNS_TOPIC_ARN"),

        # Pipeline
        "pipeline_env":   os.getenv("PIPELINE_ENV", "dev"),
        "num_transactions": int(os.getenv("NUM_TRANSACTIONS", 500)),
        "output_dir":     os.getenv("OUTPUT_DIR",
                                    "data_generator/output")
    }

    # Fail fast if any required variable is missing
    missing = [k for k in required
               if not os.getenv(k)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}\n"
            f"Check your .env file or Lambda environment settings."
        )

    return config