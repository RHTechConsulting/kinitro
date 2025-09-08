import os
from typing import Optional

import dotenv

from core.config import Config, ConfigOpts
from core.constants import NeuronType
from core.storage import R2Config

dotenv.load_dotenv()


class EvaluatorConfig(Config):
    def __init__(self):
        opts = ConfigOpts(
            neuron_type=NeuronType.Validator,
            neuron_name="evaluator",
            settings_files=["evaluator.toml"],
        )
        super().__init__(opts)
        self.pg_database = self.settings.get("pg_database")  # type: ignore
        self.duck_db = self.settings.get("duck_db")  # type: ignore

        # R2 storage configuration
        self.r2_config = self._load_r2_config()

        # Episode logging configuration
        self.episode_log_interval = self.settings.get("episode_log_interval", 1)
        self.step_log_interval = self.settings.get("step_log_interval", 1)

    def _load_r2_config(self) -> Optional[R2Config]:
        """Load R2 configuration from environment variables and settings."""
        # Try environment variables first
        endpoint_url = os.environ.get("R2_ENDPOINT_URL") or self.settings.get(
            "r2_endpoint_url"
        )
        access_key_id = os.environ.get("R2_ACCESS_KEY_ID") or self.settings.get(
            "r2_access_key_id"
        )
        secret_access_key = os.environ.get("R2_SECRET_ACCESS_KEY") or self.settings.get(
            "r2_secret_access_key"
        )
        bucket_name = os.environ.get("R2_BUCKET_NAME") or self.settings.get(
            "r2_bucket_name"
        )
        region = os.environ.get("R2_REGION", "auto") or self.settings.get(
            "r2_region", "auto"
        )
        public_url_base = os.environ.get("R2_PUBLIC_URL_BASE") or self.settings.get(
            "r2_public_url_base"
        )

        # Check if all required fields are present
        if not all([endpoint_url, access_key_id, secret_access_key, bucket_name]):
            return None

        return R2Config(
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            bucket_name=bucket_name,
            region=region,
            public_url_base=public_url_base,
        )

    def add_args(self):
        """Add command line arguments"""
        super().add_args()
        # pg database
        self._parser.add_argument(
            "--pg-database",
            type=str,
            help="PostgreSQL database URL",
            default=self.settings.get(
                "pg_database", "postgresql://user:password@localhost/dbname"
            ),  # type: ignore
        )
