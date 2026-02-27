import os
from dataclasses import dataclass


@dataclass
class Config:
    """Configuration for fund data fetch Lambda."""

    s3_bucket: str = os.environ.get("S3_BUCKET", "")
    log_level: str = os.environ.get("LOG_LEVEL", "INFO")
    max_retries: int = int(os.environ.get("MAX_RETRIES", "3"))
    retry_delay: float = float(os.environ.get("RETRY_DELAY", "1.0"))

    @classmethod
    def from_env(cls) -> "Config":
        """Create Config instance from environment variables."""
        return cls(
            s3_bucket=os.environ.get("S3_BUCKET", ""),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
            max_retries=int(os.environ.get("MAX_RETRIES", "3")),
            retry_delay=float(os.environ.get("RETRY_DELAY", "1.0")),
        )

    def validate(self) -> None:
        """Validate configuration."""
        if not self.s3_bucket:
            raise ValueError("S3_BUCKET environment variable is required")
