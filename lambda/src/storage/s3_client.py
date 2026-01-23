import io
from datetime import datetime
from typing import Optional
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from utils.logger import get_logger
from utils.retry import retry_with_backoff


class S3Client:
    """Client for uploading data to S3 in Parquet format."""

    def __init__(self, bucket_name: str) -> None:
        """Initialize S3 client.

        Args:
            bucket_name: Name of the S3 bucket to upload to
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3")
        self.logger = get_logger(__name__)

    def _get_s3_key(
        self, category: str, data_name: str, date: Optional[datetime] = None
    ) -> str:
        """Generate S3 key for data file.

        Args:
            category: Data category (fund, stock, macro)
            data_name: Name of the data file
            date: Date for partitioning (defaults to today)

        Returns:
            S3 key in format: {category}/{YYYY-MM-DD}/{data_name}.parquet
        """
        if date is None:
            date = datetime.now()

        date_str = date.strftime("%Y-%m-%d")
        return f"{category}/{date_str}/{data_name}.parquet"

    @retry_with_backoff(max_retries=3, initial_delay=1.0)
    def upload_dataframe(
        self,
        df: pd.DataFrame,
        category: str,
        data_name: str,
        date: Optional[datetime] = None,
    ) -> dict:
        """Upload a DataFrame to S3 as Parquet.

        Args:
            df: DataFrame to upload
            category: Data category (fund, stock, macro)
            data_name: Name of the data file
            date: Date for partitioning (defaults to today)

        Returns:
            Dict with upload details (bucket, key, size)
        """
        if df.empty:
            self.logger.warning(f"Empty DataFrame for {category}/{data_name}, skipping upload")
            return {"bucket": self.bucket_name, "key": None, "size": 0, "skipped": True}

        s3_key = self._get_s3_key(category, data_name, date)

        # Convert DataFrame to Parquet bytes
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
        parquet_buffer.seek(0)

        # Get file size
        file_size = parquet_buffer.getbuffer().nbytes

        self.logger.info(f"Uploading {s3_key} ({file_size:,} bytes, {len(df)} rows)")

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType="application/x-parquet",
                Metadata={
                    "row_count": str(len(df)),
                    "column_count": str(len(df.columns)),
                    "created_at": datetime.now().isoformat(),
                },
            )

            self.logger.info(f"Successfully uploaded {s3_key}")
            return {
                "bucket": self.bucket_name,
                "key": s3_key,
                "size": file_size,
                "rows": len(df),
                "skipped": False,
            }

        except ClientError as e:
            self.logger.error(f"Failed to upload {s3_key}: {e}")
            raise

    def check_bucket_exists(self) -> bool:
        """Check if the S3 bucket exists and is accessible.

        Returns:
            True if bucket exists and is accessible
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                self.logger.error(f"Bucket {self.bucket_name} does not exist")
            elif error_code == "403":
                self.logger.error(f"Access denied to bucket {self.bucket_name}")
            else:
                self.logger.error(f"Error checking bucket: {e}")
            return False
