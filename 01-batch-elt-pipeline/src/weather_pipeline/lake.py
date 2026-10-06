"""Data lake: simpan payload mentah di object storage S3-compatible (MinIO)."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import boto3
from botocore.exceptions import ClientError

from weather_pipeline.config import LakeSettings


def raw_object_key(city_id: str, day: date) -> str:
    # Partisi gaya Hive: mudah dibaca ulang per tanggal (replay / backfill).
    return f"open_meteo/daily/date={day.isoformat()}/city={city_id}.json"


class Lake:
    def __init__(self, settings: LakeSettings):
        self.bucket = settings.bucket
        self._s3 = boto3.client(
            "s3",
            endpoint_url=settings.endpoint_url,
            aws_access_key_id=settings.access_key,
            aws_secret_access_key=settings.secret_key,
        )

    def ensure_bucket(self) -> None:
        try:
            self._s3.head_bucket(Bucket=self.bucket)
        except ClientError:
            self._s3.create_bucket(Bucket=self.bucket)

    def put_json(self, key: str, payload: dict[str, Any]) -> None:
        self._s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload).encode("utf-8"),
            ContentType="application/json",
        )

    def get_json(self, key: str) -> dict[str, Any]:
        obj = self._s3.get_object(Bucket=self.bucket, Key=key)
        return json.loads(obj["Body"].read())
