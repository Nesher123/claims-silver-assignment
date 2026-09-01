"""Download immutable claims, identity reference data, and assignment documents."""

from .aws import s3_client
from .constants import CLAIMS_KEYS, DATA, DOC_KEYS, TOKEN_KEY


def download_all(cfg: dict) -> None:
    """Download all assignment inputs from S3."""
    bucket = cfg["aws"]["bucket"]
    s3 = s3_client(cfg)
    DATA.mkdir(parents=True, exist_ok=True)
    (DATA / "claims").mkdir(exist_ok=True)
    (DATA / "reference").mkdir(parents=True, exist_ok=True)
    (DATA / "docs").mkdir(exist_ok=True)

    for key in CLAIMS_KEYS:
        dest = DATA / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        print(f"download s3://{bucket}/{key}")
        s3.download_file(bucket, key, str(dest))

    dest = DATA / TOKEN_KEY
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"download s3://{bucket}/{TOKEN_KEY}")
    s3.download_file(bucket, TOKEN_KEY, str(dest))

    for key, dest in DOC_KEYS.items():
        dest.parent.mkdir(parents=True, exist_ok=True)
        print(f"download s3://{bucket}/{key} -> {dest}")
        s3.download_file(bucket, key, str(dest))

    print("download complete")
