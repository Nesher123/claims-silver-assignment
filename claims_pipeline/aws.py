"""Construct region-scoped AWS clients used for S3, Glue, and Athena operations."""

import boto3


def s3_client(cfg: dict):
    return boto3.client("s3", region_name=cfg["aws"]["region"])


def athena_client(cfg: dict):
    return boto3.client("athena", region_name=cfg["aws"]["region"])


def glue_client(cfg: dict):
    return boto3.client("glue", region_name=cfg["aws"]["region"])
