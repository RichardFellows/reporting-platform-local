"""Opt-in live Spark checks, isolated bucket and Nessie branch; NEVER merges.

Run on the compose network with the checkout mounted, using its normal Spark
runtime. Creates only uniquely named test resources. No registry or scheduler.
"""
from __future__ import annotations

import hashlib
import json
import os
import uuid


def main():
    token = uuid.uuid4().hex[:12]
    bucket = f"parsing-validation-{token}"
    branch = f"build/parsing-validation-{token}"
    os.environ["REPORTING_WAREHOUSE"] = f"s3a://{bucket}/warehouse"
    os.environ["REPORTING_LANDING"] = f"s3a://{bucket}/landing"
    from reporting_platform.common.context import Feed
    from reporting_platform.common.nessie import Nessie
    from reporting_platform.common.spark import spark_session
    from reporting_platform.common.parsing import csv_rows, feed_format, raw_values
    from reporting_platform.ingest.arrival import _client
    from reporting_platform.ingest.normalize import normalize
    from reporting_platform.ingest.ingest_feed import read_landing, reconcile_schema, _delivery_md5
    from reporting_platform.ingest.sample_diagnostics import diagnose_samples
    from tests.parsing_fixtures import supported_cases
    nessie, s3 = Nessie(), _client()
    before = nessie.get_reference("main")["reference"]["hash"]
    s3.create_bucket(Bucket=bucket)
    nessie.create_branch(branch)
    spark = None
    report = {"branch": branch, "bucket": bucket, "cases": [], "main_before": before}
    print(json.dumps(report), flush=True)
    try:
        spark = spark_session("parsing-contract-validation", ref=branch)
        report["spark_version"] = spark.version
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.parsing_validation")
        for i, (label, data, overrides) in enumerate(supported_cases()):
            fd = Feed(name=f"test_case_{i}", description="synthetic fixture", source_system="TEST",
                      filename_pattern=r"sample_(?P<cob_date>\d{8})\.csv",
                      business_key=["id"], columns=["id", "value"], control_encoding="utf-8",
                      delivery={"kind": "file", "control": {
                          "pattern": r"{stem}\.ctl", "format": {"kind": "key_value", "separator": "="},
                          "row_count": "ROWS", "md5": "MD5"}}, **overrides)
            fmt = feed_format(fd)
            parsed = list(csv_rows(data, fmt))
            expected = [raw_values(row) for row in (parsed[1:] if fd.header else parsed)]
            ctl = (f"ROWS={len(expected)}\nMD5={hashlib.md5(data).hexdigest()}\n").encode("utf-8-sig")
            key = f"landing/{fd.name}/sample_20260917.csv"
            s3.put_object(Bucket=bucket, Key=key, Body=data)
            s3.put_object(Bucket=bucket, Key=key[:-4]+".ctl", Body=ctl)
            manifest = normalize(fd, key, write=False)
            diagnostics = diagnose_samples(fd, data, ctl)
            assert diagnostics["ok"], diagnostics
            df = read_landing(spark, manifest["format"], f"s3a://{bucket}/{key}")
            actual = [list(row) for row in df.collect()]
            assert actual == expected, (label, actual, expected)
            reconciled, drift = reconcile_schema(df, fd)
            assert [list(row) for row in reconciled.select("id", "value").collect()] == expected, (label, drift)
            assert len(actual) == manifest["declared_row_count"]
            assert _delivery_md5(manifest) == manifest["declared_md5"]
            table = f"lakehouse.parsing_validation.case_{i}"
            df.writeTo(table).using("iceberg").create()
            assert spark.table(table).count() == len(expected)
            report["cases"].append({"case": label, "rows": len(actual), "status": "passed"})
            print(json.dumps(report["cases"][-1]), flush=True)
        # The production reader must refuse invalid bytes before Spark replaces them.
        key = "landing/invalid.csv"
        s3.put_object(Bucket=bucket, Key=key, Body=b"id,value\n1,\xff\n")
        try:
            read_landing(spark, feed_format(Feed(name="test_bad", description="", source_system="TEST",
                         filename_pattern="", business_key=[], columns=[])), f"s3a://{bucket}/{key}")
        except ValueError:
            report["invalid_utf8"] = "refused"
        else:
            raise AssertionError("invalid UTF-8 reached Spark")
        report["main_after"] = nessie.get_reference("main")["reference"]["hash"]
        assert report["main_after"] == before, "main changed during isolated validation"
        report["status"] = "passed"
    finally:
        if spark:
            spark.stop()
        nessie.delete_reference(branch)
        # Only this run's bucket; never a shared prefix or warehouse sweep.
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
        s3.delete_bucket(Bucket=bucket)
        report["cleanup"] = "branch and isolated bucket deleted"
        print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
