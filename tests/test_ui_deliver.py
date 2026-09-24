"""The real-files UI accepts declared control siblings without data drift."""
from unittest.mock import patch

from reporting_platform.common.context import Feed
from reporting_platform.ui import feeddata
from tests.fakes3 import FakeS3


def _feed():
    return Feed(
        name="qa_position", description="Dummy", source_system="QA",
        filename_pattern=r"qa_position_(?P<cob_date>\d{8})\.csv",
        business_key=["position_id"], columns=["position_id", "amount"],
        delimiter="|", delivery={"control": {"pattern": r"{stem}\.ctl"}},
    )


def test_data_and_control_land_unchanged_without_control_header_drift():
    s3 = FakeS3()
    data = b'position_id|amount\nP1|12.50\n'
    control = b'RECORD_COUNT|CHECKSUM\n1|dummy\n'
    with patch("boto3.client", return_value=s3):
        rows = feeddata.deliver(_feed(), [
            ("qa_position_20260914.csv", data),
            ("qa_position_20260914.ctl", control),
        ])
    assert rows[0]["control_file"] is True
    assert rows[0]["missing_columns"] == rows[0]["extra_columns"] == []
    assert rows[1]["cob_date"] == "2026-09-14"
    assert s3.objects[rows[0]["key"]][0] == control
    assert s3.objects[rows[1]["key"]][0] == data


def test_control_can_arrive_separately():
    s3 = FakeS3()
    with patch("boto3.client", return_value=s3):
        rows = feeddata.deliver(_feed(), [("qa_position_20260914.ctl", b"ROWS=1")])
    assert len(rows) == 1 and rows[0]["control_file"]


def test_unrelated_control_rejects_entire_upload_before_writing():
    s3 = FakeS3()
    with patch("boto3.client", return_value=s3):
        try:
            feeddata.deliver(_feed(), [
                ("qa_position_20260914.csv", b"position_id|amount\nP1|1\n"),
                ("another_feed_20260914.ctl", b"ROWS=1"),
            ])
        except feeddata.DataError:
            pass
        else:
            raise AssertionError("unrelated control must be refused")
    assert not s3.objects
