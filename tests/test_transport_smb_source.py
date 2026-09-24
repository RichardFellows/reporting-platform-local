"""Mock SMB I/O through the existing publisher; no live AD/share required."""
from __future__ import annotations

from contextlib import contextmanager
import io
import stat
import sys
from types import SimpleNamespace

from reporting_transport.contract import (
    TransportConflictError, TransportContractError, TransportStorageError,
)
from reporting_transport.publisher import publish_transport
from reporting_transport.sources import SMBSourceReader
from tests.fakes3 import FakeS3


@contextmanager
def fake_smb(files, *, fail_open=False):
    import types
    smb = types.ModuleType("smbclient")
    gss = types.ModuleType("gssapi")
    calls = []

    def metadata(path, **options):
        calls.append(("stat", path, options))
        return SimpleNamespace(st_size=len(files[path]), st_mtime=123,
                               st_mode=stat.S_IFREG)

    def opening(path, *, mode, **options):
        calls.append(("open", path, options))
        if fail_open:
            raise RuntimeError("ticket expired")
        return io.BytesIO(files[path])

    smb.stat = metadata
    smb.open_file = opening
    previous = {name: sys.modules.get(name) for name in ("smbclient", "gssapi")}
    sys.modules.update(smbclient=smb, gssapi=gss)
    try:
        yield calls
    finally:
        for name, old in previous.items():
            if old is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old


def _publish(files, s3):
    return publish_transport(
        legacy_feed_id="1234", producer_run_id="849217",
        cob_date="2026-09-21", source_system="RISK_ENGINE_X",
        source_observed_at="2026-09-22T01:13:00Z",
        uploaded_at="2026-09-22T01:14:22Z", files=files,
        bucket="lakehouse", client=s3)


def test_smb_data_control_publish_and_retry_without_ntlm():
    data = r"\\files.example.org\reports\feed\positions.csv"
    control = r"\\files.example.org\reports\feed\positions.ctl"
    files = {data: b"a,b\r\n1,\xff\r\n", control: b"ROWS=1\r\n"}
    s3 = FakeS3()
    with fake_smb(files) as calls:
        sources = [("data", SMBSourceReader(data)),
                   ("control", SMBSourceReader(control))]
        first = _publish(sources, s3)
        second = _publish(sources, s3)
    assert first.as_dict() == second.as_dict()
    assert [f.original_filename for f in first.files] == ["positions.csv", "positions.ctl"]
    assert all(options["auth_protocol"] == "kerberos" for _, _, options in calls)
    assert all("username" not in options and "password" not in options
               for _, _, options in calls)
    for evidence in first.files:
        assert s3.objects[evidence.object_key][0] == files[
            data if evidence.role == "data" else control]


def test_smb_open_failure_does_not_publish_marker():
    path = r"\\files.example.org\reports\feed\positions.csv"
    s3 = FakeS3()
    with fake_smb({path: b"raw"}, fail_open=True):
        try:
            _publish([("data", SMBSourceReader(path))], s3)
        except TransportStorageError as exc:
            assert "Kerberos" in str(exc)
        else:
            raise AssertionError("source open should fail")
    assert not any(key.endswith("_COMPLETE.json") for key in s3.objects)


def test_same_transport_id_with_changed_smb_bytes_conflicts():
    path = r"\\files.example.org\reports\feed\positions.csv"
    files = {path: b"first"}
    s3 = FakeS3()
    with fake_smb(files):
        source = [("data", SMBSourceReader(path))]
        _publish(source, s3)
        files[path] = b"other"
        try:
            _publish(source, s3)
        except TransportConflictError:
            pass
        else:
            raise AssertionError("changed bytes were accepted")


def test_smb_path_requires_server_share_and_basename():
    for path in (r"\\server\share", r"\\server\share\..\file.csv",
                 "/mnt/share/file.csv"):
        try:
            SMBSourceReader(path)
        except TransportContractError:
            pass
        else:
            raise AssertionError(f"accepted {path!r}")
