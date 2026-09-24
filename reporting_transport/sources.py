"""Source handles for the shared Transport publisher.

SMB is optional: importing the local publisher never imports smbprotocol.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import ntpath
from pathlib import Path
import re
import stat as stat_module

from reporting_transport.contract import TransportContractError, TransportStorageError


@dataclass(frozen=True)
class SMBSourceReader:
    """A UNC file opened with an existing Kerberos credential cache."""

    unc: str
    _connections: dict = field(default_factory=dict, compare=False, hash=False,
                               repr=False)

    def __post_init__(self):
        if not isinstance(self.unc, str) or not self.unc.startswith("\\\\"):
            raise TransportContractError("SMB source must be a UNC path")
        parts = self.unc[2:].split("\\")
        if (len(parts) < 3 or not all(parts) or
                any(part in (".", "..") for part in parts) or
                any("/" in part or "\x00" in part for part in parts) or
                not re.fullmatch(r"[A-Za-z0-9_.-]+", parts[0])):
            raise TransportContractError("SMB source must be \\\\server\\share\\file")

    @property
    def name(self):
        return ntpath.basename(self.unc)

    @property
    def server(self):
        return self.unc[2:].split("\\", 1)[0]

    @property
    def share(self):
        return self.unc[2:].split("\\")[1]

    @property
    def relative_path(self):
        return "\\".join(self.unc[2:].split("\\")[2:])

    def _client(self):
        try:
            import smbclient
            import gssapi  # noqa: F401 - required for cache-backed Kerberos
        except ImportError as exc:
            raise TransportContractError(
                "SMB Kerberos requires smbprotocol and python-gssapi; "
                "install the Transport SMB dependencies") from exc
        return smbclient

    def _options(self):
        # A private cache prevents reuse of a session authenticated by another
        # component with NTLM. The same cache is passed through DFS referrals.
        return {"auth_protocol": "kerberos", "connection_cache": self._connections}

    def stat(self):
        try:
            return self._client().stat(self.unc, **self._options())
        except FileNotFoundError:
            raise
        except Exception as exc:
            raise TransportStorageError(
                f"SMB source access failed for {self.unc}: check ticket cache, "
                "CIFS SPN, DNS, referrals and share permissions") from exc

    def open(self, mode="rb"):
        if mode != "rb":
            raise ValueError("SMB sources are read-only binary streams")
        try:
            return self._client().open_file(self.unc, mode=mode,
                                            **self._options())
        except Exception as exc:
            raise TransportStorageError(
                f"SMB source open failed for {self.unc}: check Kerberos ticket, "
                "CIFS SPN and share permissions") from exc

    def is_file(self):
        try:
            info = self.stat()
        except FileNotFoundError:
            return False
        return stat_module.S_ISREG(info.st_mode)

    def __str__(self):
        return self.unc


def source_reader(value: str | Path | SMBSourceReader):
    if isinstance(value, SMBSourceReader):
        return value
    return Path(value)
