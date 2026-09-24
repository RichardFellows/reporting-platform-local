# Transport source access

`reporting_transport.publisher` uses one publication path for local files and
Kerberos SMB sources. It derives the TransportID from `source`, legacy feed ID
and producer run ID, preserves source basenames, hashes bytes as they are read,
uploads to S3 with create-only writes, confirms stored evidence and writes
`_COMPLETE.json` last. The Transport contract has no SMB fields.

Local paths remain the default. For direct SMB, run the publisher where an
existing Kerberos credential cache is available:

```sh
python -m reporting_transport publish \
  --legacy-feed-id 1234 --producer-run-id 849217 \
  --cob-date 2026-09-21 --source-system RISK_ENGINE_X \
  --source-observed-at 2026-09-22T01:13:00Z \
  --source-access smb \
  --data '\\files.example.org\reports\feed\positions.csv' \
  --control '\\files.example.org\reports\feed\positions.ctl' \
  --bucket lakehouse
```

Install `requirements-transport-smb.txt` into the **publisher** environment.
On Linux its `gssapi` dependency requires MIT Kerberos development headers
and `krb5-config` when a prebuilt wheel is unavailable; install/build in a
build stage, then install the resulting wheels and Kerberos runtime libraries
in the final image. This is not an Airflow ingestion dependency. The runtime
uses `KRB5CCNAME` and `KRB5_CONFIG` through GSSAPI, including supported cache
types; it never runs `kinit` or accepts an SMB password. Provision and renew
the ticket outside the process. Supply a readable credential cache, Kerberos
configuration and DNS/network access to the SMB server on port 445 in
OpenShift. S3 credentials continue to use the boto3 provider chain.

The adapter separates server, share and relative file path from a UNC string,
and uses `smbclient` with `auth_protocol="kerberos"` and an isolated connection
pool. It does not silently negotiate NTLM. SMB signing remains enabled by the
library default. No principal, password or ticket contents enter Transport
evidence. Authentication errors suggest checking `klist`, DNS, share access,
and a valid `cifs/<server-fqdn>` SPN; aliases and referral targets need their
own valid SPNs. No realm or SPN is hard-coded.

The same bounded-memory multipart upload and hash/verification code handles
both source types. The default threshold and part size are 8 MiB; concurrency
is 4. Existing-object retries rehash the source and compare stored evidence.
Changed bytes conflict; incomplete reads, changes in size or modification
time, upload errors and failed verification cannot write the marker. A source
rewritten with identical size and mtime during a read may escape metadata
mutation detection; the hash still covers precisely the bytes read.

`smbprotocol` implements DFS referrals in its high-level `smbclient` API.
This implementation permits those referrals through the same Kerberos-only
connection pool. **Direct SMB and DFS have not been verified against a live
enterprise server.** Referral hostnames, permissions and CIFS SPNs must each
be tested; a working direct UNC does not prove a DFS namespace works.

For a live smoke test, first confirm `klist` shows a valid TGT, then publish a
small binary data/control pair from a direct server UNC with a fresh run ID.
Compare both downloaded S3 objects byte for byte and verify `_COMPLETE.json`
was written last. Retry the same run ID, then change a source byte and confirm
conflict. Repeat with a multipart-sized file. Finally repeat on a DFS UNC and
inspect the referral target and Kerberos service ticket. Do not log the cache,
keytab or internal share contents. A test suite with mocked SMB I/O runs via
`python -m tests.run test_transport_smb_source test_reporting_transport`.
