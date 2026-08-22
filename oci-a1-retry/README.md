OCI A1 Instance Retry
---

Works around Oracle Cloud's "Out of capacity for shape VM.Standard.A1.Flex" error on Always Free accounts by retrying `oci compute instance launch` until a capacity slot frees up, sweeping every availability domain on each attempt. It needs no configuration beyond an authenticated OCI CLI — the compartment comes from the CLI config's tenancy, and the availability domains, subnet and newest matching image are discovered at run time; `config.env` exists only to override those (see `config.env.example`). The script is idempotent, so if an instance with the configured display name already exists it exits without creating a duplicate.

```bash
./create-instance.sh               # retry loop, one sweep every 90s

# or schedule it instead (exit code 2 = still out of capacity):
# */5 * * * * flock -n /tmp/oci-a1.lock /path/to/create-instance.sh --once >> ~/oci-a1.log 2>&1
```

No always-on machine? `.github/workflows/oci-a1-retry.yml` runs it from GitHub Actions every 15 minutes (8 attempts per run). It needs three repository secrets — `OCI_PRIVATE_KEY`, `OCI_USER_OCID`, `OCI_TENANCY_OCID` — and derives the key fingerprint from the private key. Scheduled workflows only fire on the repo's default branch.
