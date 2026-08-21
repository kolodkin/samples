OCI A1 Instance Retry
---

Works around Oracle Cloud's "Out of capacity for shape VM.Standard.A1.Flex" error on Always Free accounts by retrying `oci compute instance launch` until a capacity slot frees up. Configure once via `config.env` (all OCIDs are recoverable from the console's "Save as stack" option on the failed review page), then either run the loop in the foreground or schedule single attempts with cron. The script is idempotent — if an instance with the configured display name already exists, it exits without creating a duplicate.

```bash
cp config.env.example config.env   # fill in your OCIDs
./create-instance.sh               # retry loop, one attempt every 90s

# or schedule it instead (exit code 2 = still out of capacity):
# */5 * * * * flock -n /tmp/oci-a1.lock /path/to/create-instance.sh --once >> ~/oci-a1.log 2>&1
```
