#!/usr/bin/env bash
# Retries `oci compute instance launch` until OCI has capacity for the shape.
#
# Oracle's Always Free VM.Standard.A1.Flex pool is chronically oversubscribed;
# the console returns "Out of capacity for shape VM.Standard.A1.Flex" and the
# only remedy is to retry until a slot frees up. This script automates that.
#
# Modes:
#   ./create-instance.sh          # loop forever, one attempt every RETRY_SECONDS
#   ./create-instance.sh --once   # single attempt (for cron/systemd scheduling);
#                                 # exit 0 = created or already exists,
#                                 # exit 2 = still out of capacity, 1 = fatal error
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
CONFIG="${CONFIG:-$HERE/config.env}"
if [ -f "$CONFIG" ]; then
  # shellcheck source=config.env.example
  . "$CONFIG"
fi

: "${COMPARTMENT_OCID:?set COMPARTMENT_OCID in config.env}"
: "${AVAILABILITY_DOMAIN:?set AVAILABILITY_DOMAIN in config.env}"
: "${SUBNET_OCID:?set SUBNET_OCID in config.env}"
: "${IMAGE_OCID:?set IMAGE_OCID in config.env}"
: "${SSH_PUB_KEY_FILE:?set SSH_PUB_KEY_FILE in config.env}"

SHAPE="${SHAPE:-VM.Standard.A1.Flex}"
OCPUS="${OCPUS:-2}"                 # Always Free A1 limit is now 2 OCPUs / 12 GB
MEMORY_GB="${MEMORY_GB:-12}"
BOOT_VOLUME_GB="${BOOT_VOLUME_GB:-50}"
DISPLAY_NAME="${DISPLAY_NAME:-a1-free}"
RETRY_SECONDS="${RETRY_SECONDS:-90}"

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

instance_exists() {
  local count
  count=$(oci compute instance list \
    --compartment-id "$COMPARTMENT_OCID" \
    --display-name "$DISPLAY_NAME" \
    --query "length(data[?\"lifecycle-state\"!='TERMINATED' && \"lifecycle-state\"!='TERMINATING'])" \
    --raw-output 2>/dev/null) || return 1
  [ "${count:-0}" -gt 0 ]
}

attempt_launch() {
  local out rc
  out=$(oci compute instance launch \
    --compartment-id "$COMPARTMENT_OCID" \
    --availability-domain "$AVAILABILITY_DOMAIN" \
    --subnet-id "$SUBNET_OCID" \
    --image-id "$IMAGE_OCID" \
    --shape "$SHAPE" \
    --shape-config "{\"ocpus\": $OCPUS, \"memoryInGBs\": $MEMORY_GB}" \
    --boot-volume-size-in-gbs "$BOOT_VOLUME_GB" \
    --display-name "$DISPLAY_NAME" \
    --assign-public-ip true \
    --ssh-authorized-keys-file "$SSH_PUB_KEY_FILE" \
    --query 'data.id' --raw-output 2>&1)
  rc=$?

  if [ $rc -eq 0 ]; then
    log "SUCCESS: instance created: $out"
    return 0
  fi

  case "$out" in
    *"Out of capacity"*|*"Out of host capacity"*|*InternalError*)
      log "attempt failed: out of capacity, will retry"
      return 2 ;;
    *TooManyRequests*)
      log "attempt failed: rate limited (429), will retry"
      return 2 ;;
    *LimitExceeded*)
      log "FATAL: service limit exceeded — you already use your Always Free A1"
      log "quota (2 OCPUs / 12 GB total). Terminate or shrink existing A1"
      log "instances first. Error: $out"
      return 1 ;;
    *)
      log "FATAL: launch failed with a non-capacity error, not retrying:"
      log "$out"
      return 1 ;;
  esac
}

if instance_exists; then
  log "instance '$DISPLAY_NAME' already exists and is not terminated — nothing to do"
  exit 0
fi

if [ "${1:-}" = "--once" ]; then
  attempt_launch
  exit $?
fi

attempt=0
while :; do
  attempt=$((attempt + 1))
  log "attempt #$attempt: launching $SHAPE ($OCPUS OCPU / ${MEMORY_GB}GB) in $AVAILABILITY_DOMAIN"
  attempt_launch
  rc=$?
  [ $rc -eq 0 ] && exit 0
  [ $rc -eq 1 ] && exit 1
  sleep "$RETRY_SECONDS"
done
