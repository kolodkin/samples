#!/usr/bin/env bash
# Retries `oci compute instance launch` until OCI has capacity for the shape.
#
# Oracle's Always Free VM.Standard.A1.Flex pool is chronically oversubscribed;
# the console returns "Out of capacity for shape VM.Standard.A1.Flex" and the
# only remedy is to retry until a slot frees up. This script automates that.
#
# Nothing has to be configured: compartment, availability domains, subnet and
# image are all discovered from the tenancy the OCI CLI is authenticated as.
# Any of them can still be pinned via config.env or the environment.
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

SHAPE="${SHAPE:-VM.Standard.A1.Flex}"
# Half the Always Free A1 quota (2 OCPUs / 12 GB): a smaller request fits into
# fragmented capacity far more often, and the instance can be resized up later.
OCPUS="${OCPUS:-1}"
MEMORY_GB="${MEMORY_GB:-6}"
BOOT_VOLUME_GB="${BOOT_VOLUME_GB:-50}"
DISPLAY_NAME="${DISPLAY_NAME:-a1-free}"
RETRY_SECONDS="${RETRY_SECONDS:-90}"
# MAX_ATTEMPTS=0 loops forever; a positive value exits 2 once spent, so a
# bounded runner (a GitHub Actions job) can make a few attempts per invocation
# and let the scheduler drive the long game.
MAX_ATTEMPTS="${MAX_ATTEMPTS:-0}"
OS_NAME="${OS_NAME:-Canonical Ubuntu}"
OS_VERSION="${OS_VERSION:-24.04}"

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
die() { log "FATAL: $*"; exit 1; }

command -v oci >/dev/null || die "the OCI CLI is not installed (pip install oci-cli)"

# --- resolve the ssh public key -------------------------------------------
# A public key is not a secret: keep it next to the script, or point at your own.
if [ -z "${SSH_PUB_KEY_FILE:-}" ]; then
  for candidate in "$HERE/authorized_key.pub" "$HOME/.ssh/id_ed25519.pub" "$HOME/.ssh/id_rsa.pub"; do
    if [ -s "$candidate" ]; then SSH_PUB_KEY_FILE="$candidate"; break; fi
  done
fi
[ -s "${SSH_PUB_KEY_FILE:-}" ] || die "no ssh public key found — drop one at $HERE/authorized_key.pub or set SSH_PUB_KEY_FILE"

# --- discovery -------------------------------------------------------------
# The tenancy OCID is already in the CLI config the caller authenticated with,
# so it never needs to be supplied twice.
if [ -z "${COMPARTMENT_OCID:-}" ]; then
  oci_config_file="${OCI_CLI_CONFIG_FILE:-$HOME/.oci/config}"
  COMPARTMENT_OCID="${OCI_CLI_TENANCY:-}"
  if [ -z "$COMPARTMENT_OCID" ] && [ -f "$oci_config_file" ]; then
    COMPARTMENT_OCID=$(sed -n 's/^[[:space:]]*tenancy[[:space:]]*=[[:space:]]*//p' "$oci_config_file" | head -1)
  fi
  [ -n "$COMPARTMENT_OCID" ] || die "could not read the tenancy OCID from $oci_config_file — set COMPARTMENT_OCID"
  log "using tenancy root compartment: $COMPARTMENT_OCID"
fi

# Capacity is per availability domain, so collect every AD and try each one —
# that is exactly what the console error suggests doing by hand.
if [ -n "${AVAILABILITY_DOMAIN:-}" ]; then
  ADS="$AVAILABILITY_DOMAIN"
else
  ADS=$(oci iam availability-domain list --compartment-id "$COMPARTMENT_OCID" \
        --query 'data[*].name' --raw-output 2>/dev/null | tr -d '[]", ' | grep -v '^$')
  [ -n "$ADS" ] || die "could not list availability domains — check that the OCI CLI is authenticated"
  log "availability domains: $(echo "$ADS" | tr '\n' ' ')"
fi

if [ -z "${SUBNET_OCID:-}" ]; then
  SUBNET_OCID=$(oci network subnet list --compartment-id "$COMPARTMENT_OCID" \
    --query 'data[0].id' --raw-output 2>/dev/null)
  [ -n "$SUBNET_OCID" ] && [ "$SUBNET_OCID" != "null" ] \
    || die "no subnet found in this compartment — create a VCN once (console: Networking > VCN wizard) or set SUBNET_OCID"
  log "using subnet: $SUBNET_OCID"
fi

# Image OCIDs are region-specific and get replaced whenever Oracle refreshes the
# published image, so always resolve the newest one rather than pinning it.
if [ -z "${IMAGE_OCID:-}" ]; then
  IMAGE_OCID=$(oci compute image list --compartment-id "$COMPARTMENT_OCID" \
    --operating-system "$OS_NAME" --operating-system-version "$OS_VERSION" \
    --shape "$SHAPE" --sort-by TIMECREATED --sort-order DESC \
    --query 'data[0].id' --raw-output 2>/dev/null)
  [ -n "$IMAGE_OCID" ] && [ "$IMAGE_OCID" != "null" ] \
    || die "no $OS_NAME $OS_VERSION image available for $SHAPE — set IMAGE_OCID or adjust OS_NAME/OS_VERSION"
  log "using latest $OS_NAME $OS_VERSION image: $IMAGE_OCID"
fi

instance_exists() {
  local count
  count=$(oci compute instance list \
    --compartment-id "$COMPARTMENT_OCID" \
    --display-name "$DISPLAY_NAME" \
    --query "length(data[?\"lifecycle-state\"!='TERMINATED' && \"lifecycle-state\"!='TERMINATING'])" \
    --raw-output 2>/dev/null) || return 1
  [ "${count:-0}" -gt 0 ]
}

# Launches into one AD. 0 = created, 2 = retryable (no capacity), 1 = fatal.
attempt_launch() {
  local ad="$1" out rc
  out=$(oci compute instance launch \
    --compartment-id "$COMPARTMENT_OCID" \
    --availability-domain "$ad" \
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
    log "SUCCESS: instance created in $ad: $out"
    return 0
  fi

  case "$out" in
    *"Out of capacity"*|*"Out of host capacity"*|*InternalError*)
      log "  $ad: out of capacity"
      return 2 ;;
    *TooManyRequests*)
      log "  $ad: rate limited (429)"
      return 2 ;;
    *LimitExceeded*)
      log "service limit exceeded — you already use your Always Free A1 quota"
      log "(2 OCPUs / 12 GB total). Terminate or shrink existing A1 instances first."
      log "$out"
      return 1 ;;
    *)
      log "launch failed with a non-capacity error, not retrying:"
      log "$out"
      return 1 ;;
  esac
}

# Sweeps every AD once. Same exit codes as attempt_launch.
sweep_ads() {
  local ad rc worst=2
  for ad in $ADS; do
    attempt_launch "$ad"
    rc=$?
    [ $rc -eq 0 ] && return 0
    [ $rc -eq 1 ] && worst=1
  done
  return $worst
}

if instance_exists; then
  log "instance '$DISPLAY_NAME' already exists and is not terminated — nothing to do"
  exit 0
fi

if [ "${1:-}" = "--once" ]; then
  sweep_ads
  exit $?
fi

attempt=0
while :; do
  attempt=$((attempt + 1))
  log "attempt #$attempt: launching $SHAPE ($OCPUS OCPU / ${MEMORY_GB}GB)"
  sweep_ads
  rc=$?
  [ $rc -eq 0 ] && exit 0
  [ $rc -eq 1 ] && exit 1
  if [ "$MAX_ATTEMPTS" -gt 0 ] && [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
    log "still out of capacity after $attempt attempts, giving up for now"
    exit 2
  fi
  sleep "$RETRY_SECONDS"
done
