#!/usr/bin/env bash
# Refuse to run outside the Claude Code web (cloud) environment. These scripts
# install/start throwaway local e2e services under .cache/ and mutate on-disk
# state; running them on a developer's own machine is almost never intended.
# CLAUDE_CODE_REMOTE=true is set only in the remote/web container (CLAUDECODE=1
# is set for any Claude Code session, local included, so it can't gate on that).
#
# Source this near the top of each claudeai/*.sh entrypoint and of setup.sh.
# Override the guard for a deliberate local run with CLAUDEAI_ALLOW_LOCAL=1.
if [ "${CLAUDEAI_ALLOW_LOCAL:-}" != "1" ] && [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  printf '\033[31m[claudeai]\033[0m %s\n' \
    "Refusing to run: not the Claude Code web environment (CLAUDE_CODE_REMOTE != true)." >&2
  printf '\033[31m[claudeai]\033[0m %s\n' \
    "Set CLAUDEAI_ALLOW_LOCAL=1 to override for a deliberate local run." >&2
  exit 1
fi
