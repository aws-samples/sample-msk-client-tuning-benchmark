#!/bin/bash
# Sourced by the host-side scripts. Selects the MSK control-plane endpoint used
# by `aws kafka ...` calls (get-bootstrap-brokers), based on MSK_ENDPOINT_URL:
#   unset / prod / production -> production control plane (the default here)
#   <any url>                 -> that endpoint verbatim (e.g. a beta endpoint)
#
# The benchmark cluster is a plain production Standard cluster, so prod is the
# right default. This indirection is kept so the same scripts can be pointed at
# a non-prod endpoint without edits.

_msk_endpoint_raw="${MSK_ENDPOINT_URL:-prod}"

case "$_msk_endpoint_raw" in
  prod|production|"")
    MSK_ENDPOINT_ARGS=()
    MSK_STAGE=prod
    ;;
  *)
    MSK_ENDPOINT_ARGS=(--endpoint-url "$_msk_endpoint_raw")
    MSK_STAGE=custom
    ;;
esac

echo "[lib-msk-env] MSK endpoint: ${MSK_ENDPOINT_ARGS[*]:-<production default>} (stage: $MSK_STAGE)" >&2
