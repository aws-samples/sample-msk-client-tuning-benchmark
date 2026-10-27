#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Sourced by the host-side scripts. Selects the MSK control-plane endpoint used
# by `aws kafka ...` calls (get-bootstrap-brokers), based on MSK_ENDPOINT_URL:
#   unset / prod / production -> production control plane (the default here)
#   <any url>                 -> that endpoint verbatim (e.g. a custom or VPC endpoint)
#
# The benchmark targets a regular Amazon MSK Provisioned (Standard broker) cluster,
# so the public regional endpoint is the right default. The indirection lets the
# same scripts be pointed at another endpoint without edits.

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

# msk_check_tuning_props <file>
# Refuses (exit 1) if a client tuning file sets any authentication key. MSK IAM auth
# always comes from the IAM auth file (client-iam.properties); a tuning profile that
# overrides it would silently drop or change authentication. Same rule as
# k8s/image/entrypoint.sh. A missing file is fine (nothing to check).
msk_check_tuning_props() {
  local file="$1" hits
  [[ -f "$file" ]] || return 0
  hits="$(grep -nE '^[[:space:]]*(security\.protocol|sasl\.mechanism|sasl\.jaas\.config|sasl\.client\.callback\.handler\.class)([[:space:]]*[=:]|[[:space:]]|$)' "$file" || true)"
  if [[ -n "$hits" ]]; then
    echo "ERROR: $file sets authentication keys, which are not allowed in a tuning profile:" >&2
    sed 's/^/  /' <<<"$hits" >&2
    echo "ERROR: remove security.protocol / sasl.mechanism / sasl.jaas.config / sasl.client.callback.handler.class;" >&2
    echo "       MSK IAM auth comes from /opt/kafka/config/client-iam.properties. Refusing to start." >&2
    exit 1
  fi
}
