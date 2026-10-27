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
