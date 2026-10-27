# AWS IAM policies for the msk-bench client pods

JSON cannot carry comments, so the values to replace are listed here.

| File | Purpose | Replace before use |
|---|---|---|
| `msk-bench-client-policy.json` | Permissions policy for the client pod role (Amazon MSK data plane, IAM auth) | `111122223333` -> your AWS account ID. `us-east-1` -> your Region. `load-benchmark` -> your Amazon MSK cluster name. |
| `trust-policy-pod-identity.json` | Trust policy for Amazon EKS Pod Identity | Nothing. |
| `trust-policy-irsa.json` | Trust policy for IAM roles for service accounts (IRSA), if you use IRSA instead of Pod Identity | `111122223333` -> your AWS account ID. `us-east-1` -> your Region. `REPLACE_ME_OIDC_ID` -> the ID at the end of your cluster's OIDC issuer URL (`aws eks describe-cluster --name <eks> --query cluster.identity.oidc.issuer --output text`). |

Use exactly one of the two trust policies, matching the mechanism you configure in
`k8s/base/10-serviceaccount.yaml`.
