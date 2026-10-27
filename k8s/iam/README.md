# AWS IAM policies for the msk-bench client pods

These files are only for readers who bring their own Amazon EKS cluster. If you create the cluster
with `eks/cluster.yaml` (the supported path), eksctl already creates the Pod Identity role
`msk-bench-eks-client-pod-role`, its association and the ServiceAccount `msk-bench/msk-bench-client`;
you do not need anything in this directory.

JSON cannot carry comments, so the values to replace are listed here.

| File | Purpose | Replace before use |
|---|---|---|
| `msk-bench-client-policy.json` | Permissions policy for the client pod role (Amazon MSK data plane, IAM auth) | `111122223333` -> your AWS account ID. `us-east-1` -> your Region. `load-benchmark` -> your Amazon MSK cluster name. |
| `trust-policy-pod-identity.json` | Trust policy for Amazon EKS Pod Identity | Nothing. |
| `trust-policy-irsa.json` | Trust policy for IAM roles for service accounts (IRSA), if you use IRSA instead of Pod Identity | `111122223333` -> your AWS account ID. `us-east-1` -> your Region. `REPLACE_ME_OIDC_ID` -> the ID at the end of your cluster's OIDC issuer URL (`aws eks describe-cluster --name <eks> --query cluster.identity.oidc.issuer --output text`). |

Use exactly one of the two trust policies. With Pod Identity, create the association (see
`k8s/README.md`, section 2); `k8s/base/10-serviceaccount.yaml` needs no change. With IRSA, add the
`eks.amazonaws.com/role-arn` annotation to that ServiceAccount yourself (it ships without one).
