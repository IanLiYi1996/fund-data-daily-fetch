## industry-assets-stack skill routing

This project uses the industry-assets-stack skills for CI/CD on
`gitlab.aws.dev`. When a request matches, invoke the skill via the Skill tool
instead of answering ad-hoc:

- Setup verification, "check my setup", or any auth/tooling error → invoke `preflight` first
- AWS credentials for CI, `AWS_CREDS_TARGET_ROLE`, IAM role/trust policy, "auth failing" → invoke `gitlab-aws-auth`
- Generate or extend a `.gitlab-ci.yml` (build / test / container-scan / ECR) → invoke `gitlab-ci-pipeline`
- Deliver a change: lint, branch, open a merge request → invoke `gitlab-mr-submit`
- Host a static directory (coverage report, docs, dist/) on GitLab Pages → invoke `gitlab-pages-deploy`

Hard constraints these skills enforce (do not work around them): Credential
Vendor only (never standard OIDC on gitlab.aws.dev); no Docker-in-Docker (use
Kaniko); never open an MR on a failing `glab ci lint`; never commit to the
default branch; IAM writes only after showing the target account and getting
explicit confirmation.
