# Environment access template

Keep the populated `ACCESS.md` local; Git ignores it.

## AWS identity

- Account ID: `<account-id>`
- IAM username: `<username>`
- Region: `us-east-1`

## CLI / boto3

Prefer an AWS profile or environment variables from `.env.example`:

- Access key ID: `<secret>`
- Secret access key: `<secret>`
- Session token: `<optional-secret>`

## Assignment resources

- Athena workgroup: `dataweb_assignment`
- Deliveries: `s3://<bucket>/raw/claims/`
- Token graph: `s3://<bucket>/reference/member_tokens/`
- Documents: `s3://<bucket>/docs/`
- Athena database: `dataweb_<name>`
- Writable prefix: `s3://<bucket>/candidate/<iam-username>/`
