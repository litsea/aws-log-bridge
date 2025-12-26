# aws-log-bridge

## AWS IAM Policy

```hcl
  statement {
    sid    = "LogForBridge"
    effect = "Allow"
    actions = [
      "logs:FilterLogEvents",
    ]
    resources = [
      "*"
    ]
  }
```