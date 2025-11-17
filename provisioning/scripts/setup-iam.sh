#!/bin/bash
#
# Setup IAM roles for Beta9 provisioning service
#
# This script creates the necessary IAM roles and policies for the provisioning
# service to operate in AWS.
#
# Usage:
#   ./scripts/setup-iam.sh <aws-account-id> <external-id>
#
# Example:
#   ./scripts/setup-iam.sh 123456789012 my-external-id

set -e

if [ $# -lt 2 ]; then
    echo "Usage: $0 <aws-account-id> <external-id>"
    exit 1
fi

AWS_ACCOUNT_ID="$1"
EXTERNAL_ID="$2"
ROLE_NAME="Beta9Provisioner"
POLICY_NAME="Beta9ProvisioningPolicy"

echo "🔐 Setting up IAM roles for Beta9 provisioning"
echo "   AWS Account: $AWS_ACCOUNT_ID"
echo "   External ID: $EXTERNAL_ID"
echo ""

# Create trust policy
cat > /tmp/trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::$AWS_ACCOUNT_ID:root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "$EXTERNAL_ID"
        }
      }
    }
  ]
}
EOF

# Create IAM role
echo "📝 Creating IAM role: $ROLE_NAME"
aws iam create-role \
    --role-name "$ROLE_NAME" \
    --assume-role-policy-document file:///tmp/trust-policy.json \
    --description "Role for Beta9 provisioning service" \
    || echo "Role already exists"

# Create policy document
cat > /tmp/provisioning-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EC2Permissions",
      "Effect": "Allow",
      "Action": [
        "ec2:*"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EKSPermissions",
      "Effect": "Allow",
      "Action": [
        "eks:*"
      ],
      "Resource": "*"
    },
    {
      "Sid": "RDSPermissions",
      "Effect": "Allow",
      "Action": [
        "rds:*"
      ],
      "Resource": "*"
    },
    {
      "Sid": "ElastiCachePermissions",
      "Effect": "Allow",
      "Action": [
        "elasticache:*"
      ],
      "Resource": "*"
    },
    {
      "Sid": "S3Permissions",
      "Effect": "Allow",
      "Action": [
        "s3:*"
      ],
      "Resource": "*"
    },
    {
      "Sid": "IAMPermissions",
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:DeleteRole",
        "iam:GetRole",
        "iam:PassRole",
        "iam:AttachRolePolicy",
        "iam:DetachRolePolicy",
        "iam:ListAttachedRolePolicies",
        "iam:CreateInstanceProfile",
        "iam:DeleteInstanceProfile",
        "iam:GetInstanceProfile",
        "iam:AddRoleToInstanceProfile",
        "iam:RemoveRoleFromInstanceProfile"
      ],
      "Resource": "*"
    },
    {
      "Sid": "SecretsManagerPermissions",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ],
      "Resource": "*"
    },
    {
      "Sid": "DynamoDBStatePermissions",
      "Effect": "Allow",
      "Action": [
        "dynamodb:CreateTable",
        "dynamodb:DeleteTable",
        "dynamodb:DescribeTable",
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:DeleteItem"
      ],
      "Resource": "arn:aws:dynamodb:*:$AWS_ACCOUNT_ID:table/beta9-tf-state-*"
    }
  ]
}
EOF

# Create/update policy
echo "📝 Creating IAM policy: $POLICY_NAME"
POLICY_ARN="arn:aws:iam::$AWS_ACCOUNT_ID:policy/$POLICY_NAME"

aws iam create-policy \
    --policy-name "$POLICY_NAME" \
    --policy-document file:///tmp/provisioning-policy.json \
    --description "Policy for Beta9 provisioning operations" \
    || echo "Policy already exists, updating..."

# If policy exists, update it
aws iam create-policy-version \
    --policy-arn "$POLICY_ARN" \
    --policy-document file:///tmp/provisioning-policy.json \
    --set-as-default \
    2>/dev/null || true

# Attach policy to role
echo "📎 Attaching policy to role"
aws iam attach-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-arn "$POLICY_ARN"

# Get role ARN
ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text)

echo ""
echo "✅ IAM setup complete!"
echo ""
echo "📋 Configuration to use in your meta-config:"
echo ""
echo "cloud:"
echo "  provider: aws"
echo "  region: us-east-1"
echo "  account_id: \"$AWS_ACCOUNT_ID\""
echo "  role_arn: \"$ROLE_ARN\""
echo "  external_id: \"$EXTERNAL_ID\""
echo ""

# Cleanup
rm /tmp/trust-policy.json /tmp/provisioning-policy.json
