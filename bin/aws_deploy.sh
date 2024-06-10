#!/bin/bash

aws ec2 create-key-pair --key-name beta9key --query 'KeyMaterial' --output text | tee "./deploy/aws-cloudformation/beta9key.pem"

chmod 400 beta9key.pem

aws cloudformation deploy  --stack-name beta9 --template-file "./deploy/aws-cloudformation/beta9.yaml" --region us-east-1

