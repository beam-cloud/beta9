# VPC
resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"

  enable_dns_hostnames = true

  tags = {
    Name = "${var.prefix}"
  }
}

# Internet gateway
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.prefix}"
  }
}

# Subnets
resource "aws_subnet" "private-us-east-1a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.0.0/19"
  availability_zone = "us-east-1a"

  tags = {
    "Name"                                          = "${var.prefix}-private-us-east-1a",
    "kubernetes.io/role/internal-elb"               = "1"
    "kubernetes.io/cluster/${var.k3s_cluster_name}" = "owned"
  }
}

resource "aws_subnet" "private-us-east-1b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.32.0/19"
  availability_zone = "us-east-1b"

  tags = {
    "Name"                                          = "${var.prefix}-private-us-east-1b",
    "kubernetes.io/role/internal-elb"               = "1"
    "kubernetes.io/cluster/${var.k3s_cluster_name}" = "owned"
  }
}

resource "aws_subnet" "public-us-east-1a" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.64.0/19"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true

  tags = {
    "Name"                                          = "${var.prefix}-public-us-east-1a",
    "kubernetes.io/role/elb"                        = "1"
    "kubernetes.io/cluster/${var.k3s_cluster_name}" = "owned"
  }
}

resource "aws_subnet" "public-us-east-1b" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.96.0/19"
  availability_zone       = "us-east-1b"
  map_public_ip_on_launch = true

  tags = {
    "Name"                                          = "${var.prefix}-public-us-east-1b",
    "kubernetes.io/role/elb"                        = "1"
    "kubernetes.io/cluster/${var.k3s_cluster_name}" = "owned"
  }
}

# Elastic IP
resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "${var.prefix}"
  }
}

# NAT Gateway
resource "aws_nat_gateway" "nat" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public-us-east-1a.id

  tags = {
    Name = "${var.prefix}"
  }

  depends_on = [aws_internet_gateway.igw]
}

# Route tables
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat.id
  }

  tags = {
    Name = "${var.prefix}-private"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "${var.prefix}-public"
  }
}

resource "aws_route_table_association" "private-us-east-1a" {
  subnet_id      = aws_subnet.private-us-east-1a.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private-us-east-1b" {
  subnet_id      = aws_subnet.private-us-east-1b.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "public-us-east-1a" {
  subnet_id      = aws_subnet.public-us-east-1a.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "public-us-east-1b" {
  subnet_id      = aws_subnet.public-us-east-1b.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "k3s_cluster" {
  name        = "${var.prefix}-k3s"
  description = "Security group for K3s cluster"
  vpc_id      = aws_vpc.main.id

  // Allow internal communication within the VPC
  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["10.0.0.0/16"]
  }

  // K3s specific ports
  ingress {
    from_port   = 6443
    to_port     = 6443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 10250
    to_port     = 10250
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 51820
    to_port     = 51821
    protocol    = "udp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.prefix}-k3s-sg"
  }
}

# S3 Buckets
resource "aws_s3_bucket" "image_bucket" {
  bucket = "${var.prefix}-images"
}

resource "aws_s3_bucket" "juicefs_bucket" {
  bucket = "${var.prefix}-juicefs"
}

resource "aws_iam_role" "k3s_role" {
  name = "${var.prefix}-k3s-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
    ]
  })
}

resource "aws_iam_policy" "k3s_instance_policy" {
  name        = "${var.prefix}-k3s-instance"
  description = "Policy for allowing access to S3 & SSM"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ],
        Effect = "Allow",
        Resource = [
          "${aws_s3_bucket.image_bucket.arn}",
          "${aws_s3_bucket.image_bucket.arn}/*",
          "${aws_s3_bucket.juicefs_bucket.arn}",
          "${aws_s3_bucket.juicefs_bucket.arn}/*"
        ]
      },
      {
        Action   = ["ssm:PutParameter", "ssm:GetParameter"],
        Effect   = "Allow",
        Resource = "arn:aws:ssm:*:*:parameter/${var.prefix}/k3s/*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "iam:CreateServiceLinkedRole"
        ],
        "Resource" : "*",
        "Condition" : {
          "StringEquals" : {
            "iam:AWSServiceName" : "elasticloadbalancing.amazonaws.com"
          }
        }
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "ec2:DescribeAccountAttributes",
          "ec2:DescribeAddresses",
          "ec2:DescribeAvailabilityZones",
          "ec2:DescribeInternetGateways",
          "ec2:DescribeVpcs",
          "ec2:DescribeVpcPeeringConnections",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeInstances",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DescribeTags",
          "ec2:GetCoipPoolUsage",
          "ec2:DescribeCoipPools",
          "elasticloadbalancing:DescribeLoadBalancers",
          "elasticloadbalancing:DescribeLoadBalancerAttributes",
          "elasticloadbalancing:DescribeListeners",
          "elasticloadbalancing:DescribeListenerCertificates",
          "elasticloadbalancing:DescribeSSLPolicies",
          "elasticloadbalancing:DescribeRules",
          "elasticloadbalancing:DescribeTargetGroups",
          "elasticloadbalancing:DescribeTargetGroupAttributes",
          "elasticloadbalancing:DescribeTargetHealth",
          "elasticloadbalancing:DescribeTags",
          "elasticloadbalancing:AddTags"
        ],
        "Resource" : "*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "acm:ListCertificates",
          "acm:DescribeCertificate",
          "iam:ListServerCertificates",
          "iam:GetServerCertificate",
        ],
        "Resource" : "*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "ec2:AuthorizeSecurityGroupIngress",
          "ec2:RevokeSecurityGroupIngress"
        ],
        "Resource" : "*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "ec2:CreateSecurityGroup"
        ],
        "Resource" : "*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "ec2:CreateTags"
        ],
        "Resource" : "arn:aws:ec2:*:*:security-group/*",
        "Condition" : {
          "StringEquals" : {
            "ec2:CreateAction" : "CreateSecurityGroup"
          },
          "Null" : {
            "aws:RequestTag/elbv2.k8s.aws/cluster" : "false"
          }
        }
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "ec2:CreateTags",
          "ec2:DeleteTags"
        ],
        "Resource" : "arn:aws:ec2:*:*:security-group/*",
        "Condition" : {
          "Null" : {
            "aws:RequestTag/elbv2.k8s.aws/cluster" : "true",
            "aws:ResourceTag/elbv2.k8s.aws/cluster" : "false"
          }
        }
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "ec2:AuthorizeSecurityGroupIngress",
          "ec2:RevokeSecurityGroupIngress",
          "ec2:DeleteSecurityGroup"
        ],
        "Resource" : "*",
        "Condition" : {
          "Null" : {
            "aws:ResourceTag/elbv2.k8s.aws/cluster" : "false"
          }
        }
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "elasticloadbalancing:CreateLoadBalancer",
          "elasticloadbalancing:CreateTargetGroup"
        ],
        "Resource" : "*",
        "Condition" : {
          "Null" : {
            "aws:RequestTag/elbv2.k8s.aws/cluster" : "false"
          }
        }
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "elasticloadbalancing:CreateListener",
          "elasticloadbalancing:DeleteListener",
          "elasticloadbalancing:CreateRule",
          "elasticloadbalancing:DeleteRule"
        ],
        "Resource" : "*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "elasticloadbalancing:AddTags",
          "elasticloadbalancing:RemoveTags"
        ],
        "Resource" : [
          "arn:aws:elasticloadbalancing:*:*:targetgroup/*/*",
          "arn:aws:elasticloadbalancing:*:*:loadbalancer/net/*/*",
          "arn:aws:elasticloadbalancing:*:*:loadbalancer/app/*/*"
        ],
        "Condition" : {
          "Null" : {
            "aws:RequestTag/elbv2.k8s.aws/cluster" : "true",
            "aws:ResourceTag/elbv2.k8s.aws/cluster" : "false"
          }
        }
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "elasticloadbalancing:AddTags",
          "elasticloadbalancing:RemoveTags"
        ],
        "Resource" : [
          "arn:aws:elasticloadbalancing:*:*:listener/net/*/*/*",
          "arn:aws:elasticloadbalancing:*:*:listener/app/*/*/*",
          "arn:aws:elasticloadbalancing:*:*:listener-rule/net/*/*/*",
          "arn:aws:elasticloadbalancing:*:*:listener-rule/app/*/*/*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "elasticloadbalancing:ModifyLoadBalancerAttributes",
          "elasticloadbalancing:SetIpAddressType",
          "elasticloadbalancing:SetSecurityGroups",
          "elasticloadbalancing:SetSubnets",
          "elasticloadbalancing:DeleteLoadBalancer",
          "elasticloadbalancing:ModifyTargetGroup",
          "elasticloadbalancing:ModifyTargetGroupAttributes",
          "elasticloadbalancing:DeleteTargetGroup"
        ],
        "Resource" : "*",
        "Condition" : {
          "Null" : {
            "aws:ResourceTag/elbv2.k8s.aws/cluster" : "false"
          }
        }
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "elasticloadbalancing:RegisterTargets",
          "elasticloadbalancing:DeregisterTargets"
        ],
        "Resource" : "arn:aws:elasticloadbalancing:*:*:targetgroup/*/*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "elasticloadbalancing:SetWebAcl",
          "elasticloadbalancing:ModifyListener",
          "elasticloadbalancing:AddListenerCertificates",
          "elasticloadbalancing:RemoveListenerCertificates",
          "elasticloadbalancing:ModifyRule"
        ],
        "Resource" : "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "k3_instance_access_attachment" {
  role       = aws_iam_role.k3s_role.name
  policy_arn = aws_iam_policy.k3s_instance_policy.arn
  depends_on = [aws_iam_policy.k3s_instance_policy]
}

resource "aws_iam_instance_profile" "k3s_instance_profile" {
  name       = "${var.prefix}-k3s-instance-profile"
  role       = aws_iam_role.k3s_role.name
  depends_on = [aws_iam_role_policy_attachment.k3_instance_access_attachment]
}

# New Elastic IP for k3s_master
resource "aws_eip" "k3s_master_eip" {
  domain = "vpc"

  tags = {
    Name = "${var.prefix}-k3s-master-eip"
  }
}

data "aws_region" "this" {}

resource "aws_instance" "k3s_master" {
  ami                         = var.k3s_cluster_ami
  instance_type               = var.k3s_instance_type
  subnet_id                   = aws_subnet.public-us-east-1a.id
  security_groups             = [aws_security_group.k3s_cluster.id]
  iam_instance_profile        = aws_iam_instance_profile.k3s_instance_profile.name
  user_data_replace_on_change = true

  tags = {
    Name = "${var.prefix}-k3s-master"
  }

  user_data_base64 = base64encode(<<-EOF
    #!/bin/bash

    # Install wireguard
    apt update && apt install -y wireguard awscli wget
    aws configure set region ${data.aws_region.this.name}

    # Install yq for yaml parsing
    wget -qO /usr/local/bin/yq https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64
    chmod a+x /usr/local/bin/yq

    INSTANCE_ID=$(curl http://169.254.169.254/latest/meta-data/instance-id)
    REGION=$(curl http://169.254.169.254/latest/meta-data/placement/region)
    PROVIDER_ID="aws:///$REGION/$INSTANCE_ID"
    INSTALL_K3S_VERSION="v1.28.5+k3s1"

    # Install K3s with the Elastic IP in the certificate SAN
    curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=$INSTALL_K3S_VERSION INSTALL_K3S_EXEC="--tls-san ${aws_eip.k3s_master_eip.public_ip} --node-external-ip=${aws_eip.k3s_master_eip.public_ip} \
    --disable traefik --flannel-backend=wireguard-native --flannel-external-ip --kubelet-arg=provider-id=$PROVIDER_ID" sh -    

    # Wait for K3s to start, create the kubeconfig file, and the node token
    while [ ! -f /etc/rancher/k3s/k3s.yaml ] || [ ! -f /var/lib/rancher/k3s/server/node-token ]
    do
      sleep 1
    done

    # Update the kubeconfig file with the public IP
    sed -i "s/127.0.0.1/${aws_eip.k3s_master_eip.public_ip}/g" /etc/rancher/k3s/k3s.yaml

    # Extract CA certificate, client certificate, and key from kubeconfig
    ca_certificate=$(yq e '.clusters[0].cluster."certificate-authority-data"' /etc/rancher/k3s/k3s.yaml)
    client_certificate=$(yq e '.users[0].user."client-certificate-data"' /etc/rancher/k3s/k3s.yaml)
    client_key=$(yq e '.users[0].user."client-key-data"' /etc/rancher/k3s/k3s.yaml)

    # Write cluster config to AWS Parameter Store
    aws ssm put-parameter --name "/${var.prefix}/k3s/node-token" --type "String" --value "$(cat /var/lib/rancher/k3s/server/node-token)" --overwrite
    aws ssm put-parameter --name "/${var.prefix}/k3s/kubeconfig" --type "String" --value "$(cat /etc/rancher/k3s/k3s.yaml)" --overwrite
    aws ssm put-parameter --name "/${var.prefix}/k3s/ca-certificate" --type "String" --value "$ca_certificate" --overwrite
    aws ssm put-parameter --name "/${var.prefix}/k3s/client-certificate" --type "String" --value "$client_certificate" --overwrite
    aws ssm put-parameter --name "/${var.prefix}/k3s/client-key" --type "String" --value "$client_key" --overwrite

    EOF
  )

  lifecycle {
    ignore_changes = [
      security_groups,
    ]
  }

  depends_on = [aws_eip.k3s_master_eip, aws_iam_instance_profile.k3s_instance_profile]
}

# Association of EIP with k3s_master
resource "aws_eip_association" "eip_assoc_k3s_master" {
  instance_id   = aws_instance.k3s_master.id
  allocation_id = aws_eip.k3s_master_eip.id
  depends_on    = [aws_eip.k3s_master_eip]
}

resource "null_resource" "fetch_k3s_parameters" {
  triggers = {
    instance_id = aws_instance.k3s_master.id
  }

  provisioner "local-exec" {
    command = <<EOF
      success=0
      for i in {1..20}; do
        all_parameters_exist=true

        # List of SSM parameters to check
        params="kubeconfig node-token ca-certificate client-certificate client-key"

        # Check each parameter and break if any are missing
        for param in $params; do
          parameter_name="/${var.prefix}/k3s/$param"
          if ! aws ssm get-parameter --name "$parameter_name" --query 'Parameter.Value' --output text > "${path.module}/$param"; then
            echo "Waiting for SSM parameter '$parameter_name' to be available..."
            all_parameters_exist=false
            break
          fi
        done

        if [ "$all_parameters_exist" = true ]; then
          echo "All SSM parameters fetched successfully."
          success=1
          break
        fi

        echo "Attempt $i failed, retrying in 10 seconds..."
        sleep 10
      done

      if [ "$success" -ne 1 ]; then
        echo "Failed to fetch all SSM parameters after 20 attempts."
        exit 1
      fi
    EOF
  }

  depends_on = [aws_instance.k3s_master]
}


resource "aws_instance" "k3s_worker" {
  count                       = var.k3s_worker_count
  ami                         = var.k3s_cluster_ami
  instance_type               = var.k3s_instance_type
  subnet_id                   = aws_subnet.private-us-east-1a.id
  security_groups             = [aws_security_group.k3s_cluster.id]
  iam_instance_profile        = aws_iam_instance_profile.k3s_instance_profile.name
  user_data_replace_on_change = true

  tags = {
    Name = "${var.prefix}-k3s-worker-${count.index}"
  }

  user_data_base64 = base64encode(<<-EOF
    #!/bin/bash

    # Install wireguard
    apt update && apt install -y wireguard awscli
    aws configure set region ${data.aws_region.this.name}

    INSTALL_K3S_VERSION="v1.28.5+k3s1"
    INSTANCE_ID=$(curl http://169.254.169.254/latest/meta-data/instance-id)
    REGION=$(curl http://169.254.169.254/latest/meta-data/placement/region)
    PROVIDER_ID="aws:///$REGION/$INSTANCE_ID"
    KUBELET_ARG="--kubelet-arg=provider-id=$PROVIDER_ID"
    MASTER=https://${aws_eip.k3s_master_eip.public_ip}:6443
    TOKEN=$(aws ssm get-parameter --name "/${var.prefix}/k3s/node-token" --query "Parameter.Value" --output text)
    curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC=$KUBELET_ARG K3S_URL=$MASTER K3S_TOKEN=$TOKEN INSTALL_K3S_VERSION=$INSTALL_K3S_VERSION sh -
    EOF
  )

  lifecycle {
    ignore_changes = [
      security_groups,
    ]
  }

  depends_on = [aws_instance.k3s_master, null_resource.fetch_k3s_parameters, aws_nat_gateway.nat]
}

data "aws_ssm_parameter" "kubeconfig" {
  name       = "/${var.prefix}/k3s/kubeconfig"
  depends_on = [null_resource.fetch_k3s_parameters]
}

data "aws_ssm_parameter" "ca_certificate" {
  name       = "/${var.prefix}/k3s/ca-certificate"
  depends_on = [null_resource.fetch_k3s_parameters]
}

data "aws_ssm_parameter" "client_certificate" {
  name       = "/${var.prefix}/k3s/client-certificate"
  depends_on = [null_resource.fetch_k3s_parameters]
}

data "aws_ssm_parameter" "client_key" {
  name       = "/${var.prefix}/k3s/client-key"
  depends_on = [null_resource.fetch_k3s_parameters]
}

# Postgres (RDS)
resource "aws_db_subnet_group" "default" {
  name       = var.prefix
  subnet_ids = [aws_subnet.private-us-east-1a.id, aws_subnet.private-us-east-1b.id]

  tags = {
    Name = "${var.prefix}"
  }
}

resource "aws_security_group" "rds_sg" {
  name        = var.prefix
  description = "Security group for RDS PostgreSQL instance"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.prefix}-rds-sg"
  }
}


resource "aws_db_instance" "postgres_db" {
  identifier                  = var.prefix
  engine                      = "postgres"
  engine_version              = "13.8"
  instance_class              = "db.t4g.medium"
  allocated_storage           = 20
  storage_type                = "gp2"
  username                    = "postgres"
  manage_master_user_password = true
  db_name                     = "main"
  skip_final_snapshot         = true
  db_subnet_group_name        = aws_db_subnet_group.default.name
  vpc_security_group_ids      = [aws_security_group.rds_sg.id]

  depends_on = [aws_db_subnet_group.default]
}

data "aws_secretsmanager_secret" "rds_secret" {
  arn = aws_db_instance.postgres_db.master_user_secret[0].secret_arn
}

data "aws_secretsmanager_secret_version" "current" {
  secret_id = data.aws_secretsmanager_secret.rds_secret.id
}

resource "aws_iam_user" "bucket_user" {
  name = "${var.prefix}-bucket-user"
}

resource "aws_iam_policy" "bucket_access_policy" {
  name        = "${var.prefix}-juicefs-images-bucket-access"
  description = "Policy for JuiceFS and images buckets"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads"
        ],
        Effect = "Allow",
        Resource = [
          aws_s3_bucket.juicefs_bucket.arn,
          aws_s3_bucket.image_bucket.arn
        ]
      },
      {
        Action = [
          "s3:GetObject",
          "s3:HeadObject",
          "s3:PutObject",
          "s3:DeleteObject",
        ],
        Effect = "Allow",
        Resource = [
          "${aws_s3_bucket.juicefs_bucket.arn}/*",
          "${aws_s3_bucket.image_bucket.arn}/*"
        ]
      }
    ]
  })
}


resource "aws_iam_user_policy_attachment" "bucket_access_attachment" {
  user       = aws_iam_user.bucket_user.name
  policy_arn = aws_iam_policy.bucket_access_policy.arn
}

resource "aws_iam_access_key" "bucket_user_key" {
  user = aws_iam_user.bucket_user.name
}
