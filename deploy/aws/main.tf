# VPC
resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"

  enable_dns_hostnames = true

  tags = {
    Name = "${var.prefix}-vpc"
  }
}

# Internet gateway
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.prefix}-igw"
  }
}

# Subnets
resource "aws_subnet" "private-us-east-1a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.0.0/19"
  availability_zone = "us-east-1a"

  tags = {
    "Name"                                      = "${var.prefix}-private-us-east-1a"
    "kubernetes.io/role/internal-elb"           = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "owned"
  }
}

resource "aws_subnet" "private-us-east-1b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.32.0/19"
  availability_zone = "us-east-1b"

  tags = {
    "Name"                                      = "${var.prefix}-private-us-east-1b"
    "kubernetes.io/role/internal-elb"           = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "owned"
  }
}

resource "aws_subnet" "public-us-east-1a" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.64.0/19"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true

  tags = {
    "Name"                                      = "${var.prefix}-public-us-east-1a"
    "kubernetes.io/role/elb"                    = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "owned"
  }
}

resource "aws_subnet" "public-us-east-1b" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.96.0/19"
  availability_zone       = "us-east-1b"
  map_public_ip_on_launch = true

  tags = {
    "Name"                                      = "${var.prefix}-public-us-east-1b"
    "kubernetes.io/role/elb"                    = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "owned"
  }
}

# Elastic IP
resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "${var.prefix}-nat"
  }
}

# NAT Gateway
resource "aws_nat_gateway" "nat" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public-us-east-1a.id

  tags = {
    Name = "${var.prefix}-nat"
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
  name        = "${var.prefix}-k3s-sg"
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
  bucket = "${var.prefix}-image-bucket"
}

resource "aws_s3_bucket" "juicefs_bucket" {
  bucket = "${var.prefix}-juicefs-bucket"
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

resource "aws_instance" "k3s_master" {
  ami                         = var.k3s_cluster_ami
  instance_type               = "t3.small"
  subnet_id                   = aws_subnet.public-us-east-1a.id
  security_groups             = [aws_security_group.k3s_cluster.id]
  iam_instance_profile        = aws_iam_instance_profile.k3s_instance_profile.name
  user_data_replace_on_change = true

  tags = {
    Name = "${var.prefix}-k3s-master"
  }

  user_data_base64 = base64encode(<<-EOF
    #!/bin/bash

    # Install K3s with the Elastic IP in the certificate SAN
    curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--tls-san ${aws_eip.k3s_master_eip.public_ip}" sh -

    # Wait for K3s to start, create the kubeconfig file, and the node token
    while [ ! -f /etc/rancher/k3s/k3s.yaml ] || [ ! -f /var/lib/rancher/k3s/server/node-token ]
    do
      sleep 1
    done

    # Update the kubeconfig file with the public IP
    sed -i "s/127.0.0.1/${aws_eip.k3s_master_eip.public_ip}/g" /etc/rancher/k3s/k3s.yaml

    # Write cluster config to AWS Parameter Store
    aws ssm put-parameter --name "/${var.prefix}/k3s/node-token" --type "String" --value "$(cat /var/lib/rancher/k3s/server/node-token)" --overwrite
    aws ssm put-parameter --name "/${var.prefix}/k3s/kubeconfig" --type "String" --value "$(cat /etc/rancher/k3s/k3s.yaml)" --overwrite
    EOF
  )

  depends_on = [aws_eip.k3s_master_eip, aws_iam_instance_profile.k3s_instance_profile]
}

# Association of EIP with k3s_master
resource "aws_eip_association" "eip_assoc_k3s_master" {
  instance_id   = aws_instance.k3s_master.id
  allocation_id = aws_eip.k3s_master_eip.id
  depends_on    = [aws_eip.k3s_master_eip]
}

resource "null_resource" "fetch_k3s_kubeconfig" {
  triggers = {
    instance_id = aws_instance.k3s_master.id
  }

  provisioner "local-exec" {
    command = <<EOF
      success=0
      for i in {1..10}; do
        if aws ssm get-parameter --name '/${var.prefix}/k3s/kubeconfig' --query 'Parameter.Value' --output text > ${path.module}/kubeconfig.yaml; then
          echo "SSM parameter fetched successfully."
          success=1
          break
        fi
        echo "Attempt $i failed, retrying in 10 seconds..."
        sleep 10
      done
      if [ "$success" -ne 1 ]; then
        echo "Failed to fetch SSM parameter after 5 attempts."
        exit 1
      fi
    EOF
  }

  depends_on = [aws_instance.k3s_master]
}

resource "null_resource" "fetch_k3s_nodetoken" {
  triggers = {
    instance_id = aws_instance.k3s_master.id
  }

  provisioner "local-exec" {
    command = <<EOF
      success=0
      for i in {1..10}; do
        if aws ssm get-parameter --name '/${var.prefix}/k3s/node-token' --query 'Parameter.Value' --output text > ${path.module}/nodetoken; then
          echo "SSM parameter fetched successfully."
          success=1
          break
        fi
        echo "Attempt $i failed, retrying in 10 seconds..."
        sleep 10
      done
      if [ "$success" -ne 1 ]; then
        echo "Failed to fetch SSM parameter after 5 attempts."
        exit 1
      fi
    EOF
  }

  depends_on = [aws_instance.k3s_master]
}


resource "aws_instance" "k3s_worker" {
  count                       = 2
  ami                         = var.k3s_cluster_ami
  instance_type               = "t3.small"
  subnet_id                   = aws_subnet.private-us-east-1a.id
  security_groups             = [aws_security_group.k3s_cluster.id]
  iam_instance_profile        = aws_iam_instance_profile.k3s_instance_profile.name
  user_data_replace_on_change = true

  tags = {
    Name = "${var.prefix}-k3s-worker-${count.index}"
  }

  user_data_base64 = base64encode(<<-EOF
    #!/bin/bash
    MASTER=https://${aws_eip.k3s_master_eip.public_ip}:6443
    TOKEN=$(aws ssm get-parameter --name "/${var.prefix}/k3s/node-token" --query "Parameter.Value" --output text)
    curl -sfL https://get.k3s.io | K3S_URL=$MASTER K3S_TOKEN=$TOKEN sh -
    EOF
  )

  depends_on = [aws_instance.k3s_master, null_resource.fetch_k3s_nodetoken, aws_nat_gateway.nat]
}

# Postgres (RDS)

resource "aws_db_subnet_group" "default" {
  name       = "main"
  subnet_ids = [aws_subnet.private-us-east-1a.id, aws_subnet.private-us-east-1b.id]

  tags = {
    Name = "${var.prefix}-rds-subnet-group"
  }
}

resource "aws_db_instance" "postgres_db" {
  identifier           = "${var.prefix}-postgres"
  engine               = "postgres"
  engine_version       = "13.8"
  db_subnet_group_name = aws_db_subnet_group.default.name
  instance_class       = "db.t4g.medium"
  allocated_storage    = 20
  storage_type         = "gp2"
  username             = "postgres"
  password             = "password"
  db_name              = "main"
  skip_final_snapshot  = true

  depends_on = [aws_db_subnet_group.default]
}

# Redis (ElastiCache)

resource "aws_elasticache_subnet_group" "private_subnet_group" {
  name       = "${var.prefix}-private-subnet-group"
  subnet_ids = [aws_subnet.private-us-east-1a.id, aws_subnet.private-us-east-1b.id]

  tags = {
    Name = "${var.prefix}-private-subnet-group"
  }
}

resource "aws_elasticache_cluster" "redis_cluster" {
  cluster_id           = "${var.prefix}-redis"
  engine               = "redis"
  node_type            = "cache.t4g.micro"
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  engine_version       = "7.0"
  port                 = 6379
  subnet_group_name    = aws_elasticache_subnet_group.private_subnet_group.name
}