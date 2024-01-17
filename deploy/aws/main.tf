variable "prefix" {
  description = "Global prefix for all resources"
  type        = string
  default     = "beamtest" # Set your desired prefix here
}

provider "aws" {
  region = "us-east-1" # Choose your AWS region
}

# VPC
resource "aws_vpc" "vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true

  tags = {
    Name = "${var.prefix}-vpc"
  }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.vpc.id

  tags = {
    Name = "${var.prefix}-igw"
  }
}

# EKS Subnets
resource "aws_subnet" "eks_subnet_1" {
  vpc_id                  = aws_vpc.vpc.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.prefix}-eks-subnet1"
  }
}

resource "aws_subnet" "eks_subnet_2" {
  vpc_id                  = aws_vpc.vpc.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "us-east-1b"
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.prefix}-eks-subnet2"
  }
}

resource "aws_route_table" "route_table" {
  vpc_id = aws_vpc.vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "${var.prefix}-route-table"
  }
}

resource "aws_route_table_association" "a" {
  subnet_id      = aws_subnet.eks_subnet_1.id
  route_table_id = aws_route_table.route_table.id
}

resource "aws_route_table_association" "b" {
  subnet_id      = aws_subnet.eks_subnet_2.id
  route_table_id = aws_route_table.route_table.id
}

# EKS Cluster
module "eks" {
  source          = "terraform-aws-modules/eks/aws"
  version         = "17.1.0"
  cluster_name    = "${var.prefix}-beam-cluster"
  cluster_version = "1.28"
  subnets         = [aws_subnet.eks_subnet_1.id, aws_subnet.eks_subnet_2.id]
  vpc_id          = aws_vpc.vpc.id

  node_groups = {
    default = {
      desired_capacity = 2
      max_capacity     = 3
      min_capacity     = 1

      instance_type = "m5.large"
    }
  }
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  token                  = data.aws_eks_cluster_auth.cluster.token
}

data "aws_eks_cluster" "cluster" {
  name = module.eks.cluster_id
}

data "aws_eks_cluster_auth" "cluster" {
  name = module.eks.cluster_id
}


# SSL Certificate for the service exposed
resource "aws_acm_certificate" "ssl_cert" {
  domain_name       = "eng-stage.slai.io"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

# Certificate validation
resource "aws_acm_certificate_validation" "ssl_cert" {
  certificate_arn         = aws_acm_certificate.ssl_cert.arn
  validation_record_fqdns = [for record in aws_route53_record.cert_validation : record.fqdn]
}

# DNS records for certificate validation
resource "aws_route53_record" "cert_validation" {
  for_each = {
    for dvo in aws_acm_certificate.ssl_cert.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  }

  zone_id = "Z07081541B2HAA9KWC78W" # aws_route53_zone.eng_stage.zone_id
  name    = each.value.name
  type    = each.value.type
  ttl     = 60
  records = [each.value.record]
}

# S3 Buckets
resource "aws_s3_bucket" "image_bucket" {
  bucket = "${var.prefix}-image-bucket"
}

resource "aws_s3_bucket" "juicefs_bucket" {
  bucket = "${var.prefix}-juicefs-bucket"
}

# Redis (ElastiCache)
resource "aws_subnet" "elasticache_subnet_1" {
  vpc_id                  = aws_vpc.vpc.id
  cidr_block              = "10.0.5.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.prefix}-elasticache-subnet1"
  }
}

resource "aws_elasticache_subnet_group" "elasticache_subnet_group" {
  name       = "${var.prefix}-elasticache-subnet-group"
  subnet_ids = [aws_subnet.elasticache_subnet_1.id]

  tags = {
    Name = "${var.prefix}-elasticache-subnet-group"
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
  subnet_group_name    = aws_elasticache_subnet_group.elasticache_subnet_group.name
}

# PostgreSQL (RDS)
resource "aws_subnet" "rds_subnet_1" {
  vpc_id                  = aws_vpc.vpc.id
  cidr_block              = "10.0.3.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.prefix}-rds-subnet1"
  }
}

resource "aws_subnet" "rds_subnet_2" {
  vpc_id                  = aws_vpc.vpc.id
  cidr_block              = "10.0.4.0/24"
  availability_zone       = "us-east-1b"
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.prefix}-rds-subnet2"
  }
}

resource "aws_db_subnet_group" "db_subnet_group" {
  name       = "${var.prefix}-db-subnet-group"
  subnet_ids = [aws_subnet.rds_subnet_1.id, aws_subnet.rds_subnet_2.id]

  tags = {
    Name = "${var.prefix}-db-subnet-group"
  }
}

resource "aws_db_instance" "postgres_db" {
  identifier           = "${var.prefix}-postgres"
  engine               = "postgres"
  engine_version       = "13.8"
  db_subnet_group_name = aws_db_subnet_group.db_subnet_group.name
  instance_class       = "db.t4g.medium"
  allocated_storage    = 20
  storage_type         = "gp2"
  username             = "postgres"
  password             = "password"
  db_name              = "main"
  skip_final_snapshot  = true
}


# K8s resources

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    token                  = data.aws_eks_cluster_auth.cluster.token
  }
}

resource "helm_release" "nginx_ingress" {
  name       = "nginx-ingress"
  repository = "https://kubernetes.github.io/ingress-nginx"
  chart      = "ingress-nginx"
  version    = "4.9.0"

  set {
    name  = "controller.service.type"
    value = "LoadBalancer"
  }
}


resource "helm_release" "aws_load_balancer_controller" {
  name       = "aws-load-balancer-controller"
  repository = "https://aws.github.io/eks-charts"
  chart      = "aws-load-balancer-controller"
  version    = "1.6.2"
  replace    = true

  namespace = "kube-system"

  set {
    name  = "clusterName"
    value = module.eks.cluster_id
  }
}

resource "kubernetes_ingress" "nginx-ingress" {
  metadata {
    name = "nginx-ingress"
    annotations = {
      "kubernetes.io/ingress.class"                                   = "nginx"
      "nginx.ingress.kubernetes.io/ssl-redirect"                      = "true"
      "nginx.ingress.kubernetes.io/use-regex"                         = "true"
      "nginx.ingress.kubernetes.io/backend-protocol"                  = "HTTP"
      "external-dns.alpha.kubernetes.io/hostname"                     = "eng-stage.slai.io"
      "service.beta.kubernetes.io/aws-load-balancer-ssl-cert"         = aws_acm_certificate.ssl_cert.arn
      "service.beta.kubernetes.io/aws-load-balancer-backend-protocol" = "http"
      "service.beta.kubernetes.io/aws-load-balancer-ssl-ports"        = "443"
    }
  }

  spec {
    tls {
      hosts       = ["eng-stage.slai.io"]
      secret_name = "dummy" # This can be a dummy secret as actual TLS termination will be on the LB
    }

    rule {
      host = "eng-stage.slai.io"
      http {
        path {
          backend {
            service_name = "your-service-name"
            service_port = 80
          }
          path = "/"
        }
      }
    }
  }
}

output "kubeconfig" {
  value = module.eks.kubeconfig
}