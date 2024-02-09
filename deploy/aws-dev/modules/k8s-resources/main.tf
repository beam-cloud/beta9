
provider "kubernetes" {
  host                   = var.k3s_cluster_config.endpoint.value
  cluster_ca_certificate = base64decode(var.k3s_cluster_config.ca_certificate.value)
  client_certificate     = base64decode(var.k3s_cluster_config.client_certificate.value)
  client_key             = base64decode(var.k3s_cluster_config.client_key.value)
}


provider "helm" {
  kubernetes {
    host                   = var.k3s_cluster_config.endpoint.value
    cluster_ca_certificate = base64decode(var.k3s_cluster_config.ca_certificate.value)
    client_certificate     = base64decode(var.k3s_cluster_config.client_certificate.value)
    client_key             = base64decode(var.k3s_cluster_config.client_key.value)
  }
}

# SSL Certificate for the service exposed
resource "aws_acm_certificate" "ssl_cert" {
  domain_name               = var.domain
  validation_method         = "DNS"
  subject_alternative_names = ["*.${var.domain}"]

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
    } if dvo.domain_name == var.domain
  }

  zone_id = var.domain_hosted_zone_id
  name    = each.value.name
  type    = each.value.type
  ttl     = 60
  records = [each.value.record]
}

resource "helm_release" "aws_lb_controller" {
  name       = "aws-load-balancer-controller"
  repository = "https://aws.github.io/eks-charts"
  chart      = "aws-load-balancer-controller"
  namespace  = "kube-system"
  version    = "1.4.7"

  set {
    name  = "clusterName"
    value = var.k3s_cluster_config.cluster_name.value
  }

  set {
    name  = "vpcId"
    value = var.vpc_config.vpc_id.value
  }

  depends_on = [var.k3s_cluster_config]
}


resource "helm_release" "nginx_ingress" {
  name       = "ingress-nginx"
  chart      = "ingress-nginx"
  repository = "https://kubernetes.github.io/ingress-nginx"
  namespace  = "kube-system"
  version    = "4.6.0"

  values = [
    <<-EOF
    controller:
      replicaCount: 2
      minAvailable: 1
      service:
        enabled: true
        enableHttp: true
        enableHttps: true
        annotations:
          service.beta.kubernetes.io/aws-load-balancer-name: ${var.prefix}-ingress
          service.beta.kubernetes.io/aws-load-balancer-type: external
          service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: instance
          service.beta.kubernetes.io/aws-load-balancer-scheme: internet-facing
          service.beta.kubernetes.io/aws-load-balancer-backend-protocol: http
          service.beta.kubernetes.io/aws-load-balancer-subnets: ${var.vpc_config.public_subnets.value}
          service.beta.kubernetes.io/aws-load-balancer-ssl-cert: "${aws_acm_certificate.ssl_cert.arn}"
          service.beta.kubernetes.io/aws-load-balancer-ssl-ports: "https"
          service.beta.kubernetes.io/aws-load-balancer-ssl-negotiation-policy: "ELBSecurityPolicy-TLS13-1-2-2021-06"
        targetPorts:
          http: http
          https: http
      ingressClass: nginx-public
      ingressClassResource:
        name: nginx-public
        controllerValue: k8s.io/ingress-nginx-public
      config:
        enable-access-log-for-default-backend: "true"
        enable-real-ip: "true"
        use-forwarded-headers: "true"
        use-gzip: "true"
        gzip-level: 9
        enable-brotli: "true"
        brotli-level: 11
        server-tokens: "false"
        ssl-redirect: "true"
        ssl-ciphers: EECDH+AESGCM:EDH+AESGCM:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-CHACHA20-POLY1305
        ssl-protocols: TLSv1.2 TLSv1.3
        ssl-session-cache: "true"
        ssl-session-cache-size: "10m"
        ssl-session-tickets: "true"
        ssl-reject-handshake: "true"
        enable-ocsp: "true"
        log-format-escape-json: "true"
    defaultBackend:
      enabled: true
    EOF
  ]

  depends_on = [helm_release.aws_lb_controller]
}


resource "random_password" "redis_password" {
  length  = 16
  special = false
}

resource "random_password" "juicefs_redis_password" {
  length  = 16
  special = false
}

resource "helm_release" "redis" {
  name             = "redis"
  repository       = "https://charts.bitnami.com/bitnami"
  chart            = "redis"
  version          = "18.7.1"
  namespace        = "beta9"
  create_namespace = true

  set {
    name  = "architecture"
    value = "standalone"
  }

  set {
    name  = "master.persistence.size"
    value = "5Gi"
  }

  set {
    name  = "auth.password"
    value = random_password.redis_password.result
  }

  depends_on = [helm_release.nginx_ingress]
}


resource "helm_release" "juicefs_redis" {
  name             = "juicefs-redis"
  repository       = "https://charts.bitnami.com/bitnami"
  chart            = "redis"
  version          = "18.7.1"
  namespace        = "beta9"
  create_namespace = true

  set {
    name  = "architecture"
    value = "standalone"
  }

  set {
    name  = "master.persistence.size"
    value = "5Gi"
  }

  set {
    name  = "auth.password"
    value = random_password.juicefs_redis_password.result
  }

  depends_on = [helm_release.nginx_ingress]
}

locals {
  config_content = templatefile("${path.module}/config.tpl", {
    db_user                = var.db_config.username.value
    db_host                = var.db_config.host.value
    db_password            = var.db_config.password.value
    redis_password         = random_password.redis_password.result
    juicefs_redis_password = random_password.juicefs_redis_password.result
    juicefs_bucket         = "https://${var.s3_buckets.juicefs_bucket_name}.s3.amazonaws.com"
    aws_access_key_id      = var.bucket_user_credentials.access_key
    aws_secret_access_key  = var.bucket_user_credentials.secret_key
    images_bucket          = var.s3_buckets.image_bucket_name
    aws_region             = var.aws_region
  })
}

resource "kubernetes_secret" "app_config" {
  metadata {
    name = "app-config"
  }

  data = {
    "config.yml" = local.config_content
  }

  depends_on = [
    random_password.juicefs_redis_password,
    random_password.redis_password,
    helm_release.nginx_ingress
  ]
}
