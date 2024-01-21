provider "helm" {
  kubernetes {
    host                   = var.cluster_endpoint
    cluster_ca_certificate = base64decode(var.cluster_ca_certificate)
    client_certificate     = base64decode(var.cluster_client_certificate)
    client_key             = base64decode(var.cluster_client_key)
  }
}


# SSL Certificate for the service exposed
resource "aws_acm_certificate" "ssl_cert" {
  domain_name       = var.domain
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
    value = var.cluster_name
  }

  set {
    name  = "vpcId"
    value = var.vpc_id
  }

  depends_on = [var.cluster_ca_certificate]
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
          service.beta.kubernetes.io/aws-load-balancer-subnets: ${var.public_subnets}
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
