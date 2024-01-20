provider "helm" {
  kubernetes {
    host                   = var.cluster_endpoint
    cluster_ca_certificate = base64decode(var.cluster_ca_certificate)
    client_certificate     = base64decode(var.cluster_client_certificate)
    client_key             = base64decode(var.cluster_client_key)
  }
}


resource "helm_release" "aws_lb_controller" {
  name       = "aws-load-balancer-controller"
  repository = "https://aws.github.io/eks-charts"
  chart      = "aws-load-balancer-controller"
  namespace  = "kube-system"

  set {
    name  = "clusterName"
    value = var.cluster_name
  }

  depends_on = [var.cluster_ca_certificate]
}


resource "helm_release" "nginx_ingress" {
  name       = "nginx-ingress"
  chart      = "nginx-ingress-controller"
  repository = "https://charts.bitnami.com/bitnami"
  namespace  = "kube-system"

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
          service.beta.kubernetes.io/aws-load-balancer-type: "external"
          service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: "ip"
          service.beta.kubernetes.io/aws-load-balancer-scheme: "internet-facing"
          service.beta.kubernetes.io/aws-load-balancer-backend-protocol: "tcp"
        targetPorts:
          http: http
          https: https
      ingressClass: nginx-public
      ingressClassResource:
        name: nginx-public
        controllerValue: k8s.io/ingress-nginx-public
      config:
        enable-access-log-for-default-backend: "true"
        enable-real-ip: "true"
        use-forwarded-headers: "true"
        use-proxy-protocol: "true"
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
        log-format-upstream: |
          {"time_iso8601": "$time_iso8601", "http_x_forwarded_for": "$http_x_forwarded_for", "http_x_ingress_token": "$http_x_ingress_token", "remote_addr": "$remote_addr", "remote_user": "$remote_user", "request_id": "$req_id", "request_method": "$request_method", "request_length": "$request_length", "request_time": "$request_time", "request_uri": "$request_uri", "query_string": "$query_string", "server_protocol": "$server_protocol", "host": "$host", "status": "$status", "bytes_sent": "$bytes_sent", "body_bytes_sent": "$body_bytes_sent", "http_referer": "$http_referer", "http_user_agent": "$http_user_agent", "proxy_upstream_name": "$proxy_upstream_name", "proxy_alternative_upstream_name": "$proxy_alternative_upstream_name", "upstream_addr": "$upstream_addr", "upstream_response_length": "$upstream_response_length", "upstream_response_time": "$upstream_response_time", "upstream_bytes_received": "$upstream_bytes_received", "upstream_bytes_sent": "$upstream_bytes_sent", "upstream_status": "$upstream_status"}
    defaultBackend:
      enabled: true
    EOF
  ]

  depends_on = [helm_release.aws_lb_controller]
}