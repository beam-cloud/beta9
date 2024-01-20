# Install kubernetes resources:
#  nginx
#  AWS load balancer controller
provider "helm" {
  kubernetes {
    host                   = var.cluster_endpoint
    cluster_ca_certificate = base64decode(var.cluster_ca_certificate)
    client_certificate     = base64decode(var.cluster_client_certificate)
    client_key             = base64decode(var.cluster_client_key)
  }
}

resource "helm_release" "nginx_ingress" {
  name       = "nginx-ingress"
  repository = "https://helm.nginx.com/stable"
  chart      = "nginx-ingress"
  namespace  = "kube-system"

  set {
    name  = "controller.service.type"
    value = "LoadBalancer"
  }

  depends_on = [var.cluster_endpoint]
}
