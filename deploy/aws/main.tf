module "cluster" {
  source = "./modules/cluster"
}

module "k8s_resources" {
  source = "./modules/k8s-resources"

  cluster_name               = module.cluster.cluster_name
  cluster_endpoint           = module.cluster.endpoint
  cluster_ca_certificate     = module.cluster.ca_certificate
  cluster_client_key         = module.cluster.client_key
  cluster_client_certificate = module.cluster.client_certificate
}