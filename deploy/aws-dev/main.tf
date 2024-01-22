module "cluster" {
  source = "./modules/cluster"

  prefix = var.prefix
}

module "k8s_resources" {
  source = "./modules/k8s-resources"

  domain                     = var.domain
  domain_hosted_zone_id      = var.domain_hosted_zone_id
  prefix                     = var.prefix
  cluster_name               = module.cluster.cluster_name
  cluster_endpoint           = module.cluster.endpoint
  cluster_ca_certificate     = module.cluster.ca_certificate
  cluster_client_key         = module.cluster.client_key
  cluster_client_certificate = module.cluster.client_certificate
  vpc_id                     = module.cluster.vpc_id
  public_subnets             = module.cluster.public_subnets
}