module "cluster" {
  source = "./modules/cluster"

  prefix = var.prefix
}

module "k8s_resources" {
  source = "./modules/k8s-resources"

  domain                  = var.domain
  domain_hosted_zone_id   = var.domain_hosted_zone_id
  prefix                  = var.prefix
  aws_region              = var.aws_region
  k3s_cluster_config      = module.cluster.k3s_cluster_config
  vpc_config              = module.cluster.vpc_config
  db_config               = module.cluster.db_config
  s3_buckets              = module.cluster.s3_buckets
  bucket_user_credentials = module.cluster.bucket_user_credentials
}
