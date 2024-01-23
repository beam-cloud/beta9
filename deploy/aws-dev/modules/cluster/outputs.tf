output "k3s_cluster_config" {
  value = {
    endpoint = {
      description = "K8S API server"
      value       = "https://${aws_eip.k3s_master_eip.public_ip}:6443"
    }

    ca_certificate = {
      description = "The CA Certificate for the K3s cluster"
      value       = data.aws_ssm_parameter.ca_certificate.value
    }

    client_certificate = {
      description = "The client certificate for the K3s cluster"
      value       = data.aws_ssm_parameter.client_certificate.value
    }

    client_key = {
      description = "The client key for the K3s cluster"
      value       = data.aws_ssm_parameter.client_key.value
    }

    cluster_name = {
      description = "The k3s cluster name"
      value       = var.k3s_cluster_name
    }
  }
}

output "vpc_config" {
  value = {

    vpc_id = {
      description = "VPC ID"
      value       = aws_vpc.main.id
    }

    public_subnets = {
      description = "Public subnets"
      value       = "${aws_subnet.public-us-east-1a.id},${aws_subnet.public-us-east-1b.id}"
    }

  }
}

output "db_config" {
  value = {

    host = {
      value = "${aws_db_instance.postgres_db.address}"
    }

    username = {
      value = jsondecode(data.aws_secretsmanager_secret_version.current.secret_string)["username"]
    }

    password = {
      value = jsondecode(data.aws_secretsmanager_secret_version.current.secret_string)["password"]
    }
  }

  description = "Postgres database config"
  sensitive   = true
}


output "bucket_user_credentials" {
  value = {
    access_key = aws_iam_access_key.bucket_user_key.id
    secret_key = aws_iam_access_key.bucket_user_key.secret
  }

  sensitive = true
}

output "s3_buckets" {
  value = {
    image_bucket_name   = aws_s3_bucket.image_bucket.bucket
    juicefs_bucket_name = aws_s3_bucket.juicefs_bucket.bucket
  }
}
