# output "kubeconfig" {
#   value = <<KUBECONFIG
# apiVersion: v1
# clusters:
# - cluster:
#     server: ${data.aws_eks_cluster.cluster.endpoint}
#     certificate-authority-data: ${data.aws_eks_cluster.cluster.certificate_authority[0].data}
#   name: ${aws_eks_cluster.cluster.name}
# contexts:
# - context:
#     cluster: ${aws_eks_cluster.cluster.name}
#     user: ${aws_eks_cluster.cluster.name}
#   name: ${aws_eks_cluster.cluster.name}
# current-context: ${aws_eks_cluster.cluster.name}
# kind: Config
# preferences: {}
# users:
# - name: ${aws_eks_cluster.cluster.name}
#   user:
#     exec:
#       apiVersion: client.authentication.k8s.io/v1beta1
#       command: aws-iam-authenticator
#       args:
#         - "token"
#         - "-i"
#         - "${aws_eks_cluster.cluster.name}"
# KUBECONFIG
# }