"""AWS infrastructure stack using CDKTF."""

from typing import Dict, Any

from cdktf import TerraformStack, TerraformOutput, S3Backend
from constructs import Construct

# AWS Provider imports
from cdktf_cdktf_provider_aws.provider import AwsProvider
from cdktf_cdktf_provider_aws.vpc import Vpc
from cdktf_cdktf_provider_aws.subnet import Subnet
from cdktf_cdktf_provider_aws.internet_gateway import InternetGateway
from cdktf_cdktf_provider_aws.nat_gateway import NatGateway
from cdktf_cdktf_provider_aws.eip import Eip
from cdktf_cdktf_provider_aws.route_table import RouteTable, RouteTableRoute
from cdktf_cdktf_provider_aws.route_table_association import RouteTableAssociation
from cdktf_cdktf_provider_aws.security_group import SecurityGroup, SecurityGroupIngress, SecurityGroupEgress
from cdktf_cdktf_provider_aws.eks_cluster import EksCluster, EksClusterVpcConfig
from cdktf_cdktf_provider_aws.eks_node_group import EksNodeGroup, EksNodeGroupScalingConfig
from cdktf_cdktf_provider_aws.db_instance import DbInstance
from cdktf_cdktf_provider_aws.db_subnet_group import DbSubnetGroup
from cdktf_cdktf_provider_aws.elasticache_cluster import ElasticacheCluster
from cdktf_cdktf_provider_aws.elasticache_subnet_group import ElasticacheSubnetGroup
from cdktf_cdktf_provider_aws.s3_bucket import S3Bucket
from cdktf_cdktf_provider_aws.s3_bucket_versioning import S3BucketVersioningA
from cdktf_cdktf_provider_aws.s3_bucket_lifecycle_configuration import (
    S3BucketLifecycleConfiguration,
    S3BucketLifecycleConfigurationRule,
    S3BucketLifecycleConfigurationRuleTransition,
)
from cdktf_cdktf_provider_aws.iam_role import IamRole
from cdktf_cdktf_provider_aws.iam_role_policy_attachment import IamRolePolicyAttachment

from models.config import InfraModel


class Beta9AwsStack(TerraformStack):
    """
    AWS infrastructure stack for Beta9.

    Creates:
    - VPC with public/private subnets
    - EKS cluster with node groups
    - RDS PostgreSQL instance
    - ElastiCache Redis cluster
    - S3 bucket for object storage
    """

    def __init__(self, scope: Construct, id: str, infra: InfraModel):
        """
        Initialize AWS stack.

        Args:
            scope: CDKTF construct scope
            id: Stack identifier
            infra: Infrastructure model
        """
        super().__init__(scope, id)

        self.infra = infra

        # Configure AWS provider with assumed role
        AwsProvider(
            self,
            "aws",
            region=infra.region,
            assume_role=[
                {
                    "role_arn": infra.role_arn,
                    "external_id": infra.external_id,
                    "session_name": f"beta9-provisioning-{infra.environment_id}",
                }
            ] if infra.role_arn else None,
            default_tags=[{"tags": infra.tags}],
        )

        # Configure S3 backend for state
        S3Backend(
            self,
            bucket=infra.state_backend_bucket,
            key=infra.state_backend_key,
            region=infra.state_backend_region,
            encrypt=True,
            dynamodb_table=f"{infra.state_backend_bucket}-lock",
        )

        # Create networking
        self.vpc = self._create_vpc()
        self.subnets = self._create_subnets()
        self.internet_gateway = self._create_internet_gateway()
        self.nat_gateways = self._create_nat_gateways()
        self._create_route_tables()

        # Create security groups
        self.eks_sg = self._create_eks_security_group()
        self.rds_sg = self._create_rds_security_group()
        self.redis_sg = self._create_redis_security_group()

        # Create EKS cluster
        if not infra.use_existing_cluster:
            self.eks_role = self._create_eks_role()
            self.eks_cluster = self._create_eks_cluster()
            self.node_groups = self._create_node_groups()

        # Create data layer
        self.rds_subnet_group = self._create_rds_subnet_group()
        self.rds_instance = self._create_rds_instance()

        self.redis_subnet_group = self._create_redis_subnet_group()
        self.redis_cluster = self._create_redis_cluster()

        # Create object storage
        self.s3_bucket = self._create_s3_bucket()

        # Define outputs
        self._create_outputs()

    def _create_vpc(self) -> Vpc:
        """Create VPC."""
        return Vpc(
            self,
            "vpc",
            cidr_block=self.infra.network.vpc_cidr,
            enable_dns_hostnames=True,
            enable_dns_support=True,
            tags={"Name": f"{self.infra.cluster_name}-vpc", **self.infra.tags},
        )

    def _create_subnets(self) -> Dict[str, Any]:
        """Create public and private subnets."""
        subnets: Dict[str, Any] = {"public": [], "private": []}

        # Public subnets
        for i, (cidr, az) in enumerate(
            zip(self.infra.network.public_subnets, self.infra.network.availability_zones)
        ):
            subnet = Subnet(
                self,
                f"public_subnet_{i}",
                vpc_id=self.vpc.id,
                cidr_block=cidr,
                availability_zone=az,
                map_public_ip_on_launch=True,
                tags={
                    "Name": f"{self.infra.cluster_name}-public-{az}",
                    "kubernetes.io/role/elb": "1",
                    **self.infra.tags,
                },
            )
            subnets["public"].append(subnet)

        # Private subnets
        for i, (cidr, az) in enumerate(
            zip(self.infra.network.private_subnets, self.infra.network.availability_zones)
        ):
            subnet = Subnet(
                self,
                f"private_subnet_{i}",
                vpc_id=self.vpc.id,
                cidr_block=cidr,
                availability_zone=az,
                map_public_ip_on_launch=False,
                tags={
                    "Name": f"{self.infra.cluster_name}-private-{az}",
                    "kubernetes.io/role/internal-elb": "1",
                    **self.infra.tags,
                },
            )
            subnets["private"].append(subnet)

        return subnets

    def _create_internet_gateway(self) -> InternetGateway:
        """Create internet gateway."""
        return InternetGateway(
            self,
            "igw",
            vpc_id=self.vpc.id,
            tags={"Name": f"{self.infra.cluster_name}-igw", **self.infra.tags},
        )

    def _create_nat_gateways(self) -> list[NatGateway]:
        """Create NAT gateways for private subnets."""
        nat_gateways = []

        for i, public_subnet in enumerate(self.subnets["public"]):
            eip = Eip(
                self,
                f"nat_eip_{i}",
                vpc=True,
                tags={"Name": f"{self.infra.cluster_name}-nat-eip-{i}", **self.infra.tags},
            )

            nat = NatGateway(
                self,
                f"nat_gateway_{i}",
                allocation_id=eip.id,
                subnet_id=public_subnet.id,
                tags={"Name": f"{self.infra.cluster_name}-nat-{i}", **self.infra.tags},
            )
            nat_gateways.append(nat)

        return nat_gateways

    def _create_route_tables(self) -> None:
        """Create and associate route tables."""
        # Public route table
        public_rt = RouteTable(
            self,
            "public_rt",
            vpc_id=self.vpc.id,
            route=[
                RouteTableRoute(
                    cidr_block="0.0.0.0/0",
                    gateway_id=self.internet_gateway.id,
                )
            ],
            tags={"Name": f"{self.infra.cluster_name}-public-rt", **self.infra.tags},
        )

        for i, subnet in enumerate(self.subnets["public"]):
            RouteTableAssociation(
                self,
                f"public_rta_{i}",
                subnet_id=subnet.id,
                route_table_id=public_rt.id,
            )

        # Private route tables (one per NAT gateway)
        for i, (subnet, nat) in enumerate(zip(self.subnets["private"], self.nat_gateways)):
            private_rt = RouteTable(
                self,
                f"private_rt_{i}",
                vpc_id=self.vpc.id,
                route=[
                    RouteTableRoute(
                        cidr_block="0.0.0.0/0",
                        nat_gateway_id=nat.id,
                    )
                ],
                tags={"Name": f"{self.infra.cluster_name}-private-rt-{i}", **self.infra.tags},
            )

            RouteTableAssociation(
                self,
                f"private_rta_{i}",
                subnet_id=subnet.id,
                route_table_id=private_rt.id,
            )

    def _create_eks_security_group(self) -> SecurityGroup:
        """Create security group for EKS cluster."""
        return SecurityGroup(
            self,
            "eks_sg",
            name=f"{self.infra.cluster_name}-eks-sg",
            description="Security group for EKS cluster",
            vpc_id=self.vpc.id,
            ingress=[
                SecurityGroupIngress(
                    from_port=443,
                    to_port=443,
                    protocol="tcp",
                    cidr_blocks=[self.infra.network.vpc_cidr],
                    description="Allow HTTPS from VPC",
                )
            ],
            egress=[
                SecurityGroupEgress(
                    from_port=0,
                    to_port=0,
                    protocol="-1",
                    cidr_blocks=["0.0.0.0/0"],
                    description="Allow all outbound",
                )
            ],
            tags={"Name": f"{self.infra.cluster_name}-eks-sg", **self.infra.tags},
        )

    def _create_rds_security_group(self) -> SecurityGroup:
        """Create security group for RDS."""
        return SecurityGroup(
            self,
            "rds_sg",
            name=f"{self.infra.cluster_name}-rds-sg",
            description="Security group for RDS PostgreSQL",
            vpc_id=self.vpc.id,
            ingress=[
                SecurityGroupIngress(
                    from_port=5432,
                    to_port=5432,
                    protocol="tcp",
                    cidr_blocks=[self.infra.network.vpc_cidr],
                    description="Allow PostgreSQL from VPC",
                )
            ],
            egress=[
                SecurityGroupEgress(
                    from_port=0,
                    to_port=0,
                    protocol="-1",
                    cidr_blocks=["0.0.0.0/0"],
                    description="Allow all outbound",
                )
            ],
            tags={"Name": f"{self.infra.cluster_name}-rds-sg", **self.infra.tags},
        )

    def _create_redis_security_group(self) -> SecurityGroup:
        """Create security group for Redis."""
        return SecurityGroup(
            self,
            "redis_sg",
            name=f"{self.infra.cluster_name}-redis-sg",
            description="Security group for ElastiCache Redis",
            vpc_id=self.vpc.id,
            ingress=[
                SecurityGroupIngress(
                    from_port=6379,
                    to_port=6379,
                    protocol="tcp",
                    cidr_blocks=[self.infra.network.vpc_cidr],
                    description="Allow Redis from VPC",
                )
            ],
            egress=[
                SecurityGroupEgress(
                    from_port=0,
                    to_port=0,
                    protocol="-1",
                    cidr_blocks=["0.0.0.0/0"],
                    description="Allow all outbound",
                )
            ],
            tags={"Name": f"{self.infra.cluster_name}-redis-sg", **self.infra.tags},
        )

    def _create_eks_role(self) -> IamRole:
        """Create IAM role for EKS cluster."""
        role = IamRole(
            self,
            "eks_role",
            name=f"{self.infra.cluster_name}-eks-role",
            assume_role_policy="""{
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "eks.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }]
            }""",
            tags=self.infra.tags,
        )

        IamRolePolicyAttachment(
            self,
            "eks_cluster_policy",
            role=role.name,
            policy_arn="arn:aws:iam::aws:policy/AmazonEKSClusterPolicy",
        )

        IamRolePolicyAttachment(
            self,
            "eks_vpc_resource_controller",
            role=role.name,
            policy_arn="arn:aws:iam::aws:policy/AmazonEKSVPCResourceController",
        )

        return role

    def _create_eks_cluster(self) -> EksCluster:
        """Create EKS cluster."""
        private_subnet_ids = [s.id for s in self.subnets["private"]]

        return EksCluster(
            self,
            "eks_cluster",
            name=self.infra.cluster_name,
            role_arn=self.eks_role.arn,
            vpc_config=EksClusterVpcConfig(
                subnet_ids=private_subnet_ids,
                security_group_ids=[self.eks_sg.id],
                endpoint_private_access=True,
                endpoint_public_access=True,
            ),
            tags=self.infra.tags,
        )

    def _create_node_groups(self) -> list[EksNodeGroup]:
        """Create EKS node groups."""
        node_groups = []

        # Create node role
        node_role = self._create_node_role()

        for ng_spec in self.infra.node_groups:
            node_group = EksNodeGroup(
                self,
                f"node_group_{ng_spec.name}",
                cluster_name=self.eks_cluster.name,
                node_group_name=f"{self.infra.cluster_name}-{ng_spec.name}",
                node_role_arn=node_role.arn,
                subnet_ids=[s.id for s in self.subnets["private"]],
                scaling_config=EksNodeGroupScalingConfig(
                    min_size=ng_spec.min_size,
                    max_size=ng_spec.max_size,
                    desired_size=ng_spec.desired_size,
                ),
                instance_types=[ng_spec.instance_type],
                labels=ng_spec.labels,
                tags=self.infra.tags,
            )
            node_groups.append(node_group)

        return node_groups

    def _create_node_role(self) -> IamRole:
        """Create IAM role for EKS nodes."""
        role = IamRole(
            self,
            "node_role",
            name=f"{self.infra.cluster_name}-node-role",
            assume_role_policy="""{
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }]
            }""",
            tags=self.infra.tags,
        )

        # Attach required policies
        for policy in [
            "AmazonEKSWorkerNodePolicy",
            "AmazonEKS_CNI_Policy",
            "AmazonEC2ContainerRegistryReadOnly",
        ]:
            IamRolePolicyAttachment(
                self,
                f"node_{policy.lower()}",
                role=role.name,
                policy_arn=f"arn:aws:iam::aws:policy/{policy}",
            )

        return role

    def _create_rds_subnet_group(self) -> DbSubnetGroup:
        """Create RDS subnet group."""
        return DbSubnetGroup(
            self,
            "rds_subnet_group",
            name=f"{self.infra.cluster_name}-rds-subnet",
            subnet_ids=[s.id for s in self.subnets["private"]],
            tags={"Name": f"{self.infra.cluster_name}-rds-subnet", **self.infra.tags},
        )

    def _create_rds_instance(self) -> DbInstance:
        """Create RDS PostgreSQL instance."""
        return DbInstance(
            self,
            "rds_instance",
            identifier=f"{self.infra.cluster_name}-db",
            engine="postgres",
            engine_version=self.infra.database.engine_version,
            instance_class=self.infra.database.instance_class,
            allocated_storage=self.infra.database.allocated_storage_gb,
            storage_encrypted=True,
            db_name="beta9",
            username="beta9admin",
            manage_master_user_password=True,  # AWS manages password in Secrets Manager
            multi_az=self.infra.database.multi_az,
            db_subnet_group_name=self.rds_subnet_group.name,
            vpc_security_group_ids=[self.rds_sg.id],
            backup_retention_period=self.infra.database.backup_retention_days,
            skip_final_snapshot=False,
            final_snapshot_identifier=f"{self.infra.cluster_name}-db-final",
            tags={"Name": f"{self.infra.cluster_name}-db", **self.infra.tags},
        )

    def _create_redis_subnet_group(self) -> ElasticacheSubnetGroup:
        """Create ElastiCache subnet group."""
        return ElasticacheSubnetGroup(
            self,
            "redis_subnet_group",
            name=f"{self.infra.cluster_name}-redis-subnet",
            subnet_ids=[s.id for s in self.subnets["private"]],
        )

    def _create_redis_cluster(self) -> ElasticacheCluster:
        """Create ElastiCache Redis cluster."""
        return ElasticacheCluster(
            self,
            "redis_cluster",
            cluster_id=f"{self.infra.cluster_name}-redis",
            engine="redis",
            engine_version=self.infra.cache.engine_version,
            node_type=self.infra.cache.node_type,
            num_cache_nodes=self.infra.cache.num_cache_nodes,
            subnet_group_name=self.redis_subnet_group.name,
            security_group_ids=[self.redis_sg.id],
            tags={"Name": f"{self.infra.cluster_name}-redis", **self.infra.tags},
        )

    def _create_s3_bucket(self) -> S3Bucket:
        """Create S3 bucket for object storage."""
        bucket = S3Bucket(
            self,
            "s3_bucket",
            bucket=self.infra.storage.bucket_name,
            tags={"Name": self.infra.storage.bucket_name, **self.infra.tags},
        )

        # Enable versioning
        if self.infra.storage.versioning:
            S3BucketVersioningA(
                self,
                "s3_versioning",
                bucket=bucket.id,
                versioning_configuration={"status": "Enabled"},
            )

        # Lifecycle configuration
        if self.infra.storage.lifecycle_rules:
            archive_days = self.infra.storage.lifecycle_rules.get("archive_after_days", 90)
            S3BucketLifecycleConfiguration(
                self,
                "s3_lifecycle",
                bucket=bucket.id,
                rule=[
                    S3BucketLifecycleConfigurationRule(
                        id="archive-old-objects",
                        status="Enabled",
                        transition=[
                            S3BucketLifecycleConfigurationRuleTransition(
                                days=archive_days,
                                storage_class="GLACIER",
                            )
                        ],
                    )
                ],
            )

        return bucket

    def _create_outputs(self) -> None:
        """Define Terraform outputs."""
        TerraformOutput(
            self,
            "vpc_id",
            value=self.vpc.id,
            description="VPC ID",
        )

        if not self.infra.use_existing_cluster:
            TerraformOutput(
                self,
                "cluster_name",
                value=self.eks_cluster.name,
                description="EKS cluster name",
            )

            TerraformOutput(
                self,
                "cluster_endpoint",
                value=self.eks_cluster.endpoint,
                description="EKS cluster endpoint",
            )

            TerraformOutput(
                self,
                "cluster_certificate_authority_data",
                value=self.eks_cluster.certificate_authority[0].data,
                description="EKS cluster CA data",
                sensitive=True,
            )

        TerraformOutput(
            self,
            "rds_endpoint",
            value=self.rds_instance.endpoint,
            description="RDS endpoint",
        )

        TerraformOutput(
            self,
            "rds_master_user_secret_arn",
            value=self.rds_instance.master_user_secret[0].secret_arn,
            description="RDS master user secret ARN in AWS Secrets Manager",
            sensitive=True,
        )

        TerraformOutput(
            self,
            "redis_endpoint",
            value=self.redis_cluster.cache_nodes[0].address,
            description="Redis endpoint",
        )

        TerraformOutput(
            self,
            "redis_port",
            value=self.redis_cluster.cache_nodes[0].port,
            description="Redis port",
        )

        TerraformOutput(
            self,
            "bucket_name",
            value=self.s3_bucket.bucket,
            description="S3 bucket name",
        )

        TerraformOutput(
            self,
            "bucket_arn",
            value=self.s3_bucket.arn,
            description="S3 bucket ARN",
        )
