debugMode: false
database:
  postgres:
    host: ${db_host}
    port: 5432
    username: ${db_user}
    password: "${db_password}"
    name: main
    timezone: UTC
  redis:
    mode: single
    addrs:
    - redis-master.beta9:6379
    password: "${redis_password}"
    enableTLS: false
    dialTimeout: 3s
storage:
  mode: juicefs
  fsName: beta9-fs
  fsPath: /data
  objectPath: /data/objects
  juicefs:
    redisURI: redis://:${juicefs_redis_password}@juicefs-redis-master.beta9:6379/0
    awsS3Bucket: ${juicefs_bucket}
    awsAccessKeyID: ${aws_access_key_id}
    awsSecretAccessKey: ${aws_secret_access_key}
gateway:
  host: gateway.beta9
  port: 1993
imageService:
  registryStore: s3
  registryCredentialProvider: aws
  registries:
    docker:
      username: beamcloud
      password:
    s3:
      bucket: ${images_bucket}
      region: ${aws_region}
      accessKeyID: ${aws_access_key_id}
      secretAccessKey: ${aws_secret_access_key}
  runner:
    baseImageTag: latest
    baseImageName: beta9-runner
    baseImageRegistry: public.ecr.aws/k2t1v1n6
    tags:
      python3.8: py38-latest
      python3.9: py39-latest
      python3.10: py310-latest
      python3.11: py311-latest
      python3.12: py312-latest
worker:
  pools:
    default:
      jobSpec:
        nodeSelector: {}
      poolSizing:
        defaultWorkerCpu: 1000m
        defaultWorkerMemory: 1Gi
        minFreeCpu: 1000m
        minFreeGpu: 0
        minFreeMemory: 1Gi
  # global pool attributes
  hostNetwork: false
  imageTag: latest
  imageName: beta9-worker
  imageRegistry: public.ecr.aws/k2t1v1n6
  imagePullSecrets: []
  namespace: beta9
  serviceAccountName: default
  # non-standard k8s job spec
  resourcesEnforced: false
  defaultWorkerCPURequest: 2000
  defaultWorkerMemoryRequest: 1024