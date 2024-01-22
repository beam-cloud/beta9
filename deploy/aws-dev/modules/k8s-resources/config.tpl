debugMode: false
database:
  postgres:
    host: ${var.db_host}
    port: 5432
    username: ${var.db_user}
    password: ${var.db_password}
    name: main
    timezone: UTC
  redis:
    mode: single
    addrs:
    - redis-master.beam:6379
    password:
    enableTLS: false
    dialTimeout: 3s
storage:
  mode: juicefs
  fsName: beam-fs
  fsPath: /data
  objectPath: /data/objects
  juicefs:
    redisURI: redis://juicefs-redis-master.beam:6379/0
    awsS3Bucket: ${var.juicefs_bucket}
    awsAccessKeyID: ${var.s3_access_key}
    awsSecretAccessKey: ${var.s3_secret_key}
gateway:
  host: beam.beam
  port: 1993
imageService:
  cacheURL:
  registryStore: local
  registryCredentialProvider: docker
  registries:
    docker:
      username: beamcloud
      password:
    s3:
      bucket: beam-images
      region: us-east-1
      accessKeyID: test
      secretAccessKey: test
  runner:
    baseImageTag: latest
    baseImageName: beam-runner
    baseImageRegistry: registry.localhost:5000
    tags:
      python3.8: py38-latest
      python3.9: py39-latest
      python3.10: py310-latest
      python3.11: py311-latest
      python3.12: py312-latest
worker:
  pools:
    beam-cpu:
      jobSpec:
        nodeSelector: {}
      poolSizing:
        defaultWorkerCpu: 1000m
        defaultWorkerGpuType: ""
        defaultWorkerMemory: 1Gi
        minFreeCpu: 1000m
        minFreeGpu: 0
        minFreeMemory: 1Gi
  # global pool attributes
  hostNetwork: false
  imageTag: latest
  imageName: beam-worker
  imageRegistry: registry.localhost:5000
  imagePullSecrets: []
  namespace: beam
  serviceAccountName: default
  # non-standard k8s job spec
  resourcesEnforced: false
  defaultWorkerCPURequest: 2000
  defaultWorkerMemoryRequest: 1024