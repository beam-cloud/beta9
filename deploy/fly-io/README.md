# Deploy JuiceFS S3 Gateway on fly.io

JuiceFS S3 Gateway is MinIO backed by a JuiceFS mount. There are two main reasons you may want this.

1. To browse files managed with JuiceFS using a web client in this case MinIO
1. To use multipart uploads and range downloads in order to get the most of your bandwidth

## Deploying the gateway

### Prepare your secrets

The app will need a few secrets. Take a look at [start.sh](./start.sh) to see what's available. Here's how you can create this.

```shell
cat <<'EOF' | tee > .env
TAILSCALE_AUTHKEY="my-tailscale-authkey"
JUICEFS_NAME="my-filesystem"
JUICEFS_REDIS_URI="rediss://username:password@hostname:6379/0"
JUICEFS_BUCKET="https://fly.storage.tigris.dev/my-bucket-name"
JUICEFS_ACCESS_KEY="my-access-key"
JUICEFS_SECRET_KEY="my-secret-key"
MINIO_ROOT_USER="my-username"
MINIO_ROOT_PASSWORD="my-password"
EOF
```

### Launch and deploy the fly app

This will modify the [fly.toml](./fly.toml) file in-place. It will also prompt you to confirm the configuration.

```shell
fly launch --org my-org --name my-deployment --no-deploy --copy-config --yes
```

This will import the secrets you created earlier, deploy the app, and scale it to 1 instance.

```shell
cat .env | fly secrets import
fly deploy
fly scale count 1 --max-per-region=1 --region=iad
```

# Deleting the deployment

```shell
fly app destroy --yes my-deployment
```
