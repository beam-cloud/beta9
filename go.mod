module github.com/beam-cloud/beam

go 1.21

toolchain go1.21.5

require (
	github.com/NVIDIA/go-nvml v0.12.0-1
	github.com/alicebob/miniredis/v2 v2.30.5
	github.com/aws/aws-sdk-go-v2 v1.24.0
	github.com/aws/aws-sdk-go-v2/config v1.18.34
	github.com/aws/aws-sdk-go-v2/credentials v1.13.33
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.11.78
	github.com/aws/aws-sdk-go-v2/service/ecr v1.24.4
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.20.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.38.3
	github.com/aws/karpenter v0.18.1
	github.com/beam-cloud/beat v0.0.0-20231129184723-cd62a0c5b46e
	github.com/beam-cloud/clip v0.0.0-20230822004255-4ecd939864f9
	github.com/beam-cloud/go-runc v0.0.0-20231222221338-b89899f33170
	github.com/bsm/redislock v0.9.4
	github.com/gin-gonic/gin v1.9.1
	github.com/gofrs/uuid v4.0.0+incompatible
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/uuid v1.5.0
	github.com/joho/godotenv v1.5.1
	github.com/mholt/archiver/v3 v3.5.1
	github.com/mitchellh/hashstructure/v2 v2.0.2
	github.com/moby/sys/mountinfo v0.7.1
	github.com/okteto/okteto v0.0.0-20231222160652-094ca6b3fca8
	github.com/opencontainers/runtime-spec v1.1.0
	github.com/opencontainers/umoci v0.4.7
	github.com/pkg/errors v0.9.1
	github.com/prometheus/procfs v0.12.0
	github.com/redis/go-redis/v9 v9.1.0
	github.com/samber/lo v1.39.0
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.8.4
	github.com/tj/assert v0.0.3
	google.golang.org/grpc v1.60.1
	google.golang.org/protobuf v1.32.0
	gorm.io/driver/postgres v1.3.10
	gorm.io/gorm v1.23.10
	k8s.io/api v0.29.0
	k8s.io/apimachinery v0.29.0
	k8s.io/client-go v0.29.0
	k8s.io/utils v0.0.0-20231127182322-b307cd553661
)

require (
	github.com/AdamKorcz/go-fuzz-headers v0.0.0-20210312213058-32f4d319f0d2 // indirect
	github.com/Masterminds/semver/v3 v3.2.1 // indirect
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/andybalholm/brotli v1.0.6 // indirect
	github.com/apex/log v1.4.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.14 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.13.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.2.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.5.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.40 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.1.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.34 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.33 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.15.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.13.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.15.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.21.3 // indirect
	github.com/aws/smithy-go v1.19.0 // indirect
	github.com/briandowns/spinner v1.23.0 // indirect
	github.com/bytedance/sonic v1.10.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/chenzhuoyu/base64x v0.0.0-20230717121745-296ad89f973d // indirect
	github.com/chenzhuoyu/iasm v0.9.0 // indirect
	github.com/containerd/console v1.0.3 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.3 // indirect
	github.com/cyphar/filepath-securejoin v0.2.4 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dsnet/compress v0.0.2-0.20210315054119-f66993602bf5 // indirect
	github.com/emicklei/go-restful/v3 v3.11.0 // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.2 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-openapi/jsonpointer v0.20.2 // indirect
	github.com/go-openapi/jsonreference v0.20.4 // indirect
	github.com/go-openapi/swag v0.22.6 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator v9.31.0+incompatible // indirect
	github.com/go-playground/validator/v10 v10.15.4 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/gnostic-models v0.6.8 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/pprof v0.0.0-20230207041349-798e818bf904 // indirect
	github.com/hanwen/go-fuse/v2 v2.3.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.13.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.1 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/pgtype v1.12.0 // indirect
	github.com/jackc/pgx/v4 v4.17.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.4 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/karrick/godirwalk v1.17.0 // indirect
	github.com/klauspost/compress v1.17.4 // indirect
	github.com/klauspost/cpuid/v2 v2.2.5 // indirect
	github.com/klauspost/pgzip v1.2.6 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/leodido/go-urn v1.2.4 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mholt/archiver/v3 v3.5.1 // indirect
	github.com/moby/sys/mountinfo v0.7.1 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nwaples/rardecode v1.1.3 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.0-rc5 // indirect
	github.com/opencontainers/runc v1.1.10 // indirect
	github.com/pelletier/go-toml/v2 v2.1.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.19 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rootless-containers/proto v0.1.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/tidwall/btree v1.6.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.2.11 // indirect
	github.com/ulikunitz/xz v0.5.11 // indirect
	github.com/urfave/cli v1.22.12 // indirect
	github.com/vbatts/go-mtree v0.5.0 // indirect
	github.com/xi2/xz v0.0.0-20171230120015-48954b6210f8 // indirect
	github.com/yuin/gopher-lua v1.1.0 // indirect
	golang.org/x/arch v0.5.0 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/exp v0.0.0-20231219180239-dc181d75b848 // indirect
	golang.org/x/net v0.19.0 // indirect
	golang.org/x/oauth2 v0.15.0 // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/term v0.15.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	golang.org/x/tools v0.16.1 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231212172506-995d672761c0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/klog/v2 v2.110.1 // indirect
	k8s.io/kube-openapi v0.0.0-20231214164306-ab13479f8bf8 // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.4.1 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
)
