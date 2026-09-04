module github.com/beam-cloud/beta9

go 1.25.7

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/Masterminds/squirrel v1.5.4
	github.com/Thunder-Compute/thunder-sdk v0.1.0
	github.com/VictoriaMetrics/metrics v1.37.0
	github.com/alicebob/miniredis/v2 v2.30.5
	github.com/aws/aws-sdk-go-v2 v1.42.1
	github.com/aws/aws-sdk-go-v2/config v1.32.17
	github.com/aws/aws-sdk-go-v2/credentials v1.19.16
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.17.58
	github.com/aws/aws-sdk-go-v2/service/autoscaling v1.44.4
	github.com/aws/aws-sdk-go-v2/service/cloudformation v1.45.0
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.316.1
	github.com/aws/aws-sdk-go-v2/service/ecr v1.27.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.75.3
	github.com/beam-cloud/clip v0.0.0-20260904163020-a69e81f5367e
	github.com/beam-cloud/go-runc v0.0.0-20250911154456-bb45084abfe1
	github.com/beam-cloud/goproc v0.1.12
	github.com/beam-cloud/redislock v0.0.0-20250201162619-1b534b3be324
	github.com/beam-cloud/rendezvous v0.0.0-20250415141250-2a0f81633db8
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/cloudevents/sdk-go/v2 v2.15.2
	github.com/cockroachdb/redact v1.1.8
	github.com/coreos/go-iptables v0.7.1-0.20240112124308-65c67c9f46e6
	github.com/go-faker/faker/v4 v4.6.0
	github.com/go-playground/validator/v10 v10.26.0
	github.com/go-viper/mapstructure/v2 v2.4.0
	github.com/gofrs/uuid v4.4.0+incompatible
	github.com/google/go-containerregistry v0.21.7
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/uuid v1.6.0
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674
	github.com/hanwen/go-fuse/v2 v2.10.1
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/jmoiron/sqlx v1.4.0
	github.com/json-iterator/go v1.1.12
	github.com/knadh/koanf/parsers/json v0.1.0
	github.com/knadh/koanf/parsers/yaml v0.1.0
	github.com/knadh/koanf/providers/file v0.1.0
	github.com/knadh/koanf/providers/rawbytes v0.1.0
	github.com/knadh/koanf/v2 v2.0.1
	github.com/labstack/echo-contrib v0.17.1
	github.com/labstack/echo/v4 v4.13.3
	github.com/lib/pq v1.10.9
	github.com/mitchellh/hashstructure/v2 v2.0.2
	github.com/mitchellh/mapstructure v1.5.0
	github.com/opencontainers/runtime-spec v1.2.0
	github.com/opencontainers/umoci v0.4.7
	github.com/openmeterio/openmeter v1.0.0-beta.47
	github.com/oracle/oci-go-sdk v24.3.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/pressly/goose/v3 v3.21.1
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/procfs v0.20.1
	github.com/redis/go-redis/v9 v9.5.1
	github.com/rs/zerolog v1.34.0
	github.com/s2-streamstore/s2-sdk-go v0.18.0
	github.com/sashabaranov/go-openai v1.35.7
	github.com/shirou/gopsutil/v4 v4.26.5
	github.com/sirupsen/logrus v1.9.4
	github.com/stretchr/testify v1.11.1
	github.com/tj/assert v0.0.3
	github.com/vishvananda/netlink v1.3.1
	github.com/vishvananda/netns v0.0.5
	github.com/yandex-cloud/geesefs v0.0.0-20260530201649-8ba8948bc7e4
	go.opentelemetry.io/otel v1.43.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.33.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.33.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.15.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.31.0
	go.opentelemetry.io/otel/log v0.15.0
	go.opentelemetry.io/otel/sdk v1.43.0
	go.opentelemetry.io/otel/sdk/log v0.15.0
	go.opentelemetry.io/otel/sdk/metric v1.43.0
	go.opentelemetry.io/otel/trace v1.43.0
	golang.org/x/exp v0.0.0-20260410095643-746e56fc9e2f
	golang.org/x/net v0.56.0
	golang.org/x/sync v0.22.0
	golang.org/x/sys v0.47.0
	golang.org/x/time v0.15.0
	google.golang.org/grpc v1.82.1
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v2 v2.4.0
	gotest.tools v2.2.0+incompatible
	gvisor.dev/gvisor v0.0.0-20260224225140-573d5e7127a8
	k8s.io/api v0.34.0
	k8s.io/apimachinery v0.34.0
	k8s.io/client-go v0.34.0
	k8s.io/utils v0.0.0-20250604170112-4c0f3b243397
	tailscale.com v1.95.0-pre.0.20260305003012-d58bfb8a1b51
)

require (
	cel.dev/expr v0.25.1 // indirect
	cloud.google.com/go v0.120.0 // indirect
	cloud.google.com/go/auth v0.16.5 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/iam v1.5.2 // indirect
	cloud.google.com/go/monitoring v1.24.2 // indirect
	cloud.google.com/go/storage v1.50.0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.2 // indirect
	github.com/Azure/azure-sdk-for-go v32.1.0+incompatible // indirect
	github.com/Azure/azure-storage-blob-go v0.7.1-0.20190724222048-33c102d4ffd2 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.11.18 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.24 // indirect
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.7 // indirect
	github.com/Azure/go-autorest/autorest/azure/cli v0.4.2 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.1 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.32.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.50.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.50.0 // indirect
	github.com/aws/aws-sdk-go v1.55.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.11 // indirect
	github.com/cncf/xds/go v0.0.0-20260202195803-dba9d589def2 // indirect
	github.com/containerd/console v1.0.5 // indirect
	github.com/creachadair/msync v0.7.1 // indirect
	github.com/dimchansky/utfbom v1.1.1 // indirect
	github.com/docker/cli v29.5.3+incompatible // indirect
	github.com/docker/docker-credential-helpers v0.9.3 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.37.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.3 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/gabriel-vasile/mimetype v1.4.8 // indirect
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	github.com/go-openapi/swag/jsonname v0.25.5 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.6 // indirect
	github.com/googleapis/gax-go/v2 v2.15.0 // indirect
	github.com/huin/goupnp v1.3.0 // indirect
	github.com/jacobsa/fuse v0.0.0-20230810134708-ab21db1af836 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/mattn/go-ieproxy v0.0.0-20190805055040-f9202b1cfdeb // indirect
	github.com/mattn/go-sqlite3 v1.14.24 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/oasdiff/yaml v0.1.1 // indirect
	github.com/oasdiff/yaml3 v0.0.14 // indirect
	github.com/opencontainers/runc v1.2.8 // indirect
	github.com/opencontainers/selinux v1.13.0 // indirect
	github.com/pires/go-proxyproto v0.8.1 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/prometheus-community/pro-bing v0.4.0 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2 // indirect
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/winfsp/cgofuse v1.5.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.43.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.61.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.64.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	google.golang.org/api v0.249.0 // indirect
	google.golang.org/genproto v0.0.0-20250603155806-513f23925822 // indirect
	gopkg.in/evanphx/json-patch.v4 v4.12.0 // indirect
	gotest.tools/v3 v3.5.2 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.3.0 // indirect
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/AdamKorcz/go-fuzz-headers v0.0.0-20210312213058-32f4d319f0d2 // indirect
	github.com/akutz/memconn v0.1.0 // indirect
	github.com/alexbrainman/sspi v0.0.0-20231016080023-1a75b4708caa // indirect
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/apex/log v1.4.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.10 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.30 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.30 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.5.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.30 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.42.1
	github.com/aws/smithy-go v1.27.3
	github.com/beam-cloud/ristretto v0.0.0-20241013204426-d1403e359aa2
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bsm/redislock v0.9.4
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coder/websocket v1.8.14 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/cyphar/filepath-securejoin v0.6.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dblohm7/wingoes v0.0.0-20240119213807-a09d6be7affa // indirect
	github.com/deckarep/golang-set/v2 v2.8.0
	github.com/dgraph-io/ristretto v1.0.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/djherbis/atime v1.1.0
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/ebitengine/purego v0.10.0 // indirect
	github.com/emicklei/go-restful/v3 v3.12.2 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/fxamacker/cbor/v2 v2.9.0 // indirect
	github.com/gaissmai/bart v0.26.1 // indirect
	github.com/getkin/kin-openapi v0.144.0 // indirect
	github.com/go-chi/chi/v5 v5.1.0 // indirect
	github.com/go-json-experiment/json v0.0.0-20250813024750-ebf49471dced // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-openapi/jsonpointer v0.22.5 // indirect
	github.com/go-openapi/jsonreference v0.21.0 // indirect
	github.com/go-openapi/swag v0.23.0 // indirect
	github.com/godbus/dbus/v5 v5.2.2 // indirect
	github.com/gofrs/flock v0.12.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/gnostic-models v0.7.0 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.2
	github.com/hdevalence/ed25519consensus v0.2.0 // indirect
	github.com/jellydator/ttlcache/v3 v3.2.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/jsimonetti/rtnetlink v1.4.1 // indirect
	github.com/karrick/godirwalk v1.17.0 // indirect
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/klauspost/pgzip v1.2.6
	github.com/knadh/koanf/maps v0.1.1 // indirect
	github.com/labstack/gommon v0.4.2 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/lufia/plan9stats v0.0.0-20240226150601-1dcf7310316a // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.23 // indirect
	github.com/mdlayher/netlink v1.7.3-0.20250113171957-fbb4dce95f42 // indirect
	github.com/mdlayher/socket v0.5.0 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-ps v1.0.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/sys/mountinfo v0.7.2
	github.com/moby/sys/user v0.4.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/oapi-codegen/runtime v1.1.1 // indirect
	github.com/oklog/ulid/v2 v2.1.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/opencontainers/runtime-tools v0.9.1-0.20230914150019-408c51e934dc // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.69.0 // indirect
	github.com/rootless-containers/proto v0.1.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/safchain/ethtool v0.4.0 // indirect
	github.com/sethvargo/go-retry v0.2.4 // indirect
	github.com/shirou/gopsutil v3.21.11+incompatible // indirect
	github.com/syndtr/gocapability v0.0.0-20200815063812-42c35b437635 // indirect
	github.com/tailscale/certstore v0.1.1-0.20260409135935-3638fb84b77d // indirect
	github.com/tailscale/go-winio v0.0.0-20231025203758-c4f33415bf55 // indirect
	github.com/tailscale/hujson v0.0.0-20260302212456-ecc657c15afd // indirect
	github.com/tailscale/peercred v0.0.0-20250107143737-35a0c7bd7edc // indirect
	github.com/tailscale/web-client-prebuilt v0.0.0-20250124233751-d4cd19a26976 // indirect
	github.com/tailscale/wireguard-go v0.0.0-20250716170648-1d0488a3d7da // indirect
	github.com/tidwall/btree v1.8.1 // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/urfave/cli v1.22.15 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/fasttemplate v1.2.2 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/vbatts/go-mtree v0.5.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/yuin/gopher-lua v1.1.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/proto/otlp v1.9.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	go4.org/mem v0.0.0-20240501181205-ae6ca9944745 // indirect
	go4.org/netipx v0.0.0-20231129151722-fdeea329fbba // indirect
	golang.org/x/crypto v0.54.0
	golang.org/x/mod v0.37.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/term v0.45.0
	golang.org/x/text v0.40.0 // indirect
	golang.zx2c4.com/wintun v0.0.0-20230126152724-0fa3db229ce2 // indirect
	golang.zx2c4.com/wireguard/windows v0.5.3 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260414002931-afd174a4e478
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	k8s.io/kube-openapi v0.0.0-20250710124328-f3f2b991d03b // indirect
	sigs.k8s.io/json v0.0.0-20241014173422-cfa47c3a1cc8 // indirect
	sigs.k8s.io/yaml v1.6.0
	tags.cncf.io/container-device-interface v0.8.0
	tags.cncf.io/container-device-interface/specs-go v0.8.0
)

replace github.com/yandex-cloud/geesefs => github.com/beam-cloud/geesefs v0.0.0-20260904152738-869a10c011dd

replace github.com/aws/aws-sdk-go => github.com/beam-cloud/geesefs/s3ext v0.0.0-20250606164905-2f3593d03f4f

replace github.com/winfsp/cgofuse => github.com/vitalif/cgofuse v0.0.0-20230609211427-22e8fa44f6b8

replace github.com/jacobsa/fuse => github.com/beam-cloud/gofuse v0.0.0-20260903160913-d6bd57646e38
