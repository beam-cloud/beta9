# Hetzner Cloud CPU Pools

Hetzner Cloud can back managed private pools with CPU-only servers. The initial default offer set is US-based Hetzner Cloud locations: Ashburn, VA (`ash`) and Hillsboro, OR (`hil`). The Hetzner server type is used only when creating or extending pool capacity. After the server joins as a private-pool agent, workloads scheduled with `pool=<name>` use the normal scheduler and its existing CPU, memory, GPU, runtime, and flag matching.

Hetzner nodes are launched on a configured Hetzner Cloud Network so nodes in the same network zone can communicate on private IPs. The Network must already exist and must have a `cloud` subnet in each network zone where Beam will launch nodes.

## Credentials

Create a project-scoped Hetzner Cloud API token from:

- [Hetzner Cloud API docs](https://docs.hetzner.cloud/reference/cloud)
- [Using the Hetzner Cloud API](https://docs.hetzner.com/cloud/api/getting-started/using-api/)

Configure this secret value:

- `apiToken`

Use a token from the same Hetzner Cloud project that owns the Networks and servers. Hetzner tokens are sent as `Authorization: Bearer <token>`.

## Minimal Rights

Hetzner Cloud tokens are project-scoped. Use a read/write token for managed CPU pools because Beam needs to discover offers and create/delete servers:

```text
GET    /locations
GET    /server_types
GET    /networks
POST   /servers
GET    /servers/{id}
DELETE /servers/{id}
```

If you only want to test offer discovery, a read-only token is enough for `GET /locations` and `GET /server_types`.

## Private Networks

Hetzner Networks are limited by network zone. Locations in the same zone can share one Network; locations in different zones need separate Networks, configured globally when every enabled location uses the same Network or with per-region overrides when they differ. The initial supported/default locations are `ash` and `hil`:

```text
ash              -> us-east
hil              -> us-west
fsn1, nbg1, hel1 -> eu-central
sin              -> ap-southeast
```

Beam validates that the selected Network has a `cloud` subnet in the location's `network_zone` before creating a server. Server creation includes `networks: [network_id]`, so the node receives a private interface during provisioning while retaining Hetzner's default public networking for bootstrap.

## Config

```yaml
providers:
  hetzner:
    apiToken: ${HETZNER_API_TOKEN}
    baseURL: https://api.hetzner.cloud/v1
    image: ubuntu-24.04
    defaultRegions:
      - ash
      - hil
    imageByRegion:
      ash: ubuntu-24.04
      hil: ubuntu-24.04
    sshKeys:
      - beam-workers
    sshKeysByRegion:
      ash:
        - beam-workers-ash
      hil:
        - beam-workers-hil
    privateNetwork:
      # May be the Hetzner Network ID or name.
      id: 456
      name: ""
      # Optional per-region overrides. Use these when enabled regions are
      # in different Hetzner network zones.
      regionIds:
        ash: 456
        hil: 789
      regionNames: {}
      requireSubnet: true
    serverTypePrices:
      "cpx31": 0.0312
      "cpx31:ash": 0.0312
      "cpx31:hil": 0.0312
    serverTypeCategories:
      cpx31: shared
    regionMetadata:
      ash:
        displayName: Ashburn
        latitude: 39.0438
        longitude: -77.4874
      hil:
        displayName: Hillsboro
        latitude: 45.5229
        longitude: -122.9898
```

`image` can be a Hetzner image name or ID accepted by `POST /servers`. `sshKeys` and `sshKeysByRegion` are optional; omit them when bootstrap access is handled entirely through cloud-init user data.

`defaultRegions` controls which Hetzner locations appear when a pool request does not specify regions. It defaults to `ash` and `hil`. A pool can still request a different explicit `regions` value when we decide to test or enable non-US locations.

`privateNetwork` is required for launching Hetzner nodes. Beam resolves `id`, `name`, `regionIds`, or `regionNames` through `GET /networks`. With `requireSubnet: true`, Beam refuses to launch if the selected Network does not have a `cloud` subnet in the selected location's network zone.

`serverTypePrices` overrides are hourly USD values and are useful when you need to pin internal pricing independent of Hetzner's API response. Never log or return `apiToken`; redact it in config dumps, provider errors, and support traces.
