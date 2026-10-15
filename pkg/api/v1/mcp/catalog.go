package mcp

func (g *Group) catalog() []mcpTool {
	target := schema(props{"name": str("App name: its newest active version"), "deployment_id": str("A specific version instead")})
	targetConfirm := schema(props{
		"name":          str("App name: its newest active version"),
		"deployment_id": str("A specific version instead"),
		"confirm":       boolean(),
	})
	name := schema(props{"name": str("")}, "name")
	nameConfirm := schema(props{"name": str(""), "confirm": boolean()}, "name")
	database := schema(props{"kind": databaseKind, "name": str("")}, "kind", "name")
	window := schema(props{"name": str("App name"), "stub_id": str("Or a stub id"), "window_minutes": integer(60)})
	request := props{
		"name":          str("App name: its newest active version"),
		"deployment_id": str("A specific version instead"),
		"path":          str("Path under the app URL, e.g. /predict"),
		"method":        str("HTTP method; default POST"),
		"body":          map[string]any{"description": "JSON body"},
		"headers":       map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
		"body_base64":   str("Binary request body, exclusive with body"),
		"port":          integer(0),
	}

	return withToolPolicies(append([]mcpTool{
		// workspace
		{
			Name:        "capabilities",
			Description: "Live workspace limits, GPU inventory and transport capabilities. Availability is advisory, not a reservation.",
			Schema:      schema(props{}),
			Run:         g.capabilities,
		},
		{
			Name: "wait_deployment",
			Description: "Verify HTTP application readiness on the exact revision. A container is ready once / answers " +
				"below 500 (a 404 still means its server is up); a path you name, and an endpoint's default " +
				"/health, must answer 2xx. On timeout, health is the app's last answer. TCP and portless " +
				"workloads require their protocol-specific check.",
			Schema: schema(props{
				"name":          str("App name"),
				"deployment_id": str("Exact revision"),
				"path":          str("Safe GET path that answers 2xx once ready; default / for a container, /health otherwise"),
				"port":          integer(0),
				"wait_seconds":  integer(20),
			}),
			Run: g.waitDeployment,
		},
		{
			Name: "whoami",
			Description: "Workspace id, name and gateway URL for this token. Where prepaid credit applies, `credit.ok` " +
				"says whether work can run and `credit.message` where to add credits when it cannot.",
			Schema: schema(props{}),
			Run:    g.whoami,
		},
		{
			Name: "list_apps",
			Description: "Apps in the workspace with their newest active deployment. Its url is that version's own; " +
				"latest_url follows each new deployment, so share that one. A multi-port container has a latest " +
				"URL per port in port_urls.",
			Schema: schema(props{"name": str("Filter by name"), "cursor": str("Next page cursor"), "limit": integer(50)}),
			Run:    g.listApps,
		},
		{
			Name: "get_app",
			Description: "One app: its config (resources, scaling, env, secret bindings, ports, disks) and URLs: url is " +
				"the version's own, latest_url (or port_urls per port) follows new deployments.",
			Schema: target,
			Run:    g.getApp,
		},
		{
			Name:        "delete_app",
			Description: "Delete an app and every version of it. Requires confirm=true.",
			Schema:      nameConfirm,
			Confirm:     "delete_app removes every deployment of the app.",
			Run:         g.deleteApp,
		},
		// deployments
		{
			Name:        "list_deployments",
			Description: "Deployment versions; filter by name and active.",
			Schema:      schema(props{"name": str(""), "active": boolean(), "limit": integer(50), "offset": integer(0)}),
			Run:         g.listDeployments,
		},
		{
			Name:        "get_deployment",
			Description: "One deployment version: active, URL, stub.",
			Schema:      target,
			Run:         g.getDeployment,
		},
		{
			Name: "redeploy",
			Description: "Deploy an existing configuration again, which restarts an app on fresh containers that read " +
				"current secrets, or roll back using an older deployment_id. Choose rollout=replace to retire " +
				"previous revisions; auto preserves them.",
			Schema: schema(props{
				"name":          str("App name"),
				"deployment_id": str("Specific configuration to deploy"),
				"rollout":       map[string]any{"type": "string", "enum": []string{"auto", "blue-green", "replace"}},
			}),
			Destructive: true,
			Run:         g.redeploy,
		},
		{
			Name:        "stop_deployment",
			Description: "Stop a deployment version; its containers drain and requests fail until started.",
			Schema:      target,
			Destructive: true,
			Run:         g.stopDeployment,
		},
		{
			Name:        "start_deployment",
			Description: "Start a stopped deployment version.",
			Schema:      target,
			Destructive: true,
			Run:         g.startDeployment,
		},
		{
			Name:        "delete_deployment",
			Description: "Delete one deployment version. Requires confirm=true.",
			Schema:      targetConfirm,
			Confirm:     "delete_deployment is irreversible.",
			Run:         g.deleteDeployment,
		},
		{
			Name:        "scale_deployment",
			Description: "Set the replica count of a pod deployment.",
			Schema:      schema(props{"name": str(""), "deployment_id": str(""), "containers": integer(1)}, "containers"),
			Destructive: true,
			Run:         g.scaleDeployment,
		},
		{
			Name: "invoke",
			Description: "Call an exact HTTP revision. Beam credentials authenticate private apps; public apps receive " +
				"only caller-supplied Authorization headers. Returns status, headers, and body.",
			Schema:      schema(request),
			Destructive: true,
			Run:         g.invoke,
		},
		// settings
		{
			Name: "update_config",
			Description: "Change settings and deploy a new version: dotted paths such as runtime.cpu (millicores), " +
				"runtime.memory (MB), runtime.gpu, runtime.gpu_count, autoscaler.max_containers, " +
				"autoscaler.min_containers, autoscaler.tasks_per_container, keep_warm_seconds, " +
				"concurrent_requests, workers, max_pending_tasks, task_policy.timeout, task_policy.max_retries, " +
				"authorized, ports, entry_point. Use set_env for variables.",
			Schema:      schema(props{"name": str("App name"), "fields": map[string]any{"type": "object", "description": "path -> value"}}, "name", "fields"),
			Destructive: true,
			Run:         g.updateConfig,
		},
		{
			Name: "set_env",
			Description: "Set or remove environment variables on an app and deploy a new version. Values may be " +
				"${{secret.NAME}}, ${{db.NAME.DATABASE_URL}} (or HOST, PORT, USERNAME, PASSWORD, DATABASE) or " +
				"${{app.NAME.URL}} (one port of a multi-port app: ${{app.NAME.URL.<port>}}; its TLS host:port " +
				"on the TCP gateway: ${{app.NAME.TCP.<port>}}).",
			Schema: schema(props{
				"name":  str("App name"),
				"env":   map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
				"unset": strList("Variables to remove"),
			}, "name"),
			Destructive: true,
			Run:         g.setEnv,
		},
		{
			Name: "connect_services",
			Description: "Wire `target` to `source`: a database's URL and parts, or an app's URL, as env references on " +
				"target; deploys a new version of target.",
			Schema: schema(props{
				"source":             str("Database or app name"),
				"target":             str("App that receives the variables"),
				"env_name":           str("URL variable name instead of the standard set; authenticated applications also receive <prefix>_TOKEN"),
				"rotate_credentials": boolean(),
			}, "source", "target"),
			Destructive: true,
			Run:         g.connectServices,
		},
		// secrets
		{Name: "list_secrets", Description: "Workspace secret names, approved hosts and placeholders; never secret values.", Schema: schema(props{}), Run: g.listSecrets},
		{
			Name:        "create_secret",
			Description: "Create a workspace secret. Containers/pods, functions, endpoints, task queues and image builds receive plaintext environment values and do not require hosts. Setting hosts does not enable proxying for them. Only sandbox/VM runtime bindings use approved hosts and HTTPS-header placeholders; explicit BETA9_TOKEN/BEAM_TOKEN bindings remain plaintext.",
			Schema:      schema(props{"name": str(""), "value": str("Secret value; 1–16384 bytes when hosts are set."), "hosts": strList("Optional for ordinary workloads; required for brokered sandbox/VM runtime secrets except BETA9_TOKEN/BEAM_TOKEN. Up to 64 credential hostnames or *.wildcards (subdomains only); no schemes, ports, paths, IPs or CIDRs.")}, "name", "value"),
			Destructive: true,
			Run:         g.createSecret,
		},
		{
			Name:        "update_secret",
			Description: "Change a secret's value or approved hosts. Containers/pods, functions, endpoints and task queues receive plaintext values at their next container start, even when hosts are set. Brokered sandbox/VM bindings refresh on the next request after the worker's 15-second cache expires.",
			Schema:      schema(props{"name": str(""), "value": str("Secret value; 1–16384 bytes when hosts are set."), "hosts": strList("Up to 64 credential hostnames or *.wildcards; omit to preserve hosts, [] to clear, or supply a list to replace. Clearing hosts prevents ordinary sandbox/VM secret bindings.")}, "name", "value"),
			Destructive: true,
			Run:         g.updateSecret,
		},
		{
			Name:        "delete_secret",
			Description: "Delete a workspace secret. Requires confirm=true.",
			Schema:      nameConfirm,
			Confirm:     "delete_secret breaks deployments still bound to it.",
			Run:         g.deleteSecret,
		},
		// databases
		{
			Name:        "list_databases",
			Description: "Managed database services and their state.",
			Schema:      schema(props{}),
			Run:         g.listDatabases,
		},
		{
			Name: "create_database",
			Description: "Create a managed Postgres, Redis, MySQL or MongoDB service on a durable disk. Credentials " +
				"become secrets; reference them with ${{db.<name>.DATABASE_URL}}.",
			Schema: schema(props{
				"kind":        databaseKind,
				"name":        str(""),
				"always_on":   boolean(),
				"size":        str("Disk capacity, e.g. 10Gi"),
				"cpu":         map[string]any{"type": "integer", "default": 1000, "description": "Millicores; 1000 is one CPU"},
				"memory":      map[string]any{"type": "integer", "default": 512, "description": "MiB"},
				"pool":        str("Optional worker pool"),
				"snapshot_id": str("Restore an available qcow snapshot into a new database; the source is retained"),
				"username":    str("Original Postgres role when restoring a snapshot"),
				"database":    str("Original Postgres database name when restoring a snapshot"),
			}, "kind", "name"),
			Destructive: true,
			Run:         g.createDatabase,
		},
		{
			Name:        "database_credentials",
			Description: "Connection string and parts for a database service.",
			Schema:      database,
			Run:         g.databaseCredentials,
		},
		{
			Name: "database_readiness",
			Description: "Probe Postgres or Redis through its TLS endpoint with stored credentials (up to five seconds). " +
				"May wake a serverless database. Repeat while ready=false; error explains the last failure. Pin " +
				"deployment_id to reject a replaced revision.",
			Schema: schema(props{"name": str(""), "deployment_id": str("Expected deployment revision")}, "name"),
			Run:    g.databaseReadiness,
		},
		{
			Name:        "rotate_database_credentials",
			Description: "Rotate a database's password; the database and every app bound to it restart with the new credentials.",
			Schema:      database,
			Destructive: true,
			Run:         g.rotateDatabase,
		},
		{
			Name:        "delete_database",
			Description: "Delete a database service, its credential secrets and its durable disk. Requires confirm=true.",
			Schema:      schema(props{"kind": databaseKind, "name": str(""), "confirm": boolean()}, "kind", "name"),
			Confirm:     "delete_database removes the service, its credentials and its durable disk.",
			Run:         g.deleteDatabase,
		},
		// storage
		{
			Name:        "list_volumes",
			Description: "Persistent volumes (mount with Volume(name, mount_path) in app code).",
			Schema:      schema(props{}),
			Run:         g.listVolumes,
		},
		{
			Name:        "create_volume",
			Description: "Create a persistent volume.",
			Schema:      name,
			Destructive: true,
			Run:         g.createVolume,
		},
		// stacks
		{
			Name:        "list_stacks",
			Description: "Stacks: named groups of apps shown together on the dashboard board.",
			Schema:      schema(props{}),
			Run:         g.listStacks,
		},
		{
			Name:        "create_stack",
			Description: "Create a stack, optionally with apps (by name).",
			Schema:      schema(props{"name": str(""), "apps": strList("App names")}, "name"),
			Destructive: true,
			Run:         g.createStack,
		},
		{
			Name:        "update_stack",
			Description: "Add or remove apps (by name) on a stack; the apps themselves are untouched.",
			Schema: schema(props{
				"name":   str(""),
				"add":    strList(""),
				"remove": strList(""),
				"spec": map[string]any{
					"type":        "object",
					"description": "Merge fields into the existing stack spec; preserves dashboard fields",
				},
				"expected_revision": str("Revision from list_stacks; prevents stale updates"),
			}, "name"),
			Destructive: true,
			Run:         g.updateStack,
		},
		{
			Name:        "delete_stack",
			Description: "Delete a stack; its apps are untouched.",
			Schema:      name,
			Destructive: true,
			Run:         g.deleteStack,
		},
		// observe
		{
			Name: "logs",
			Description: "Recent logs, newest last. By app name (every version), or one deployment, stub, task or " +
				"container. Each line carries its stream: stdout, stderr or system (the worker's notes: failed " +
				"or slow image loads, mount and checkpoint problems; a healthy start writes none); `stream` " +
				"keeps one of them out of the `tail` newest lines.",
			Schema: schema(props{
				"name":          str("App name"),
				"deployment_id": str(""),
				"stub_id":       str(""),
				"task_id":       str(""),
				"container_id":  str(""),
				"tail":          integer(100),
				"since_minutes": integer(0),
				"search":        str("Substring filter"),
				"stream":        logStream,
			}),
			Run: g.logs,
		},
		{
			Name:        "list_tasks",
			Description: "Recent tasks (invocations), newest first.",
			Schema: schema(props{
				"stub_id": str(""),
				"status":  str("Comma-separated: pending, running, complete, error, cancelled, timeout"),
				"limit":   integer(20),
			}),
			Run: g.listTasks,
		},
		{
			Name:        "get_task",
			Description: "Status, results, artifacts and failure details of a task. Optionally wait up to 55 seconds.",
			Schema:      schema(props{"task_id": str(""), "wait_seconds": integer(0)}, "task_id"),
			Run:         g.getTask,
		},
		{
			Name:        "stop_task",
			Description: "Stop a running or pending task.",
			Schema:      schema(props{"task_id": str("")}, "task_id"),
			Destructive: true,
			Run:         g.stopTask,
		},
		{
			Name: "metrics",
			Description: "CPU, memory, GPU memory, network and container count for an app over a window, per 1m or 1h " +
				"bucket, plus the latest bucket as `now`. Averages are per container; memory_limit and " +
				"cpu_limit are the configured resources.",
			Schema: schema(props{
				"name":           str("App name"),
				"deployment_id":  str(""),
				"stub_id":        str(""),
				"window_minutes": integer(60),
				"interval":       str("1m (default) or 1h"),
			}),
			Run: g.metrics,
		},
		{
			Name: "request_stats",
			Description: "Request count, 5xx share and p50/p95/p99 latency for an endpoint over a window (upper bounds " +
				"from a fixed histogram). Endpoints only: pods, functions and queues have `metrics` and `logs`.",
			Schema: window,
			Run:    g.requestStats,
		},
		{
			Name:        "list_webhooks",
			Description: "Workspace webhooks (URL, event types, enabled).",
			Schema:      schema(props{}),
			Run:         g.listWebhooks,
		},
		{
			Name: "create_webhook",
			Description: "Register a signed HTTP webhook for workspace events (stub.*, task.*, endpoint.request_stats). " +
				"Returns the signing secret once.",
			Schema:      schema(props{"url": str(""), "event_types": strList(""), "description": str("")}, "url"),
			Destructive: true,
			Run:         g.createWebhook,
		},
		// everything else
		{
			Name: "api_routes",
			Description: "Every gateway REST route, for use with `api`: containers, metrics timeseries, event history, " +
				"tokens, webhooks, volumes, disks, pods and more.",
			Schema: schema(props{"path": str("Filter path or RPC name; includes schemas when supplied")}),
			Run:    g.apiRoutes,
		},
		{
			Name: "api",
			Description: "Call any gateway REST route with this token. Methods other than GET need confirm=true. {ws} in " +
				"the path becomes your workspace id.",
			Schema: schema(props{
				"method":      str("Default GET"),
				"path":        str("e.g. /api/v1/container/{ws}"),
				"body":        map[string]any{"description": "JSON body"},
				"headers":     map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
				"body_base64": str("Binary request body"),
				"confirm":     boolean(),
			}, "path"),
			Destructive: true,
			Run:         g.api,
		},
	}, g.vmCatalog()...))
}
