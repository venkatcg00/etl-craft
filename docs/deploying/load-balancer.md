# Behind a load balancer

Run PostgreSQL, one active `etl-craft controller --no-ui`, and two or more
`etl-craft ui` copies using the same package, project configuration and Engine DB. Install
`etl-craft[ui]` (or `[all]`) in the API image. These are small configuration fragments;
S7.F supplies the complete Compose and Kubernetes layouts.

## Health, timeouts and shutdown

Probe **`GET /health` on each UI copy**, accepting only 200. It returns 503 with the cause
when the Engine DB cannot be reached, its applied migrations differ from the package, or
shutdown has begun. Public responses name a safe failure category; SQL, parameters and
internal exception details are never included. Server logs record the exception class.
A recovered Engine DB becomes ready without restarting the copy.
The controller's probe remains a process-liveness check; do not use it for API readiness.

Use no sticky sessions. Sign-in, refresh, revocation and run state live in the Engine DB.
Set proxy idle/read timeouts **above 30 seconds** for agent claim polls and above the longest
quiet interval expected in live log streams; 300 seconds is an example, not a maximum stream
length. Disable response buffering for streamed logs. AWS ALB's default 60-second idle timeout
is enough for a 30-second claim; raise it for quieter log streams.
See [ALB attributes](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/edit-load-balancer-attributes.html).

On SIGTERM, a UI copy immediately reports 503 and rejects new requests with 503. Admitted
requests can finish until `Orchestration.Shutdown_grace_seconds`; remaining streams are then
closed. Long polls can use the same shutdown signal to return an empty answer and retry
elsewhere; S5.C adds the agent routes. Give the container/process supervisor a stop deadline
slightly longer than that setting so it can finish cleanup. A browser or agent reconnects to
another copy without an affinity cookie.

## Proxy trust and TLS

Reuse S7.K's explicit setting; it also controls API forwarding headers:

```yaml
Docs_site:
  Trusted_proxies: [10.20.1.0/24, 10.20.2.0/24]
```

List only the actual proxy source networks. The API ignores `X-Forwarded-For` and
`X-Forwarded-Proto` from other peers. Through trusted proxies it walks the address chain from
the right, stopping at the first untrusted hop, and accepts one `http` or `https` protocol
value. Configure the edge to overwrite client-supplied forwarding headers. Audited HTTP
requests record the resolved address and scheme in `AUD_ACTIONS.ARGUMENTS._http`.

TLS can end at the load balancer, with HTTP to a private API listener. To keep TLS on the
backend hop, use a copy-local TLS proxy/sidecar and forward to the UI only over loopback;
the application listener itself is HTTP. Trust that proxy's source and restrict direct
network access to API listeners. Certificates belong to the proxy's secret management.

## Kubernetes Service and Ingress

A pod's readiness probe removes an unavailable copy from Service endpoints. Use a TCP
liveness probe if restarting a pod for a database outage would be wrong. Example fragments:

```yaml
# In the ui container:
readinessProbe:
  httpGet: {path: /health, port: 8730}
  periodSeconds: 5
  timeoutSeconds: 3
---
apiVersion: v1
kind: Service
metadata: {name: etl-craft-ui}
spec:
  selector: {app: etl-craft-ui}
  ports: [{port: 80, targetPort: 8730}]
  sessionAffinity: None
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: {name: etl-craft-ui}
spec:
  ingressClassName: your-ingress-class
  tls: [{hosts: [etl.example.com], secretName: etl-tls}]
  rules:
    - host: etl.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service: {name: etl-craft-ui, port: {number: 80}}
```

Configure idle timeouts and forwarding-header trust in the selected ingress controller;
they are controller-specific, not generic Ingress fields. See the Kubernetes
[probe guide](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
and [Ingress reference](https://kubernetes.io/docs/concepts/services-networking/ingress/).

## Docker Compose with Caddy

Merge this fragment into a project that already provides Engine DB credentials and project
mounts. Use an image containing `etl-craft[ui]`; omit a fixed container name or host port on
`ui`, so it can scale:

```yaml
services:
  ui:
    image: your-etl-craft-ui-image
    command: [etl-craft, ui]
    expose: ["8730"]
    stop_grace_period: 65s
    healthcheck:
      test: [CMD, python, -c, "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8730/health', timeout=3)"]
  proxy:
    image: caddy:2
    ports: ["443:443"]
    volumes: ["./Caddyfile:/etc/caddy/Caddyfile:ro"]
```

Set `Orchestration.Api_address: 0.0.0.0:8730` for those containers and choose the actual proxy
network for `Trusted_proxies`. An example Caddyfile discovers the scaled containers:

```caddyfile
etl.example.com {
    reverse_proxy {
        dynamic a {
            name ui
            port 8730
            refresh 5s
        }
        fail_duration 10s
        unhealthy_status 503
    }
}
```

Start with `docker compose up -d --scale ui=3`. Caddy's dynamic upstreams use passive failure
detection; active health checks do not run for them. Compose's per-container health status
alone does not remove a container from routing. For active per-copy readiness checks use the
Kubernetes/cloud options or an active-checking proxy such as HAProxy below. See
[Caddy upstream discovery](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy)
and [Compose scaling](https://docs.docker.com/reference/cli/docker/compose/up/).

## Cloud load balancer

Register each UI copy as an HTTP backend, probe `/health` for 200, disable target stickiness,
and configure the idle timeout as above. Terminate TLS on the listener, or re-encrypt to each
copy-local TLS proxy. Trust the load balancer's actual source subnets; keep the backend private.
Use its target draining/deregistration facility together with the application's shutdown grace.

## nginx or HAProxy on servers

For nginx, a short upstream fragment (add your normal TLS listener and certificates):

```nginx
upstream etl_ui {
    server 10.20.1.11:8730;
    server 10.20.1.12:8730;
}
server {
    listen 443 ssl;
    location / {
        proxy_pass http://etl_ui;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
        proxy_buffering off;
    }
}
```

The community nginx upstream uses passive failure handling; arrange active `/health` checks
externally, or use HAProxy's native check. See the
[nginx proxy directives](https://nginx.org/en/docs/http/ngx_http_proxy_module.html).
For an HAProxy backend behind your TLS frontend:

```text
backend etl_ui
    mode http
    timeout connect 5s
    timeout server 300s
    option httpchk GET /health
    http-check expect status 200
    http-request set-header X-Forwarded-For %[src]
    http-request set-header X-Forwarded-Proto https
    server ui1 10.20.1.11:8730 check
    server ui2 10.20.1.12:8730 check
```

Set the frontend's client timeout above 30 seconds too. If it accepts plaintext HTTP, derive
the protocol from that listener rather than asserting `https`.
