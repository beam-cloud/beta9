#!/usr/bin/env python3
"""Reload staging's held Okteto gateway only after complete target deregistration.

Requires the temporary healthy peer and service-selection label established for
hosted endpoint development. No image build, Pod replacement, or Okteto reconnect.
"""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import threading
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', default='arn:aws:eks:us-east-1:683656326989:cluster/eks-stage-01')
    parser.add_argument('--namespace', default='beta9')
    parser.add_argument('--aws-profile', default='beam-engineering-stage')
    parser.add_argument('--serving-label', default='beam.cloud/codex-gateway-serving')
    parser.add_argument('--restart', action='store_true', help='Validate a graceful restart of the current binary without releasing the build hold.')
    parser.add_argument('--evidence', default='/private/tmp/hosted-safe-reload.json')
    args = parser.parse_args()
    if args.context != 'arn:aws:eks:us-east-1:683656326989:cluster/eks-stage-01' or args.namespace != 'beta9':
        parser.error('This helper is restricted to the staging beta9 namespace.')
    k = ['kubectl', '--context', args.context, '-n', args.namespace]
    aws = ['aws', '--profile', args.aws_profile, '--region', 'us-east-1']
    runtime = '/var/tmp/beta9-hosted-dev'
    proof = {'started_at': time.time(), 'public_checks': [], 'restart_current': args.restart}
    evidence = Path(args.evidence)
    if evidence.exists():
        parser.error('Choose a new evidence file; existing results are never overwritten.')

    def run(command, **kwargs):
        result = subprocess.run(command, capture_output=True, **kwargs)
        if result.returncode:
            raise RuntimeError('Command failed: ' + ' '.join(command[:4]))
        return result.stdout

    def get(command):
        return json.loads(run(command))

    def save():
        evidence.write_text(json.dumps(proof, indent=2) + '\n')
        evidence.chmod(0o600)

    def pod(name):
        return get(k + ['get', 'pod', name, '-o', 'json'])

    def state(name):
        return get(k + ['exec', name, '-c', 'main', '--', 'python3', '-c',
            "import pathlib,json;p=pathlib.Path('/var/tmp/beta9-hosted-dev');"
            "print(json.dumps({'hold':p.joinpath('build-hold').exists(),"
            "'pid':p.joinpath('gateway.pid').read_text().strip(),"
            "'watcher':p.joinpath('reload.pid').read_text().strip(),"
            "'binary':p.joinpath('current-binary').read_text().strip(),"
            "'fingerprint':p.joinpath('current-fingerprint').read_text().strip()}))"])

    def label(name, value):
        value_pod = pod(name)
        patch = [{'op': 'test', 'path': '/metadata/uid', 'value': value_pod['metadata']['uid']},
                 {'op': 'add', 'path': '/metadata/labels/' + args.serving_label.replace('/', '~1'), 'value': value}]
        run(k + ['patch', 'pod', name, '--type=json', '-p', json.dumps(patch)])

    pods = get(k + ['get', 'pods', '-l', 'app.kubernetes.io/component=gateway', '-o', 'json'])['items']
    devs = [p for p in pods if p['metadata']['labels'].get('interactive.dev.okteto.com') == 'beta9-gateway']
    assert len(devs) == 1, 'Expected exactly one Okteto gateway'
    dev = devs[0]
    name = dev['metadata']['name']
    ip = dev['status']['podIP']
    peers = [p for p in pods if p['metadata']['uid'] != dev['metadata']['uid']
             and p['metadata']['labels'].get(args.serving_label) == 'true'
             and any(c['type'] == 'Ready' and c['status'] == 'True' for c in p['status'].get('conditions', []))]
    assert peers, 'A separately ready and selected gateway is required'
    peer_ips = {p['status']['podIP'] for p in peers}
    initial = state(name)
    assert initial['hold'], 'Build hold is required before this command starts'
    readiness_cli = subprocess.run(k + ['exec', name, '-c', 'main', '--', 'test', '-x', runtime + '/grpc-ready'], capture_output=True)
    assert readiness_cli.returncode == 0, 'Staging readiness checker is required; no traffic changed'
    desired = None
    if not args.restart:
        script = """set -euo pipefail
cd /workspace
{ find cmd pkg proto -type f ! -name '*_test.go' ! -name '.st*' -printf '%p %T@ %s\\n' | LC_ALL=C sort;
  sha256sum go.mod go.sum;
  if [[ -f /var/tmp/beta9-hosted-dev/cache-ready ]]; then cat /var/tmp/beta9-hosted-dev/cache-ready; fi;
} | sha256sum | cut -d' ' -f1
"""
        desired = run(k + ['exec', name, '-c', 'main', '--', 'bash', '-c', script]).decode().strip()
        if desired == initial['fingerprint']:
            proof.update(ok=True, reloaded=False, reason='No synced source changes', hold=True)
            save()
            print(json.dumps(proof))
            return
    identity = (dev['metadata']['uid'], dev['status']['containerStatuses'][0]['containerID'])
    bindings = get(k + ['get', 'targetgroupbindings.elbv2.k8s.aws', '-o', 'json'])['items']
    groups = {b['spec']['serviceRef']['name']: b['spec']['targetGroupARN'] for b in bindings
              if b['spec']['serviceRef']['name'] in ('beta9-gateway-proxy', 'beta9-gateway-proxy-tcp')}
    assert len(groups) == 2, 'Both gateway target groups are required'
    services = ('beta9-gateway', *groups)
    for service in services:
        assert get(k + ['get', 'service', service, '-o', 'json'])['spec']['selector'].get(args.serving_label) == 'true'

    def health():
        result = {}
        for service, arn in groups.items():
            records = get(aws + ['elbv2', 'describe-target-health', '--target-group-arn', arn])['TargetHealthDescriptions']
            states = {r['Target']['Id']: r['TargetHealth']['State'] for r in records}
            assert any(states.get(peer_ip) == 'healthy' for peer_ip in peer_ips), 'Healthy peer lost'
            result[service] = states
        return result

    stop = threading.Event()

    def probe():
        while not stop.is_set():
            started = time.time()
            record = {'at': started}
            try:
                request = urllib.request.Request('https://app.stage.beam.cloud/api/v1/health', headers={'Connection': 'close'})
                with urllib.request.urlopen(request, timeout=5) as response:
                    record['status'] = response.status
                    record['ok'] = response.status == 200
            except Exception as error:
                record.update(ok=False, error_type=type(error).__name__)
            record['latency_ms'] = round((time.time() - started) * 1000, 3)
            proof['public_checks'].append(record)
            stop.wait(0.5)

    def checks():
        assert proof['public_checks'] and all(r['ok'] for r in proof['public_checks']), 'Public check failed; evidence retained'

    watcher = threading.Thread(target=probe)
    watcher.start()
    held = True
    try:
        health()
        label(name, 'false')
        proof['excluded_at'] = time.time()
        print('Gateway excluded; waiting for complete AWS deregistration.', flush=True)
        deadline = time.monotonic() + 360
        while time.monotonic() < deadline:
            checks()
            states = health()
            slices = get(k + ['get', 'endpointslices', '-o', 'json'])['items']
            selected = any(ip in e['addresses'] for s in slices
                           if s['metadata'].get('labels', {}).get('kubernetes.io/service-name') in services for e in s['endpoints'])
            if not selected and all(values.get(ip) in (None, 'unused') for values in states.values()):
                break
            time.sleep(3)
        else:
            raise RuntimeError('Deregistration deadline; gateway is still held and excluded')
        proof['fully_deregistered_at'] = time.time()
        proof['deregistered_target_states'] = states
        source = get(aws + ['secretsmanager', 'get-secret-value', '--secret-id', 'beta9'])
        assert get(aws + ['sts', 'get-caller-identity'])['Account'] == '683656326989'
        run(k + ['annotate', 'externalsecret', 'beta9', 'force-sync=' + str(time.time_ns()), '--overwrite'])
        expected = hashlib.sha256(source['SecretString'].encode()).hexdigest()
        for _ in range(45):
            secret = get(k + ['get', 'secret', 'beta9-config', '-o', 'json'])
            mounted = run(k + ['exec', name, '-c', 'main', '--', 'sha256sum', '/etc/beta9/config.yaml']).decode().split()[0]
            if base64.b64decode(secret['data']['config.yaml']) == source['SecretString'].encode() and mounted == expected:
                break
            time.sleep(2)
        else:
            raise RuntimeError('AWS, ExternalSecret, and mounted config differ')
        latest = get(aws + ['secretsmanager', 'describe-secret', '--secret-id', 'beta9'])
        assert 'AWSCURRENT' in latest['VersionIdsToStages'][source['VersionId']]
        proof['secret'] = {'version': source['VersionId'], 'sha256': expected, 'verified_at': time.time()}
        checks()
        if args.restart:
            script = """import os,pathlib,signal,sys
p=pathlib.Path('/var/tmp/beta9-hosted-dev');assert p.joinpath('build-hold').exists()
pid=int(p.joinpath('gateway.pid').read_text());assert str(pid)==sys.argv[1]
fd=os.pidfd_open(pid);assert pathlib.Path('/proc',str(pid),'cmdline').read_bytes().split(b'\\0')[:-1]==[sys.argv[2].encode()]
signal.pidfd_send_signal(fd,signal.SIGTERM);os.close(fd)
"""
            proof['signal_at'] = time.time()
            run(k + ['exec', name, '-c', 'main', '--', 'python3', '-c', script, initial['pid'], initial['binary']])
        else:
            run(k + ['exec', name, '-c', 'main', '--', 'rm', runtime + '/build-hold'])
            held = False
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            checks()
            current = state(name)
            ready = subprocess.run(k + ['exec', name, '-c', 'main', '--', 'curl', '-fsS', '--max-time', '2', 'http://127.0.0.1:1994/api/v1/health'], capture_output=True).returncode == 0
            expected_build = args.restart or (current['fingerprint'] == desired and current['binary'] != initial['binary'])
            if ready and current['pid'] != initial['pid'] and expected_build:
                break
            time.sleep(1)
        else:
            raise RuntimeError('Reload deadline; inspect gateway logs while peer serves')
        run(k + ['exec', name, '-c', 'main', '--', 'touch', runtime + '/build-hold'])
        held = True
        current_pod = pod(name)
        assert (current_pod['metadata']['uid'], current_pod['status']['containerStatuses'][0]['containerID']) == identity
        assert current['watcher'] == initial['watcher'], 'Okteto watcher changed'
        readiness = get(k + ['exec', name, '-c', 'main', '--', runtime + '/grpc-ready'])
        assert readiness['grpc_readiness'] == 'SERVING', 'Gateway gRPC readiness failed'
        proof['binary_sha256'] = run(k + ['exec', name, '-c', 'main', '--', 'sha256sum', '/proc/' + current['pid'] + '/exe']).decode().split()[0]
        label(name, 'true')
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            checks()
            final_health = health()
            if all(values.get(ip) == 'healthy' for values in final_health.values()):
                break
            time.sleep(3)
        else:
            raise RuntimeError('Target health deadline after restart')
        proof.update(ok=True, finished_at=time.time(), before=initial, after=current, same_pod_container=True, final_target_health=final_health, hold=True)
    finally:
        if not held:
            run(k + ['exec', name, '-c', 'main', '--', 'touch', runtime + '/build-hold'])
        stop.set()
        watcher.join(timeout=6)
        save()
    print(json.dumps({'ok': proof['ok'], 'public_checks': len(proof['public_checks']), 'failures': sum(not r['ok'] for r in proof['public_checks']), 'binary_sha256': proof['binary_sha256'], 'hold': True, 'evidence': str(evidence)}))


if __name__ == '__main__':
    main()
