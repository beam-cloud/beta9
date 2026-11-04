import json
import math
import pathlib

root = pathlib.Path(__file__).resolve().parent
runs = [json.loads(p.read_text()) for p in sorted((root / 'runs').glob('*.json'))]
assert len(runs) == 12
samples = [s for r in runs for s in r['samples']]
assert len(samples) == 200
assert len({s['guest_proof'].splitlines()[0] for s in samples}) == 200
for s in samples:
    assert s['removed']
    assert s['seed_command']['exit_code'] == 0
    assert s['seed_command']['stdout'] == s['guest_proof']
    assert s['restore_commands']
    assert all(c['exit_code'] == 0 and c['stdout'] == s['guest_proof'] for c in s['restore_commands'])
    assert not s.get('initial_proof_mismatch') and not s.get('restore_proof_mismatch')
assert all(not p.get('errors') for r in runs for p in r['phases'])

out = {
    'commit': json.loads((root / 'source-verification.json').read_text())['commit'],
    'worker_sha256': json.loads((root / 'binary-checksums.json').read_text())['worker.merge-ready-v51']['sha256'],
    'image_digest': 'sha256:f17418b50b10748b0beb0affbedabae765dfcc0f23168a56ee2ff28acd0b5de2',
    'gateway': 'Normal Okteto Air supervisor; no custom diagnostic gateway or startup profiling',
    'fixture': {
        'cpu': .1, 'memory_mib': 512, 'disk': '1GiB',
        'image_prepared_outside_clock': True,
        'client_connections_prepared_outside_clock': True,
        'per_vm_smt_isolation_preserved': True,
        'recent_stub_ttl_seconds': 604800,
        'postgres_connections': 32, 'vm_batch_cap': 128,
    },
    'goproc': {
        'version': 'v0.1.18',
        'commit': 'a70f7739dd771769faa7af3df8c9579f5aec429b',
        'release': 'https://github.com/beam-cloud/goproc/releases/tag/v0.1.18',
        'ci_run': 'https://github.com/beam-cloud/goproc/actions/runs/41861454053',
        'sha256_amd64': '206a2532e099eddc36bc7ab34bc524dc321696ef351e783cedd5c11da8048853',
        'sha256_arm64': 'bcaeeb1ad4ac8bb60d50edaac04fe699e04ed524a838cc2c4bf654a912f58309',
    },
    'total_vms': 200,
    'initial_and_restored_proofs_exact': True,
    'fresh_boot_ids_globally_unique': True,
    'all_owned_vms_removed': True,
    'all_phase_budgets_passed': all(r['passed'] for r in runs),
    'all_startup_and_pause_budgets_passed': all(
        p.get('passed', True) for r in runs for p in r['phases']
        if p['phase'] in ('launch', 'pause_0')
    ),
    'failed_budgets': [
        {'cohort': f"n{r['config']['count']}-" + r['run_id'],
         'phase': p['phase'], 'wall_ms': round(p['wall_ms'], 3), 'budget_ms': p['budget_ms']}
        for r in runs for p in r['phases'] if p.get('passed') is False
    ],
    'cohorts': {},
}

def q(values):
    v = sorted(values)
    return {'count': len(v), **{
        name: round(v[math.ceil(len(v) * p) - 1], 3)
        for name, p in [('p50_ms', .5), ('p95_ms', .95), ('p99_ms', .99), ('max_ms', 1)]
    }}

for n in [10, 50]:
    rs = [r for r in runs if r['config']['count'] == n]
    data = {'bursts': len(rs), 'samples': sum(len(r['samples']) for r in rs),
            'per_vm': {}, 'burst_wall_ms': {}}
    for phase in ['launch', 'pause_0', 'restore_0', 'verify_launch', 'verify_restore_0']:
        data['per_vm'][phase] = q([s[phase + '_ms'] for r in rs for s in r['samples']])
        data['burst_wall_ms'][phase] = [round(next(
            p['wall_ms'] for p in r['phases'] if p['phase'] == phase
        ), 3) for r in rs]
    out['cohorts'][str(n)] = data

(root / 'summary.json').write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps(out, indent=2))
