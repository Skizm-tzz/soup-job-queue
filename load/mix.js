// Open-loop load against the standard stack: k6's constant-arrival-rate executor
// starts iterations on schedule whether or not earlier responses came back, so a
// slow server shows up as latency and dropped iterations, never as a slower client.
//
// Mix: 60% create, 30% get, 5% list, 5% cancel.
// Run (see README): docker compose --profile load run --rm -e RUN=<id> k6 run /load/mix.js

import http from 'k6/http';
import { Counter } from 'k6/metrics';

const BASE = __ENV.API_URL || 'http://api:8000';
const RUN = __ENV.RUN;
const RATE = Number(__ENV.RATE || 100);
const DURATION = __ENV.DURATION || '10m';
const WORK_MIN = Number(__ENV.WORK_MIN || 5);
const WORK_MAX = Number(__ENV.WORK_MAX || 40);

if (!RUN) throw new Error('RUN is required: it prefixes every Idempotency-Key and names the results directory');

const ops = ['create', 'get', 'list', 'cancel'];
const thresholds = {};
for (const op of ops) thresholds[`http_req_duration{op:${op}}`] = ['max>=0']; // always true; makes k6 report each op

export const options = {
  scenarios: {
    mix: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 200,
      maxVUs: 2000,
    },
  },
  thresholds,
  summaryTrendStats: ['p(50)', 'p(95)', 'p(99)', 'max', 'count'],
};

// One counter per (op, status class); created at init time as k6 requires.
const status = {};
for (const op of ops) for (const s of ['2xx', '4xx', '5xx', 'error']) status[`${op}_${s}`] = new Counter(`status_${op}_${s}`);

function record(op, res) {
  const s = res.status === 0 ? 'error' : `${Math.floor(res.status / 100)}xx`;
  status[`${op}_${s}`].add(1);
}

const created = []; // ids this VU created; k6 VUs share no memory

function create() {
  const key = `${RUN}-${__VU}-${__ITER}`;
  const body = JSON.stringify({ work_ms: WORK_MIN + Math.floor(Math.random() * (WORK_MAX - WORK_MIN + 1)), outcome: 'ok' });
  const res = http.post(`${BASE}/jobs`, body, {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    tags: { op: 'create' },
  });
  record('create', res);
  if (res.status === 201 || res.status === 200) created.push(res.json('id'));
}

export default function () {
  const r = Math.random();
  if (r < 0.6 || created.length === 0) return create();
  const id = created[Math.floor(Math.random() * created.length)];
  if (r < 0.9) {
    record('get', http.get(`${BASE}/jobs/${id}`, { tags: { op: 'get', name: 'GET /jobs/{id}' } }));
  } else if (r < 0.95) {
    const st = ['', '&state=queued', '&state=running', '&state=succeeded'][Math.floor(Math.random() * 4)];
    record('list', http.get(`${BASE}/jobs?limit=50${st}`, { tags: { op: 'list', name: 'GET /jobs' } }));
  } else {
    const recent = created[Math.max(0, created.length - 1 - Math.floor(Math.random() * 20))];
    record('cancel', http.post(`${BASE}/jobs/${recent}/cancel`, null, { tags: { op: 'cancel', name: 'POST /jobs/{id}/cancel' } }));
  }
}

export function handleSummary(data) {
  const out = {};
  out[`/results/load/${RUN}/k6-summary.json`] = JSON.stringify(data, null, 2);
  const m = data.metrics;
  const lines = [`k6 ${RUN}: target ${RATE}/s for ${DURATION}`];
  for (const op of ops) {
    const t = m[`http_req_duration{op:${op}}`];
    if (t) lines.push(`  ${op.padEnd(7)} p50 ${t.values['p(50)'].toFixed(1)} ms  p95 ${t.values['p(95)'].toFixed(1)}  p99 ${t.values['p(99)'].toFixed(1)}  max ${t.values.max.toFixed(1)}  n ${t.values.count}`);
  }
  lines.push(`  requests ${m.http_reqs.values.count} (${m.http_reqs.values.rate.toFixed(1)}/s), dropped iterations ${m.dropped_iterations ? m.dropped_iterations.values.count : 0}, max VUs ${m.vus_max.values.max}`);
  const st = Object.keys(m).filter((k) => k.startsWith('status_')).map((k) => `${k.slice(7)}=${m[k].values.count}`);
  lines.push(`  statuses ${st.join(' ')}`);
  out.stdout = lines.join('\n') + '\n';
  return out;
}
