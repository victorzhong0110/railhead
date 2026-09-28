import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    streams: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 10),
      duration: __ENV.DURATION || '20s',
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
};

export function setup() {
  const res = http.post(
    `${__ENV.BASE_URL}/admin/keys`,
    JSON.stringify({
      name: `k6-stream-${Date.now()}`,
      quota: Number(__ENV.QUOTA || 5000000),
      rpm_limit: 0,
      tpm_limit: 0,
      concurrency_limit: 0,
    }),
    {
      headers: {
        'Content-Type': 'application/json',
        'X-Admin-Token': __ENV.ADMIN_TOKEN,
      },
    },
  );
  if (res.status !== 201) {
    throw new Error(`create key failed: ${res.status} ${res.body}`);
  }
  return { key: res.json('key') };
}

export default function (data) {
  const payload = JSON.stringify({
    model: __ENV.MODEL || 'railhead-mock',
    messages: [{ role: 'user', content: 'Stream a short reply.' }],
    max_tokens: Number(__ENV.MAX_TOKENS || 16),
    stream: true,
  });
  const res = http.post(`${__ENV.BASE_URL}/v1/chat/completions`, payload, {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${data.key}`,
    },
  });
  check(res, {
    'status is 200': (r) => r.status === 200,
    'sse done': (r) => String(r.body).indexOf('data: [DONE]') !== -1,
  });
}
