import http from 'k6/http';
import { check } from 'k6';

// 多把密钥分摊额度行锁。单密钥的行锁上限另外用 KEY_COUNT=1 测。
const KEY_COUNT = Number(__ENV.KEY_COUNT || 32);
const QUOTA = Number(__ENV.QUOTA || 5000000);
const MAX_TOKENS = Number(__ENV.MAX_TOKENS || 16);

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 20),
      duration: __ENV.DURATION || '30s',
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
};

export function setup() {
  const base = __ENV.BASE_URL;
  const admin = __ENV.ADMIN_TOKEN;
  const keys = [];
  for (let i = 0; i < KEY_COUNT; i++) {
    const res = http.post(
      `${base}/admin/keys`,
      JSON.stringify({
        name: `k6-${Date.now()}-${i}`,
        quota: QUOTA,
        rpm_limit: 0,
        tpm_limit: 0,
        concurrency_limit: 0,
      }),
      { headers: { 'Content-Type': 'application/json', 'X-Admin-Token': admin } },
    );
    if (res.status !== 201) {
      throw new Error(`create key failed: ${res.status} ${res.body}`);
    }
    keys.push(res.json('key'));
  }
  return { keys };
}

export default function (data) {
  const key = data.keys[(__VU - 1) % data.keys.length];
  const payload = JSON.stringify({
    model: __ENV.MODEL || 'railhead-mock',
    messages: [{ role: 'user', content: 'Say hello in a short sentence.' }],
    max_tokens: MAX_TOKENS,
    temperature: 0.7,
  });
  const res = http.post(`${__ENV.BASE_URL}/v1/chat/completions`, payload, {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${key}`,
    },
  });
  check(res, { 'status is 200': (r) => r.status === 200 });
}
