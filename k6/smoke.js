import {
  assertApplicationIsUp,
  buildRunId,
  publishNotification,
} from './lib/client.js';

/**
 * Smoke test: valida rapidamente se o caminho HTTP -> RabbitMQ está funcional.
 */
export const options = {
  scenarios: {
    smoke: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 5,
      maxDuration: '30s',
    },
  },
  thresholds: {
    checks: ['rate==1'],
    accepted_notifications: ['rate==1'],
    'http_req_failed{endpoint:notifications}': ['rate==0'],
    'http_req_duration{endpoint:notifications}': ['p(95)<500'],
  },
};

export function setup() {
  assertApplicationIsUp();
  return { runId: buildRunId('smoke') };
}

export default function (data) {
  publishNotification('SUCCESS', data.runId);
}
