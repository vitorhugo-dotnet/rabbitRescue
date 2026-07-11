import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';
import {
  assertApplicationIsUp,
  buildRunId,
  envPositiveInt,
  listDeadLetters,
  publishNotification,
} from './lib/client.js';

const dlqVerification = new Rate('dlq_verification');
const rate = envPositiveInt('RATE', 10);
const preAllocatedVUs = envPositiveInt('PRE_ALLOCATED_VUS', 20);
const maxVUs = envPositiveInt('MAX_VUS', 100);

/**
 * Teste de confiabilidade com uma mistura aproximada de:
 *   80% SUCCESS
 *   15% FLAKY
 *    5% INVALID
 *
 * INVALID deve aparecer na DLQ. FLAKY deve entrar em retry e depois processar,
 * mas o MVP ainda não expõe um endpoint GET por messageId para comprovar o
 * estado final externamente. Essa parte segue coberta pelos Testcontainers.
 */
export const options = {
  scenarios: {
    reliability: {
      executor: 'constant-arrival-rate',
      rate,
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs,
      maxVUs,
      gracefulStop: '15s',
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
    accepted_notifications: ['rate>0.99'],
    dlq_verification: ['rate==1'],
    'http_req_failed{endpoint:notifications}': ['rate<0.01'],
    'http_req_duration{endpoint:notifications}': [
      'p(95)<750',
      'p(99)<1500',
    ],
    dropped_iterations: ['count==0'],
  },
};

export function setup() {
  assertApplicationIsUp();
  return { runId: buildRunId('reliability') };
}

export default function (data) {
  publishNotification(selectMode(), data.runId);
}

export function teardown(data) {
  const settleSeconds = envPositiveInt('SETTLE_SECONDS', 5);
  const verifyAttempts = envPositiveInt('VERIFY_ATTEMPTS', 5);
  const verifyIntervalSeconds = envPositiveInt('VERIFY_INTERVAL_SECONDS', 2);

  // Dá tempo para o consumer encaminhar as poison messages para a DLQ.
  sleep(settleSeconds);

  let responseStatus = 0;
  let matchingDeadLetters = [];

  for (let attempt = 1; attempt <= verifyAttempts; attempt += 1) {
    const result = listDeadLetters(100);
    responseStatus = result.response.status;

    if (responseStatus === 200 && Array.isArray(result.body)) {
      matchingDeadLetters = result.body.filter((item) => (
        item !== null
        && typeof item.correlationId === 'string'
        && item.correlationId.startsWith(data.runId)
        && item.payload !== null
        && item.payload.processingMode === 'INVALID'
      ));
    }

    if (matchingDeadLetters.length > 0) {
      break;
    }

    if (attempt < verifyAttempts) {
      sleep(verifyIntervalSeconds);
    }
  }

  const verified = responseStatus === 200 && matchingDeadLetters.length > 0;

  check({ responseStatus, matchingDeadLetters }, {
    'consulta da DLQ retorna 200': (result) => result.responseStatus === 200,
    'mensagem INVALID do teste chegou à DLQ': (result) => (
      result.matchingDeadLetters.length > 0
    ),
    'DLQ preservou correlationId e erro': (result) => (
      result.matchingDeadLetters.length > 0
      && result.matchingDeadLetters.every((item) => (
        item.correlationId.startsWith(data.runId)
        && typeof item.errorType === 'string'
        && item.errorType.length > 0
        && item.attempt >= 1
      ))
    ),
  }, { endpoint: 'dead-letters' });

  dlqVerification.add(verified);
}

function selectMode() {
  // __ITER é local ao VU. Somar __VU distribui melhor os modos entre VUs.
  const bucket = (__ITER + __VU) % 20;

  if (bucket === 0) {
    return 'INVALID';
  }

  if (bucket <= 3) {
    return 'FLAKY';
  }

  return 'SUCCESS';
}
