import http from 'k6/http';
import { check, fail } from 'k6';
import { Rate } from 'k6/metrics';

/**
 * Percentual de notificações aceitas corretamente pela API.
 *
 * Essa métrica é separada de http_req_failed porque também valida o contrato
 * retornado pelo RabbitRescue: status 202, messageId, correlationId e PUBLISHED.
 */
export const acceptedNotifications = new Rate('accepted_notifications');

/**
 * URL base da aplicação.
 *
 * Exemplos:
 *   BASE_URL=http://localhost:8080
 *   BASE_URL=http://host.docker.internal:8080
 */
export function getBaseUrl() {
  return (__ENV.BASE_URL || 'http://localhost:8080').replace(/\/+$/, '');
}

/**
 * Lê um inteiro de variável de ambiente sem permitir zero ou valor inválido.
 */
export function envPositiveInt(name, fallback) {
  const parsed = Number.parseInt(__ENV[name], 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

/**
 * Identifica uma execução inteira do k6.
 *
 * O prefixo também permite localizar, na DLQ, somente mensagens criadas pelo
 * teste atual, sem confundir com mensagens antigas deixadas por seres humanos.
 */
export function buildRunId(testName) {
  return `k6-${testName}-${Date.now()}`;
}

/**
 * Gera um correlationId único por iteração e preservável entre retries.
 */
export function buildCorrelationId(runId, mode) {
  return `${runId}-${mode.toLowerCase()}-vu${__VU}-iter${__ITER}-${Date.now()}`;
}

/**
 * Publica uma notificação usando o mesmo contrato exposto pela aplicação.
 */
export function publishNotification(mode, runId) {
  const correlationId = buildCorrelationId(runId, mode);
  const payload = JSON.stringify({
    recipients: ['load-test@example.com'],
    subject: `[k6] ${mode} - ${runId}`,
    content: 'Mensagem criada pelo teste de carga do RabbitRescue.',
    processingMode: mode,
  });

  const response = http.post(`${getBaseUrl()}/notifications`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'X-Correlation-Id': correlationId,
    },
    tags: {
      endpoint: 'notifications',
      mode,
      name: 'POST /notifications',
    },
  });

  const body = safeJson(response);
  const accepted = response.status === 202
    && body !== null
    && typeof body.messageId === 'string'
    && body.messageId.length > 0
    && body.correlationId === correlationId
    && body.status === 'PUBLISHED';

  check(response, {
    'POST /notifications retorna 202': (result) => result.status === 202,
  }, { endpoint: 'notifications', mode });

  check(body, {
    'resposta possui messageId': (result) => result !== null
      && typeof result.messageId === 'string'
      && result.messageId.length > 0,
    'correlationId foi preservado': (result) => result !== null
      && result.correlationId === correlationId,
    'status inicial é PUBLISHED': (result) => result !== null
      && result.status === 'PUBLISHED',
  }, { endpoint: 'notifications', mode });

  acceptedNotifications.add(accepted, { mode });

  return {
    response,
    body,
    correlationId,
  };
}

/**
 * Interrompe o teste cedo quando a aplicação não está disponível.
 */
export function assertApplicationIsUp() {
  const response = http.get(`${getBaseUrl()}/actuator/health`, {
    tags: {
      endpoint: 'health',
      name: 'GET /actuator/health',
    },
  });

  const body = safeJson(response);
  const healthy = response.status === 200
    && body !== null
    && body.status === 'UP';

  check(response, {
    'aplicação está disponível': () => healthy,
  }, { endpoint: 'health' });

  if (!healthy) {
    fail(`RabbitRescue indisponível em ${getBaseUrl()}. Status HTTP: ${response.status}`);
  }
}

/**
 * Consulta a DLQ sem remover definitivamente as mensagens.
 * O próprio backend usa basic.get e devolve as entregas ao broker.
 */
export function listDeadLetters(limit = 100) {
  const response = http.get(
    `${getBaseUrl()}/notifications/dead-letters?limit=${limit}`,
    {
      tags: {
        endpoint: 'dead-letters',
        name: 'GET /notifications/dead-letters',
      },
    },
  );

  return {
    response,
    body: safeJson(response),
  };
}

function safeJson(response) {
  try {
    return response.json();
  } catch (_error) {
    return null;
  }
}
