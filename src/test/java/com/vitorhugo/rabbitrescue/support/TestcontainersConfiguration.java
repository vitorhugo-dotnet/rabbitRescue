package com.vitorhugo.rabbitrescue.support;

import java.time.Duration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Infraestrutura exclusiva dos testes de integração.
 *
 * O container é um bean do Spring, portanto:
 * 1. sobe antes dos beans que dependem do RabbitMQ;
 * 2. fornece host, porta e credenciais por meio de @ServiceConnection;
 * 3. é encerrado somente depois que o ApplicationContext termina.
 *
 * Assim evitamos portas fixas, Docker Compose paralelo e propriedades manuais.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    RabbitMQContainer rabbitMQContainer() {
        return new RabbitMQContainer(
                DockerImageName.parse("rabbitmq:4-management")
        ).withStartupTimeout(Duration.ofSeconds(90));
    }
}
