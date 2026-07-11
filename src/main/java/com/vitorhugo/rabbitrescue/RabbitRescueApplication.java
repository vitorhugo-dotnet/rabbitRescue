package com.vitorhugo.rabbitrescue;

import com.vitorhugo.rabbitrescue.config.MessagingProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@EnableConfigurationProperties(MessagingProperties.class)
@SpringBootApplication
public class RabbitRescueApplication {

    public static void main(String[] args) {
        SpringApplication.run(RabbitRescueApplication.class, args);
    }

}
