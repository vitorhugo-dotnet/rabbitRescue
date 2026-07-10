package com.vitorhugo.rabbitrescue;

import org.springframework.boot.SpringApplication;

public class TestRabbitRescueApplication {

    public static void main(String[] args) {
        SpringApplication.from(RabbitRescueApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
