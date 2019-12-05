package com.ywf;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class YwfStormApplication {
    private static ConfigurableApplicationContext context = null;

    public static synchronized void run(String... args) {
        if (null == context) {
            context = SpringApplication.run(YwfStormApplication.class, args);
        }
    }
}
