package com.hussain.kafka.connect.manager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {
		"com.hussain.kafka.connect.manager",  // Your existing connectors code
		"com.hussain.kafka.connect.manager.streams"  // New streams code
})
@EnableScheduling
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}
