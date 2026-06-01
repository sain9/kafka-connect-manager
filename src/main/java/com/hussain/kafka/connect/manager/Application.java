package com.hussain.kafka.connect.manager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {
		"com.hussain.kafka.connect.manager",  // Your existing connectors code
		"com.hussain.kafka.connect.manager.streams"  // New streams code
})
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}
