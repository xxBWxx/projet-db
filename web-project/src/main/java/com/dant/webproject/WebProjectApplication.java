package com.dant.webproject;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.dant.webproject.services.DatabaseService;

@SpringBootApplication
public class WebProjectApplication {

	public static void main(String[] args) {
		SpringApplication.run(WebProjectApplication.class, args);
	}

	@Bean
	CommandLineRunner commandLineRunner(DatabaseService databaseService) {
		return args -> {
			List<String> columnNames = new ArrayList<>();

			columnNames.add("youness");
			columnNames.add("diakite");
			columnNames.add("baran");

			databaseService.createTable("test", columnNames);

			databaseService.add("test", "baran", "i love coding!!!");

			//Rajout de la condition pour les repartition des donnée


		};
	};
}