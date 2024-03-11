package com.dant.webproject;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import com.dant.webproject.services.DatabaseService;

@SpringBootApplication
public class WebProjectApplication {

	public static void main(String[] args) {
		SpringApplication.run(WebProjectApplication.class, args);
	}

	@Bean
	CommandLineRunner commandLineRunner(DatabaseService databaseService, Environment env) {
		return args -> {
			List<String> columnNames = new ArrayList<>();

			/*------------------Partie serveur gerer par Youness---------------*/

			columnNames.add("youness");
			columnNames.add("diakite");
			columnNames.add("baran");

			databaseService.createTable("test", columnNames);

			databaseService.add("test", "baran", "i love coding!!!");

			// Récupération du port actuel à partir de l'environnement
			String serverPort = env.getProperty("local.server.port");
			String name = env.getProperty("spring.profiles.active");

			//Test pour la repartition des donnée en fonction du serveur
			//Fonctionne trés bien
			if("8081".equals(serverPort)) {
				System.out.println("BON, on est bien sur le port "+serverPort);
				System.out.println(name);
			}

		};
	};
}