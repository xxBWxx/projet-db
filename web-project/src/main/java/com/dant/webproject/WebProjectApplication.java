package com.dant.webproject;

import com.dant.webproject.services.DatabaseService;
import com.dant.webproject.utils.ParquetManager;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@SpringBootApplication
public class WebProjectApplication {

  public static void main(String[] args) {
    SpringApplication.run(WebProjectApplication.class, args);
  }

  @Bean
  CommandLineRunner commandLineRunner(
    DatabaseService databaseService,
    Environment env
  ) {
    return args -> {

      // Récupération du port actuel à partir de l'environnement
      String serverPort = env.getProperty("local.server.port");
      String name = env.getProperty("spring.profiles.active");

      //Test pour la repartition des donnée en fonction du serveur
      //Fonctionne trés bien
      /*
				On sera toujours branché au serveur 1 de port 8080.

				On a pas de clé donc on ne peux pas faire Sharding

				Requete envoyé sur le serveur 1

				Si elle n'est pas trouver, il va communiquer avec les serveur 2 et 3
				en utilisant des RestTemplate pour faire des appels HTTP aux endpoints des serveurs 2 et 3

			 */

      System.out.println("---------------------------------------------");
      System.out.println("Port: " + serverPort);
      System.out.println("Server name: " +name);
      System.out.println("---------------------------------------------");

    };
  }
}
