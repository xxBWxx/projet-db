package com.dant.webproject;

import com.dant.webproject.services.DatabaseService;
import com.dant.webproject.utils.ParquetReader;
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
      /*------------------Partie serveur gerer par Youness---------------*/

      // Récupération du port actuel à partir de l'environnement
      String serverPort = env.getProperty("local.server.port");
      String name = env.getProperty("spring.profiles.active");

      List col= new ArrayList<>();
      col.add("col1");
      col.add("col2");
      col.add("col3");
      col.add("col4");
      col.add("col5");
      col.add("col6");
      col.add("col7");
      col.add("col8");
      col.add("col9");
      col.add("col10");
      col.add("col11");
      col.add("col12");
      col.add("col13");
      databaseService.createTable("test", col);

      //Test pour la repartition des donnée en fonction du serveur
      //Fonctionne trés bien
      /*
				On sera toujours branché au serveur 1 de port 8080.

				On a pas de clé donc on ne peux pas faire Sharding

				Requete envoyé sur le serveur 1

				Si elle n'est pas trouver, il va communiquer avec les serveur 2 et 3
				en utilisant des RestTemplate pour faire des appels HTTP aux endpoints des serveurs 2 et 3

			 */
      if ("8080".equals(serverPort)) {
        System.out.println("BON, on est bien sur le port " + serverPort);
        System.out.println(name);

        long startTime = System.currentTimeMillis();

        for(int i=0; i<8000; i++){
          databaseService.add("test", "col1", "serveur1");
          databaseService.add("test", "col2", "serveur1");
          databaseService.add("test", "col3", "serveur1");
          databaseService.add("test", "col4", "serveur1");
          databaseService.add("test", "col5", "serveur1");
          databaseService.add("test", "col6", "serveur1");
          databaseService.add("test", "col7", "serveur1");
          databaseService.add("test", "col8", "serveur1");
          databaseService.add("test", "col9", "serveur1");
          databaseService.add("test", "col10", "serveur1");
          databaseService.add("test", "col11", "serveur1");
          databaseService.add("test", "col12", "serveur1");
          databaseService.add("test", "col13", "serveur1");
        }
        long endTime = System.currentTimeMillis();
        // Calculer la durée
        long duration = endTime - startTime;
        // Afficher la durée en millisecondes
        System.out.println("Le chargement des données a pris " + duration + " millisecondes.");

        // TODO: change file path
        /*
        ParquetReader.parseParquetFile(
          "C:\\Users\\SA\\Desktop\\SU\\L3\\S2\\Web\\projet-db\\web-project\\src\\main\\java\\com\\dant\\webproject\\yellow_tripdata_2012-01.parquet"
        );

         */
      }

      if ("8081".equals(serverPort)) {
        System.out.println("BON, on est bien sur le port " + serverPort);
        System.out.println(name);

        long startTime = System.currentTimeMillis();

        for(int i=0; i<500000; i++){
          databaseService.add("test", "col1", "serveur2");
          databaseService.add("test", "col2", "serveur2");
          databaseService.add("test", "col3", "serveur2");
          databaseService.add("test", "col4", "serveur2");
          databaseService.add("test", "col5", "serveur2");
          databaseService.add("test", "col6", "serveur2");
          databaseService.add("test", "col7", "serveur2");
          databaseService.add("test", "col8", "serveur2");
          databaseService.add("test", "col9", "serveur2");
          databaseService.add("test", "col10", "serveur2");
          databaseService.add("test", "col11", "serveur2");
          databaseService.add("test", "col12", "serveur2");
          databaseService.add("test", "col13", "serveur2");
        }
        long endTime = System.currentTimeMillis();
        // Calculer la durée
        long duration = endTime - startTime;
        // Afficher la durée en millisecondes
        System.out.println("Le chargement des données a pris " + duration + " millisecondes.");
      }

      if ("8082".equals(serverPort)) {
        System.out.println("BON, on est bien sur le port " + serverPort);
        System.out.println(name);
        long startTime = System.currentTimeMillis();
        for(int i=0; i<500000; i++){

          databaseService.add("test", "col1", "serveur3");
          databaseService.add("test", "col2", "serveur3");
          databaseService.add("test", "col3", "serveur3");
          databaseService.add("test", "col4", "serveur3");
          databaseService.add("test", "col5", "serveur3");
          databaseService.add("test", "col6", "serveur3");
          databaseService.add("test", "col7", "serveur3");
          databaseService.add("test", "col8", "serveur3");
          databaseService.add("test", "col9", "serveur3");
          databaseService.add("test", "col10", "serveur3");
          databaseService.add("test", "col11", "serveur3");
          databaseService.add("test", "col12", "serveur3");
          databaseService.add("test", "col13", "serveur3");
        }
        long endTime = System.currentTimeMillis();
        // Calculer la durée
        long duration = endTime - startTime;
        // Afficher la durée en millisecondes
        System.out.println("Le chargement des données a pris " + duration + " millisecondes.");
      }
    };
  }
}
