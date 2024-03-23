package com.dant.webproject.services;

import com.google.gson.Gson;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.File;
import java.io.IOException;
import java.io.IOException;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.generic.GenericRecord;
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
// import org.apache.avro.generic.GenericRecord;
// import org.apache.parquet.avro.AvroParquetReader;
// import org.apache.parquet.column.page.PageReadStore;
// import org.apache.parquet.example.data.simple.SimpleGroup;
// import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
// import org.apache.parquet.hadoop.ParquetFileReader;
// import org.apache.parquet.hadoop.ParquetReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class DatabaseService {

  // Structure de données pour stocker les tables et les données
  // private Map<String, Map<String, Object>> database = new
  // ConcurrentHashMap<>();

  private static Map<String, Map<String, List<String>>> database = null;

  public static Map<String, Map<String, List<String>>> getDatabase() {
    if (database == null) {
      database = new HashMap<>();
    }

    return database;
  }

  @Autowired
  private RestTemplate restTemplate;

  // Méthode pour effectuer une opération de sélection (SELECT)
  public List<Map<String, List<String>>> select(
    String table,
    List<String> columnNames
  ) {
    List<Map<String, List<String>>> results = new ArrayList<>();

    // Vérifier si la table existe dans la base de données
    if (!database.containsKey(table)) {
      throw new IllegalArgumentException("Table non trouvee dans db");
    }

    // Récupérer les données de la table
    Map<String, List<String>> tableData = database.get(table);

    // Filtrer les données en fonction des conditions
    // On neglige pour l'instant les conditions
    // On renvoie donc toutes les colonnes pour les colonnes indiquées
    for (Map.Entry<String, List<String>> entry : tableData.entrySet()) { // parcours des colonnes de notre table
      Map<String, List<String>> column = new HashMap<>();

      for (String columnName : columnNames) { // parcours des colonnes ındıques dans les param
        // Vérifier si la colonne est présente dans les données de la table
        if (!entry.getKey().equals(columnName)) {
          throw new IllegalArgumentException(
            "La colonne " + columnName + " n'existe pas dans la tbl"
          );
        }

        // Ajouter la colonne et sa valeur à la ligne
        column.put(columnName, entry.getValue());
      }

      results.add(column);
    }

    return results;
  }

  public void add(String tableName, String columnName, String data) {
    if (database.get(tableName) == null) {
      throw new IllegalArgumentException(
        "La table " + tableName + " n'existe pas dans la base de donnees"
      );
    }

    Map<String, List<String>> table = database.get(tableName);

    if (table.get(columnName) == null) {
      throw new IllegalArgumentException(
        "La colonne " + columnName + " n'existe pas dans la tbl"
      );
    }

    List<String> column = table.get(columnName);

    column.add(data);
  }

  public void createTable(String tableName, List<String> columns) {
    if (database.get(tableName) != null) {
      throw new IllegalArgumentException(
        "La table " + tableName + " existe deja dans la base de donnees"
      );
    }

    Map<String, List<String>> table = new HashMap<>();

    for (String column : columns) {
      table.put(column, new ArrayList<>());
    }

    database.put(tableName, table);
  }

  //------------------------Partie serveur youness => etablissement de la logique de communication entre serveurs

  public List<String> getColumnContent(String tableName, String columnName) {
    if (database.containsKey(tableName)) {
      Map<String, List<String>> table = database.get(tableName);
      return table.getOrDefault(columnName, Collections.emptyList());
    }
    return Collections.emptyList(); // Retourne une liste vide si la table ou la colonne n'existe pas
  }

  public List<String> getColumnContentOrFetchRemotely(
    String tableName,
    String columnName
  ) {
    List<String> localValue = getColumnContent(tableName, columnName);
    if (!localValue.isEmpty()) {
      return localValue;
    } else {
      System.out.println("Valeur non trouver dans le serveur 1");
      String[] serverUrls = {
        "http://localhost:8081",
        "http://localhost:8082",
      };
      for (String serverUrl : serverUrls) {
        try {
          String url =
            serverUrl +
            "/api/data?tableName=" +
            tableName +
            "&columnName=" +
            columnName;
          List<String> remoteValue = restTemplate.getForObject(url, List.class);
          if (remoteValue != null && !remoteValue.isEmpty()) {
            System.out.println("Communication avec " + serverUrl);
            return remoteValue;
          }
        } catch (Exception e) {
          e.printStackTrace(); // Handle exception or log it
        }
      }
    }
    return Collections.emptyList(); // Return empty if not found anywhere
  }

  //----------------------------------------------------------------------------------

  //----------travaux DIAKITE 11/03-------------//

  //ajout de méthode pour select toutes les lignes avec un nom donné
  public List<String> select_elem(
    String table,
    String columnName,
    String nomelem
  ) {
    List<String> results = new ArrayList<String>();

    // Vérifier si la table existe dans la base de données
    if (!database.containsKey(table)) {
      throw new IllegalArgumentException(
        "Table non trouvée dans la base de données"
      );
    }

    // Récupérer les données de la table
    Map<String, List<String>> tableData = database.get(table);

    // Vérifier si la colonne existe dans les données de la table
    if (!tableData.containsKey(columnName)) {
      throw new IllegalArgumentException(
        "La colonne " + columnName + " n'existe pas dans la table"
      );
    }

    //récupérer la colonne
    List<String> col = tableData.get(columnName);

    for (String elt : col) {
      if (elt.equals(nomelem)) results.add(elt);
    }

    return results;
  }

  //ajout de méthode pour supprimer une liste de colonnes passé en argument
  public void deleteColumn(String table, List<String> columnNames) {
    // Vérifier si la table existe dans la base de données
    if (!database.containsKey(table)) {
      throw new IllegalArgumentException(
        "Table non trouvée dans la base de données"
      );
    }

    // Récupérer les données de la table
    Map<String, List<String>> tableData = database.get(table);

    for (Map.Entry<String, List<String>> entry : tableData.entrySet()) { // parcours des colonnes de notre table
      for (String columnName : columnNames) { // parcours des colonnes ındıques dans les param
        // Vérifier si la colonne est présente dans les données de la table
        if (!entry.getKey().equals(columnName)) {
          throw new IllegalArgumentException(
            "La colonne " + columnName + " n'existe pas dans la tbl"
          );
        }

        // Supprimer la colonne de la table
        tableData.remove(columnName);
      }
    }
  }

  //mettre à jour une colonne donné
  public void updateColumn(
    String tableName,
    String columnName,
    List<String> newData
  ) {
    // Vérifier si la table existe dans la base de données
    if (!database.containsKey(tableName)) {
      throw new IllegalArgumentException(
        "La table " + tableName + " n'existe pas dans la base de données"
      );
    }

    // Vérifier si la colonne existe dans la table
    Map<String, List<String>> table = database.get(tableName);
    if (!table.containsKey(columnName)) {
      throw new IllegalArgumentException(
        "La colonne " + columnName + " n'existe pas dans la table " + tableName
      );
    }

    // Mettre à jour les données de la colonne
    table.put(columnName, newData);
  }

  //supprimer une cellule selon le nom
  public void deleteRows(String tableName, String columnName, String value) {
    // Vérifier si la table existe dans la base de données
    if (!database.containsKey(tableName)) {
      throw new IllegalArgumentException(
        "La table " + tableName + " n'existe pas dans la base de données"
      );
    }

    // Vérifier si la colonne existe dans la table
    Map<String, List<String>> table = database.get(tableName);
    if (!table.containsKey(columnName)) {
      throw new IllegalArgumentException(
        "La colonne " + columnName + " n'existe pas dans la table " + tableName
      );
    }

    // Supprimer les lignes où la valeur de la colonne correspond à la valeur spécifiée
    List<String> columnData = table.get(columnName);
    /*for(String cel : columnData){
            if(cel.equals(value))columnData.remove(cel);
        }*/
    columnData.removeIf(cel -> cel.equals(value)); //equivalent au code commenté ci-desssus
  }

  //fusionner 2 tables
  public void mergeTables(
    String newTableName,
    String firstTableName,
    String secondTableName
  ) {
    // Vérifier si les tables existent dans la base de données
    if (
      !database.containsKey(firstTableName) ||
      !database.containsKey(secondTableName)
    ) {
      throw new IllegalArgumentException(
        "Les tables spécifiées n'existent pas dans la base de données"
      );
    }

    // Fusionner les données des deux tables
    Map<String, List<String>> newTable = new HashMap<>();
    Map<String, List<String>> firstTable = database.get(firstTableName);
    Map<String, List<String>> secondTable = database.get(secondTableName);

    // Fusionner les colonnes de la première table
    for (Map.Entry<String, List<String>> entry : firstTable.entrySet()) {
      newTable.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }

    // Fusionner les colonnes de la deuxième table
    for (Map.Entry<String, List<String>> entry : secondTable.entrySet()) {
      if (newTable.containsKey(entry.getKey())) {
        // S'il y a une collision de noms de colonne, renommer la colonne de la deuxième table
        newTable.put(
          secondTableName + "_" + entry.getKey(),
          new ArrayList<>(entry.getValue())
        );
      } else {
        newTable.put(entry.getKey(), new ArrayList<>(entry.getValue()));
      }
    }

    // Ajouter la nouvelle table fusionnée à la base de données
    if (database.containsKey(newTableName)) {
      throw new IllegalArgumentException(
        "Une table avec le même nom existe déjà"
      );
    } else {
      database.put(newTableName, newTable);
    }
  }

  //trier selon les valeurs d'une colonne spécifique
  public void sortTablebycolumn(String tableName, String columnName) {
    // Vérifier si la table existe dans la base de données
    if (!database.containsKey(tableName)) {
      throw new IllegalArgumentException(
        "La table " + tableName + " n'existe pas dans la base de données"
      );
    }

    // Vérifier si la colonne existe dans la table
    Map<String, List<String>> table = database.get(tableName);
    if (!table.containsKey(columnName)) {
      throw new IllegalArgumentException(
        "La colonne " + columnName + " n'existe pas dans la table " + tableName
      );
    }

    // Récupérer les données de la colonne à trier
    List<String> columnData = table.get(columnName);

    // Trier les données de la colonne
    columnData.sort(null);
  }

  //joindre 2 tables
  public List<Map<String, List<String>>> joinTables(
    String firstTableName,
    String secondTableName,
    String commonColumn
  ) {
    List<Map<String, List<String>>> results = new ArrayList<>();

    // Vérifier si les tables existent dans la base de données
    if (
      !database.containsKey(firstTableName) ||
      !database.containsKey(secondTableName)
    ) {
      throw new IllegalArgumentException(
        "Les tables spécifiées n'existent pas dans la base de données"
      );
    }

    // Récupérer les données des tables
    Map<String, List<String>> firstTable = database.get(firstTableName);
    Map<String, List<String>> secondTable = database.get(secondTableName);

    // Vérifier si la colonne commune existe dans les deux tables
    if (
      !firstTable.containsKey(commonColumn) ||
      !secondTable.containsKey(commonColumn)
    ) {
      throw new IllegalArgumentException(
        "La colonne commune spécifiée n'existe pas dans les deux tables"
      );
    }

    // Joindre les tables en fonction de la colonne commune
    for (Map.Entry<String, List<String>> firstEntry : firstTable.entrySet()) {
      if (firstEntry.getKey().equals(commonColumn)) {
        continue; // Éviter de traiter la colonne commune elle-même
      }
      for (Map.Entry<String, List<String>> secondEntry : secondTable.entrySet()) {
        if (secondEntry.getKey().equals(commonColumn)) {
          continue; // Éviter de traiter la colonne commune elle-même
        }
        if (firstEntry.getValue().equals(secondEntry.getValue())) {
          // Fusionner les données des deux tables dans un résultat
          Map<String, List<String>> resultRow = new HashMap<>();
          resultRow.putAll(firstTable);
          resultRow.putAll(secondTable);
          results.add(resultRow);
        }
      }
    }

    return results;
  }

  //compter le nombre de lignes
  public int countRows(String tableName) {
    // Vérifier si la table existe dans la base de données
    if (!database.containsKey(tableName)) {
      throw new IllegalArgumentException(
        "La table " + tableName + " n'existe pas dans la base de données"
      );
    }

    // Compter le nombre de lignes dans la table
    Map<String, List<String>> table = database.get(tableName);
    int rowCount = 0;
    for (Map.Entry<String, List<String>> entry : table.entrySet()) {
      rowCount = Math.max(rowCount, entry.getValue().size());
    }
    return rowCount;
  }
  //----------travaux DIAKITE 11/03-------------//

  // TODO : URGENT (read a .parquet file and parse it to json)
  // public void test() throws IllegalArgumentException, IOException {
  // List<SimpleGroup> simpleGroups = new ArrayList<>();
  // ParquetFileReader reader = ParquetFileReader
  // .open(HadoopInputFile.fromPath(new Path("yellow_tripdata_2012-01.parquet"),
  // new Configuration()));
  // MessageType schema = reader.getFooter().getFileMetaData().getSchema();
  // List<Type> fields = schema.getFields();
  // PageReadStore pages;
  // while ((pages = reader.readNextRowGroup()) != null) {
  // long rows = pages.getRowCount();
  // MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
  // RecordReader recordReader = columnIO.getRecordReader(pages, new
  // GroupRecordConverter(schema));

  // for (int i = 0; i < rows; i++) {
  // SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
  // simpleGroups.add(simpleGroup);
  // }
  // }
  // }
}
