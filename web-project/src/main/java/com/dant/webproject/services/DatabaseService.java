package com.dant.webproject.services;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// import org.apache.avro.generic.GenericRecord;
// import org.apache.parquet.avro.AvroParquetReader;
// import org.apache.parquet.column.page.PageReadStore;
// import org.apache.parquet.example.data.simple.SimpleGroup;
// import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
// import org.apache.parquet.hadoop.ParquetFileReader;
// import org.apache.parquet.hadoop.ParquetReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.io.File;

import com.google.gson.Gson;

@Component
public class DatabaseService {

    // Structure de données pour stocker les tables et les données
    // private Map<String, Map<String, Object>> database = new
    // ConcurrentHashMap<>();
    private Map<String, Map<String, List<String>>> database = new HashMap<>();

    // Méthode pour effectuer une opération de sélection (SELECT)
    public List<Map<String, List<String>>> select(String table, List<String> columnNames) {
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
                    throw new IllegalArgumentException("La colonne " + columnName + " n'existe pas dans la tbl");
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
            throw new IllegalArgumentException("La table " + tableName + " n'existe pas dans la base de donnees");
        }

        Map<String, List<String>> table = database.get(tableName);

        if (table.get(columnName) == null) {
            throw new IllegalArgumentException("La colonne " + columnName + " n'existe pas dans la tbl");
        }

        List<String> column = table.get(columnName);

        column.add(data);
    }

    public void createTable(String tableName, List<String> columns) {
        if (database.get(tableName) != null) {
            throw new IllegalArgumentException("La table " + tableName + " existe deja dans la base de donnees");
        }

        Map<String, List<String>> table = new HashMap<>();

        for (String column : columns) {
            table.put(column, new ArrayList<>());
        }

        database.put(tableName, table);
    }

    public Map<String, Map<String, List<String>>> getDatabase() {
        return database;
    }

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
