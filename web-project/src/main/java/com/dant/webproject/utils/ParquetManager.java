package com.dant.webproject.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.chainsaw.Main;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.beans.factory.annotation.Autowired;

import com.dant.webproject.controllers.DatabaseManagementController;
import com.dant.webproject.dbcomponents.Column;
import com.dant.webproject.services.DatabaseManagementService;
import com.dant.webproject.services.DatabaseService;

public class ParquetManager {
  @Autowired
  private final DatabaseManagementService databaseManagementService;

  @Autowired
  private ParquetManager(DatabaseManagementService databaseManagementService) {
    this.databaseManagementService = databaseManagementService;
  }

  private static final Logger logger = LoggerFactory.getLogger(Main.class.getName());

  private static ParquetManager parquetManager = null;

  public static synchronized ParquetManager getParquetManager(DatabaseManagementService databaseManagementService) {
    if (parquetManager == null) {
      parquetManager = new ParquetManager(databaseManagementService);
    }

    return parquetManager;
  }

  private Map<String, String> getFieldValueMap(SimpleGroup group) {
    Map<String, String> res = new HashMap<>();

    int fieldCount = group.getType().getFieldCount();

    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
      int valueCount = group.getFieldRepetitionCount(fieldIndex);

      Type fieldType = group.getType().getType(fieldIndex);
      String fieldName = fieldType.getName();

      for (int i = 0; i < valueCount; i++) {
        res.put(fieldName, group.getValueToString(fieldIndex, i));
      }
    }

    return res;
  }

  // TODO riu
  private List<String> getValues(SimpleGroup group) {
    List<String> res = new ArrayList<>();

    int fieldCount = group.getType().getFieldCount();

    for (int field = 0; field < fieldCount; field++) {
      int valueCount = group.getFieldRepetitionCount(field);

      if (valueCount == 0) {
        res.add("-");
      } else {
        res.add(group.getValueToString(field, 0));
      }
    }

    return res;
  }

  // TODO riu
  private String getValueForField(SimpleGroup group, String fieldName) {
    String res = "-";

    int fieldCount = group.getType().getFieldCount();

    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
      if (fieldName == group.getType().getFieldName(fieldIndex)) {
        res = group.getValueToString(fieldIndex, 0);

        break;
      }
    }

    return res;
  }

  // TODO riu
  private List<String> getFieldNames(SimpleGroup group) {
    int fieldCount = group.getType().getFieldCount();

    List<String> res = new ArrayList<>();

    for (int field = 0; field < fieldCount; field++) {
      Type fieldType = group.getType().getType(field);
      String fieldName = fieldType.getName();
      res.add(fieldName);
    }

    return res;
  }

  // TODO riu
  public void readParquetFile(String filePath) {
    List<SimpleGroup> simpleGroups = new ArrayList<>();

    try {
      ParquetFileReader reader = ParquetFileReader.open(
          HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      List<Type> fields = schema.getFields();
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        long rows = pages.getRowCount();
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        RecordReader recordReader = columnIO.getRecordReader(
            pages,
            new GroupRecordConverter(schema));
        for (int i = 0; i < 2; i++) {
          SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
          simpleGroup.getFieldRepetitionCount(0);
          simpleGroups.add(simpleGroup);
        }
      }
      reader.close();
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
  }

  public void parseParquetFile(String filePath, String tableName) {
    try {
      ParquetFileReader reader = ParquetFileReader.open(
          HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      List<Type> fields = schema.getFields();

      // TODO: change variable name for more comprehension
      Map<String, List<String>> fieldMap = new HashMap<String, List<String>>();

      for (Type field : fields) {
        fieldMap.put(field.toString().split("\\s")[2], new ArrayList<>());
      }

      // DatabaseService.getDatabase().put(tableName, fieldMap);
      databaseManagementService.createTable(tableName);

      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        long rows = pages.getRowCount();

        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        RecordReader recordReader = columnIO.getRecordReader(
            pages,
            new GroupRecordConverter(schema));

        // TODO: replace 1 with rows
        for (int i = 0; i < 1; i++) {
          SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();

          for (String fieldName : fieldMap.keySet()) {
            String value = getFieldValueMap(simpleGroup).get(fieldName);
            String valueToAdd = value == null ? "-" : value;

            fieldMap.get(fieldName).add(valueToAdd);
          }
        }

        // String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };

        // for (String serverUrl : serverUrls) {
        // try {
        // String url = serverUrl + "/select/selectallfrom?tableName=" + tableName;

        // // Utilisation de ParameterizedTypeReference pour la désérialisation correcte
        // Map<String, List<Object>> result = restTemplate.exchange(url, HttpMethod.GET,
        // null, new ParameterizedTypeReference<Map<String, List<Object>>>() {
        // }).getBody();
        // result.forEach((key, val) -> value.get(key).addAll(val));

        // } catch (Exception e) {
        // e.printStackTrace(); // Handle exception or log it
        // }
        // }

        System.out.println(DatabaseService.getDatabase());
      }
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public String uploadFile(InputStream fileStream) throws IOException {
    Files.copy(fileStream, new File("tempFile.parquet").toPath());

    // logger.info("Uploaded to database.");

    return "tempFile.parquet";
  }

  public void deleteFile(String filePath) throws IOException {
    Files.delete(new File(filePath).toPath());
  }
}
