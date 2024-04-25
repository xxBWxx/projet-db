package com.dant.webproject.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import com.dant.webproject.dbcomponents.DataType;

@Component
public class ParquetService {
  @Autowired
  private final DistributedService distributedService;

  @Autowired
  private ParquetService(DistributedService distributedService) {
    this.distributedService = distributedService;
  }

  // private static final Logger logger =
  // LoggerFactory.getLogger(Main.class.getName());

  private String getValueForField(SimpleGroup group, String fieldName) {
    String res = "-";

    int fieldCount = group.getType().getFieldCount();

    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
      try {
        if (fieldName == group.getType().getFieldName(fieldIndex)) {
          res = group.getValueToString(fieldIndex, 0);

          break;
        }
      } catch (Exception e) {
        break;
      }
    }

    return res;
  }

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

  public void parseParquetFile(String filePath, String tableName) {
    try {
      ParquetFileReader reader = ParquetFileReader.open(
          HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();

      distributedService.createTableDistributed(tableName);

      int serverIndex;

      List<DataType> types = new ArrayList<>();
      List<String> columns = new ArrayList<>();
      List<String> values;
      // List<List<String>> valuesList = new ArrayList<>();

      HttpHeaders headers = new HttpHeaders();
      headers.setContentType(MediaType.APPLICATION_JSON);

      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        long rows = pages.getRowCount();

        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        RecordReader recordReader = columnIO.getRecordReader(
            pages,
            new GroupRecordConverter(schema));

        // TODO: replace random number with rows
        for (int i = 0; i < 100; i++) {
          SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();

          if (i == 0) {
            types = getTypesOfGroup(simpleGroup);
            columns = getFieldNames(simpleGroup);

            distributedService.createTableColDistributed(tableName, columns, types);
          }

          values = new ArrayList<>();
          for (int j = 0; j < columns.size(); j++) {
            values.add(getValueForField(simpleGroup, columns.get(j)));
          }

          serverIndex = i % 3;
          distributedService.insertRowDistributed(tableName, columns, values, serverIndex);

          // if (serverIndex == 0) {
          // tableModificationService.insert(tableName, columns, values);
          // }

          // else {
          // String url = serverUrls[serverIndex] + "/tableModification/insert?tableName="
          // + tableName;

          // // Construire le corps de la requête
          // Map<String, Object> requestBody = new HashMap<>();
          // requestBody.put("col_name", columns);
          // requestBody.put("value", values);

          // HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody,
          // headers);

          // // Effectuer la requête HTTP POST
          // restTemplate.exchange(url, HttpMethod.POST, requestEntity, Void.class);
          // }

          // valuesList.add(values);
        }

        // distributedService.insertDistributed(tableName, columns, valuesList);
      }
      reader.close();
    } catch (

    IOException e) {
      e.printStackTrace();
    }
  }

  public String uploadFile(InputStream fileStream) throws IOException {
    Files.copy(fileStream, new File("tempFile.parquet").toPath());

    // logger.info("...");

    return "tempFile.parquet";
  }

  public void deleteFile(String filePath) throws IOException {
    Files.delete(new File(filePath).toPath());
  }

  private List<DataType> getTypesOfGroup(SimpleGroup simpleGroup) {
    List<DataType> res = new ArrayList<>();

    String groupStr = simpleGroup.getType().toString().replace("ı", "i");

    try (BufferedReader reader = new BufferedReader(new StringReader(groupStr))) {
      String previousLine = null;
      boolean firstLineSkipped = false;

      String line;
      while ((line = reader.readLine()) != null) {
        if (!firstLineSkipped) {
          firstLineSkipped = true;

          continue;
        }

        if (previousLine != null) {
          if (previousLine.contains("TIMESTAMP_MICROS")) {
            res.add(DataType.DATETIME_STRING);
          }

          else if (previousLine.contains(" int32 ") || previousLine.contains(" int64 ")) {
            res.add(DataType.INTEGER);
          }

          else if (previousLine.contains(" double ")) {
            res.add(DataType.DOUBLE);
          }

          else {
            res.add(DataType.STRING);
          }
        }

        previousLine = line;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return res;
  }
}
