package com.dant.webproject.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.management.MemoryManagerMXBean;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

import com.dant.webproject.dbcomponents.Column;
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
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.dant.webproject.dbcomponents.DataType;
import org.springframework.web.client.RestTemplate;

@Component
public class ParquetService {
  @Autowired
  private final DistributedService distributedService;
  @Autowired
  private final TableModificationService tableModificationService;
  @Autowired
  private RestTemplate restTemplate;

  @Autowired
  private ParquetService(DistributedService distributedService, TableModificationService tableModificationService) {
    this.distributedService = distributedService;
    this.tableModificationService = tableModificationService;
  }

  // private static final Logger logger =
  // LoggerFactory.getLogger(Main.class.getName());

  private String getValueForField(SimpleGroup group, String fieldName, int position) {
    String res = "-";

    try {
      return group.getValueToString(position, 0);
    } catch (Exception e) {
      return res;
    }

    /*
     * int fieldCount = group.getType().getFieldCount();
     * 
     * for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
     * try {
     * if (fieldName == group.getType().getFieldName(fieldIndex)) {
     * res = group.getValueToString(fieldIndex, 0);
     * 
     * break;
     * }
     * } catch (Exception e) {
     * break;
     * }
     * }
     * 
     * return res;
     */
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

  public void parseParquetFile(InputStream inputStream, String tableName) {
    //ExecutorService executor = Executors.newFixedThreadPool(10); // Créer un pool de 2 threads
    ExecutorService executor = new ThreadPoolExecutor(
            5, 40,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>()
    );
    try {
      Files.copy(inputStream, new File("tempFile.parquet").toPath());

      long start = System.currentTimeMillis();
      ParquetFileReader reader = ParquetFileReader.open(
          HadoopInputFile.fromPath(new Path("tempFile.parquet"), new Configuration()));
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();

      int serverIndex;

      List<DataType> types;
      List<String> columns = null;

      List<String> values;
      List<List<String>>[] files = new List[2];
      files[0] = new ArrayList<>();
      files[1] = new ArrayList<>();

      PageReadStore pages;

      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      long end1 = System.currentTimeMillis();
      System.err.println("Time took " + (end1 - start));
      long a = 0, b = 0;

      start = System.currentTimeMillis();

      int batchSize = 12000;
      // 12000; 5 Thread 1 000 000 => 5.71 s
      // 45000 batchSize; 10 Thread pour 3 000 000 de lignes => 20 secondes

      // Runtime.getRuntime().availableProcessors();
      while ((pages = reader.readNextRowGroup()) != null) {

        a++;

        RecordReader recordReader = columnIO.getRecordReader(
            pages,
            new GroupRecordConverter(schema));

        // TODO: replace random number with rows
        SimpleGroup simpleGroup;
        for (int i = 0; i < 1000000; i++) {
          simpleGroup = (SimpleGroup) recordReader.read();
          b++;

          if (i == 0) {
            types = getTypesOfGroup(simpleGroup);
            columns = getFieldNames(simpleGroup);

            // distributedService.insertColDistributed(tableName, columns, types);
            distributedService.createTableColDistributed(tableName, columns, types);
          }

          if (i % 100 == 0) {
            long endParse = System.currentTimeMillis();
            System.out.println((endParse - start) + " ms for " + i);
          }

          values = new ArrayList<>();
          for (int j = 0; j < columns.size(); j++) {
            values.add(getValueForField(simpleGroup, columns.get(j), j));
          }

          serverIndex = i % 3;

          if (serverIndex == 0) {
            tableModificationService.insert(tableName, columns, values);
            values.clear();
            continue;
          }

          files[serverIndex-1].add(values);
          if (files[serverIndex-1].size() >= batchSize) {
              List<List<String>> tmp = files[serverIndex-1];
              final List<String> finalColumns = columns;
              final int s = serverIndex-1;
              executor.submit(() -> sendBatch(tmp, s, tableName, finalColumns));
              files[serverIndex-1] = new ArrayList<>();
          }

//          if (serverIndex == 1) {
//            file2.add(values);
//            if (file2.size() >= batchSize) {
//              List<List<String>> tmp = file2;
//              final List<String> finalColumns = columns;
//              executor.submit(() -> sendBatch(tmp, 0, tableName, finalColumns));
//              file2 = new ArrayList<>();
//            }
//            continue;
//          }
//
//          if (serverIndex == 2) {
//            file3.add(values);
//            if (file3.size() >= batchSize) {
//              List<List<String>> tmp = file3;
//              final List<String> finalColumns = columns;
//              executor.submit(() -> sendBatch(tmp, 1, tableName, finalColumns));
//              file3 = new ArrayList<>();
//            }
//          }
        }
      }

      final List<String> finalColumns = columns;
      if (!files[0].isEmpty()) {
        final List<List<String>> finalFile = files[0];
        executor.submit(() -> sendBatch(finalFile, 0, tableName, finalColumns));
      }

      if (!files[1].isEmpty()) {
        final List<List<String>> finalFile = files[1];
        executor.submit(() -> sendBatch(finalFile, 1, tableName, finalColumns));
      }

      System.err.println(a + " " + b);
      reader.close();
      executor.shutdown();

      try {
        if (!executor.awaitTermination(60, TimeUnit.MINUTES)) {
          executor.shutdownNow();
          if (!executor.awaitTermination(60, TimeUnit.SECONDS))
            System.err.println("Executor did not terminate");
        }
      } catch (InterruptedException ie) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }

      System.out.println("File data has been successfully loaded to database.");
    } catch (IOException e) {
      e.printStackTrace();

      System.out.println("File cannot be loaded to database.");
    } finally {
      try {
        if (inputStream != null) {
          inputStream.close();
        }

      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void sendBatch(List<List<String>> dataBatch, int serverIndex, String tableName, List<String> columns) {
    String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);

    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("col_name", columns);
    requestBody.put("value", dataBatch);

    HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);

    // Construire l'URL pour le point de terminaison d'insertion
    String insertUrl = serverUrls[serverIndex] + "/tableModification/insertMult?table=" + tableName;
    // Effectuer la requête HTTP POST
    restTemplate.exchange(insertUrl, HttpMethod.POST, requestEntity, Void.class);
    dataBatch.clear();
  }

  public void parseParquetFile1(String filePath, String tableName) {
    try {
      long start = System.currentTimeMillis();
      ParquetFileReader reader = ParquetFileReader.open(
          HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();

      int serverIndex;

      List<DataType> types = new ArrayList<>();
      List<String> columns = new ArrayList<>();
      List<String> values;
      // List<List<String>> valuesList = new ArrayList<>();

      HttpHeaders headers = new HttpHeaders();
      headers.setContentType(MediaType.APPLICATION_JSON);

      PageReadStore pages;

      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      long end1 = System.currentTimeMillis();
      System.err.println("Time took " + (end1 - start));
      long a = 0, b = 0;

      start = System.currentTimeMillis();
      while ((pages = reader.readNextRowGroup()) != null) {

        a++;
        // long rows = pages.getRowCount();

        RecordReader recordReader = columnIO.getRecordReader(
            pages,
            new GroupRecordConverter(schema));

        // TODO: replace random number with rows
        for (int i = 0; i < 800; i++) {
          SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
          b++;
          if (i == 0) {
            types = getTypesOfGroup(simpleGroup);
            columns = getFieldNames(simpleGroup);

            distributedService.createTableColDistributed(tableName, columns, types);
          }
          if (i % 100 == 0) {
            long endParse = System.currentTimeMillis();
            System.out.println((endParse - start) + " ms for " + i);
          }

          values = new ArrayList<>();
          for (int j = 0; j < columns.size(); j++) {
            values.add(getValueForField(simpleGroup, columns.get(j), j));
          }

          // System.err.println("Time took for parsing " + (endParse - end1));

          serverIndex = i % 3;
          distributedService.insertRowDistributed(tableName, columns, values, serverIndex);

          // System.err.println("Time took for insert " + (endInsert - endParse));

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
      System.err.println(a + " " + b);
      reader.close();
    } catch (

    IOException e) {
      e.printStackTrace();
    }
  }

  public String uploadFile(InputStream fileStream) throws IOException {
    Files.copy(fileStream, new File("tempFile.parquet").toPath());

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
