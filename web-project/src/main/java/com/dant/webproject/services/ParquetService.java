package com.dant.webproject.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    try{
      return group.getValueToString(position, 0);
    } catch(Exception e) {
      return res;
    }


    /*int fieldCount = group.getType().getFieldCount();

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

    return res;*/
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
    ExecutorService executor = Executors.newFixedThreadPool(4);  // Créer un pool de 2 threads
    try {
      long start = System.currentTimeMillis();
      ParquetFileReader reader = ParquetFileReader.open(
          HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();

      int serverIndex;

      List<DataType> types;
      List<String> columns = null;

      List<String> values;
      List<List<String>> file2 = new ArrayList<>();
      List<List<String>> file3 = new ArrayList<>();


      PageReadStore pages;

      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      long end1 = System.currentTimeMillis();
      System.err.println("Time took " + (end1 - start));
      long a = 0, b = 0;

      start = System.currentTimeMillis();

      int batchSize = 3000;
      //12000; 5 Thread 1 000 000 => 5.71 s
      //45000 batchSize; 10 Thread pour 3 000 000 de lignes => 20 secondes

      while ((pages = reader.readNextRowGroup()) != null) {

        a++;

        RecordReader recordReader = columnIO.getRecordReader(
            pages,
            new GroupRecordConverter(schema));

        // TODO: replace random number with rows
        SimpleGroup simpleGroup;
        for (int i = 0; i < 30000; i++) {
          simpleGroup = (SimpleGroup) recordReader.read();
          b++;
          if (i == 0) {
            types = getTypesOfGroup(simpleGroup);
            columns = getFieldNames(simpleGroup);

            distributedService.createTableColDistributed(tableName, columns, types);
          }
          if(i % 100 == 0) {
            long endParse = System.currentTimeMillis();
            System.out.println((endParse - start) + " ms for " + i);
          }

          values = new ArrayList<>();
          for (int j = 0; j < columns.size(); j++) {
            values.add(getValueForField(simpleGroup, columns.get(j), j));
          }
          serverIndex = i % 3;
          if(serverIndex == 0){
//            final List<String> finalColumns = columns;
//            final List<String> finalVal = values;
//            executor.submit(() -> tableModificationService.insert(tableName, finalColumns, finalVal));
            tableModificationService.insert(tableName, columns, values);
            continue;
          }
          if(serverIndex == 1){
            file2.add(values);
            if(file2.size()>=batchSize){
              List<List<String>> tmp = file2;
              final List<String> finalColumns = columns;
              executor.submit(() -> sendBatch(tmp, 0, tableName, finalColumns));
              //sendBatch(tmp, 0, tableName, columns);
              file2=new ArrayList<>();
              //file2.clear();
            }
            continue;
          }

          if(serverIndex == 2){
            file3.add(values);
            if(file3.size()>=batchSize){
              List<List<String>> tmp = file3;
              final List<String> finalColumns = columns;
              executor.submit(() -> sendBatch(tmp, 1, tableName, finalColumns));
              //sendBatch(tmp, 1, tableName, columns);
              //file3.clear();
              file3=new ArrayList<>();
            }
          }
        }
      }

      if(!file2.isEmpty()){
        //sendBatch(file2, 0, tableName, columns);
        final List<List<String>> finalFile = file2;
        final List<String> finalColumns = columns;
        executor.submit(() -> sendBatch(finalFile, 0, tableName, finalColumns));
      }
      if(!file3.isEmpty()){
        final List<List<String>> finalFile = file3;
        final List<String> finalColumns = columns;
        executor.submit(() -> sendBatch(finalFile, 1, tableName, finalColumns));
        //sendBatch(file3, 1, tableName, columns);
      }


      System.err.println(a + " " + b);
      reader.close();

    } catch (

    IOException e) {
      e.printStackTrace();
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
//        long rows = pages.getRowCount();

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
          if(i % 100 == 0) {
            long endParse = System.currentTimeMillis();
            System.out.println((endParse - start) + " ms for " + i);
          }

          values = new ArrayList<>();
          for (int j = 0; j < columns.size(); j++) {
            values.add(getValueForField(simpleGroup, columns.get(j), j));
          }


          //System.err.println("Time took for parsing " + (endParse - end1));


          serverIndex = i % 3;
          distributedService.insertRowDistributed(tableName, columns, values, serverIndex);


          //System.err.println("Time took for insert " + (endInsert - endParse));

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
    long st = System.currentTimeMillis();
    Files.copy(fileStream, new File("tempFile.parquet").toPath());

    System.out.println("Copy file took " + (System.currentTimeMillis() - st));
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
