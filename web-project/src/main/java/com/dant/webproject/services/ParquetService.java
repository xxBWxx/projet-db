package com.dant.webproject.services;

import java.io.*;
import java.lang.management.MemoryManagerMXBean;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

import com.dant.webproject.dbcomponents.Column;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.*;
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

  private static class SeekableByteArrayInputStream extends SeekableInputStream {
    private final byte[] data;
    private int position;

    public SeekableByteArrayInputStream(byte[] data) {
      this.data = data;
      this.position = 0;
    }

    @Override
    public long getPos() throws IOException {
      return position;
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos < 0 || newPos > data.length) {
        throw new IOException("Invalid seek position");
      }
      this.position = (int) newPos;
    }

    @Override
    public int read() throws IOException {
      if (position >= data.length) {
        return -1;
      }
      return data[position++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (position >= data.length) {
        return -1;
      }
      int toRead = Math.min(len, data.length - position);
      System.arraycopy(data, position, b, off, toRead);
      position += toRead;
      return toRead;
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
      int remaining = Math.min(byteBuffer.remaining(), data.length - position);
      byteBuffer.put(data, position, remaining);
      position += remaining;
      return remaining;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
      if (len < 0 || start < 0 || start + len > data.length) {
        throw new IOException("Invalid read length");
      }
      System.arraycopy(data, position, bytes, start, len);
      position += len;
    }

    @Override
    public void readFully(ByteBuffer byteBuffer) throws IOException {
      int len = byteBuffer.remaining();
      if (position + len > data.length) {
        throw new IOException("Invalid read length");
      }
      byteBuffer.put(data, position, len);
      position += len;
    }

    @Override
    public void close() throws IOException {
      // Nothing to close
    }
  }


  public void parseParquetFile(InputStream inputStream, String tableName) {
    ExecutorService executor = Executors.newFixedThreadPool(5);
    List<Future<?>> futures = new ArrayList<>();

    try (BoundedInputStream boundedInputStream = new BoundedInputStream(inputStream)) {
      // Buffer the input stream to get its length
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      boundedInputStream.transferTo(buffer);
      byte[] inputBytes = buffer.toByteArray();

      SeekableInputStream seekableInputStream = new SeekableByteArrayInputStream(inputBytes);

      InputFile inputFile = new InputFile() {
        @Override
        public long getLength() {
          return inputBytes.length;
        }

        @Override
        public SeekableInputStream newStream() {
          return seekableInputStream;
        }
      };

      long start = System.currentTimeMillis();
      ParquetFileReader reader = ParquetFileReader.open(inputFile);
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();

      List<DataType> types = null;
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

      while ((pages = reader.readNextRowGroup()) != null) {
        a++;

        RecordReader<Group> recordReader = columnIO.getRecordReader(
                pages, new GroupRecordConverter(schema));

        for (int i = 0; i < 1000000; i++) {
          SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
          b++;

          if (i == 0) {
            types = getTypesOfGroup(simpleGroup);
            columns = getFieldNames(simpleGroup);
            distributedService.addColumnColDistributed(tableName, columns, types);
          }

          if (i % 100 == 0) {
            long endParse = System.currentTimeMillis();
            System.out.println((endParse - start) + " ms for " + i);
          }

          values = new ArrayList<>();
          for (int j = 0; j < columns.size(); j++) {
            values.add(getValueForField(simpleGroup, columns.get(j), j));
          }

          int serverIndex = i % 3;

          if (serverIndex == 0) {
            tableModificationService.insert(tableName, columns, values);
            values.clear();
            continue;
          }

          files[serverIndex - 1].add(values);
          if (files[serverIndex - 1].size() >= batchSize) {
            List<List<String>> tmp = files[serverIndex - 1];
            final List<String> finalColumns = columns;
            final int s = serverIndex - 1;
            Future<?> future = executor.submit(() -> {
              sendBatch(tmp, s, tableName, finalColumns);
            });
            futures.add(future);
            files[serverIndex - 1] = new ArrayList<>();
          }
        }
      }

      final List<String> finalColumns = columns;
      if (!files[0].isEmpty()) {
        final List<List<String>> finalFile = files[0];
        Future<?> future = executor.submit(() -> {
          sendBatch(finalFile, 0, tableName, finalColumns);
        });
        futures.add(future);
      }

      if (!files[1].isEmpty()) {
        final List<List<String>> finalFile = files[1];
        Future<?> future = executor.submit(() -> {
          sendBatch(finalFile, 1, tableName, finalColumns);
        });
        futures.add(future);
      }

      System.err.println(a + " " + b);
      reader.close();

      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }

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




  public void parseParquetFile1(InputStream inputStream, String tableName) {
    //ExecutorService executor = Executors.newFixedThreadPool(10); // Créer un pool de 2 threads
    ExecutorService executor = new ThreadPoolExecutor(
            15, 15,
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

      int batchSize = 45000;
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
        for (int i = 0; i < 4000000; i++) {
          simpleGroup = (SimpleGroup) recordReader.read();
          b++;

          if (i == 0) {
            types = getTypesOfGroup(simpleGroup);
            columns = getFieldNames(simpleGroup);

            distributedService.addColumnColDistributed(tableName, columns, types);
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
