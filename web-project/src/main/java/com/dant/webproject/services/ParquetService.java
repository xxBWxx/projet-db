package com.dant.webproject.services;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.dant.webproject.dbcomponents.DataType;

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

  private String getValueForField(SimpleGroup group, String fieldName, int position) {
    String res = "-";

    try {
      return group.getValueToString(position, 0);
    } catch (Exception e) {
      return res;
    }
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

  public ResponseEntity<String> parseParquetFile(InputStream inputStream, String tableName) {
    ExecutorService executor = Executors.newFixedThreadPool(10);
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

      ParquetFileReader reader = ParquetFileReader.open(inputFile);
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();

      List<DataType> types = null;
      List<String> columns = null;

      List<String> values;

      @SuppressWarnings("unchecked")
      List<List<String>>[] files = new List[2];
      files[0] = new ArrayList<>();
      files[1] = new ArrayList<>();

      PageReadStore pages;
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

      int batchSize = 45000;
      int serverIndex = 0;
      RecordReader<Group> recordReader;
      while ((pages = reader.readNextRowGroup()) != null) {
        // long rows = pages.getRowCount();

        recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

        SimpleGroup simpleGroup;
        for (int i = 0; i < 3000000; i++) {
          simpleGroup = (SimpleGroup) recordReader.read();

          if (i == 0) {
            types = getTypesOfGroup(simpleGroup);
            columns = getFieldNames(simpleGroup);
            distributedService.addColumnColDistributed(tableName, columns, types);
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

      return ResponseEntity.status(HttpStatus.OK)
          .body("File successfully added to database.");
    } catch (IOException e) {
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body("File cannot be added to database.");
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
    // String[] serverUrls = { "http://localhost:8081", "http://localhost:8082" };
    String[] serverUrls = { "http://132.227.114.34:8081", "http://132.227.114.35:8082" };

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
