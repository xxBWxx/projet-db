package com.dant.webproject.utils;

import com.dant.webproject.services.DatabaseService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.print.DocFlavor.STRING;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class ParquetReader {

  private static Map<String, String> getFieldValueMap(Group g) {
    Map<String, String> res = new HashMap<>();

    int fieldCount = g.getType().getFieldCount();

    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
      int valueCount = g.getFieldRepetitionCount(fieldIndex);

      Type fieldType = g.getType().getType(fieldIndex);
      String fieldName = fieldType.getName();

      for (int i = 0; i < valueCount; i++) {
        res.put(fieldName, g.getValueToString(fieldIndex, i));
      }
    }

    return res;
  }

  // TODO riu
  private static List<String> getValues(SimpleGroup group) {
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
  private static String getValueForField(SimpleGroup group, String fieldName) {
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
  private static List<String> getFieldNames(SimpleGroup group) {
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
  public static void readParquetFile(String filePath) {
    List<SimpleGroup> simpleGroups = new ArrayList<>();

    try {
      ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(new Path(filePath), new Configuration())
      );
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      List<Type> fields = schema.getFields();
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        long rows = pages.getRowCount();
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        RecordReader recordReader = columnIO.getRecordReader(
          pages,
          new GroupRecordConverter(schema)
        );
        for (int i = 0; i < 2; i++) {
          SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
          simpleGroup.getFieldRepetitionCount(0);
          simpleGroups.add(simpleGroup);
          System.out.println(simpleGroup);
        }
      }
      reader.close();
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
  }

  public static void parseParquetFile(String filePath) {
    try {
      ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(new Path(filePath), new Configuration())
      );
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      List<Type> fields = schema.getFields();

      // TODO: change variable name for more comprehension
      Map<String, List<String>> fieldMap = new HashMap<String, List<String>>();

      for (Type field : fields) {
        fieldMap.put(field.toString().split("\\s")[2], new ArrayList<>());
      }

      String tableName = createTableNameFromFilePath(filePath);
      DatabaseService.getDatabase().put(tableName, fieldMap);

      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        long rows = pages.getRowCount();

        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        RecordReader recordReader = columnIO.getRecordReader(
          pages,
          new GroupRecordConverter(schema)
        );

        // TODO: replace 1 with rows
        for (int i = 0; i < 1; i++) {
          SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();

          for (String fieldName : fieldMap.keySet()) {
            String value = getFieldValueMap(simpleGroup).get(fieldName);
            String valueToAdd = value == null ? "-" : value;

            fieldMap.get(fieldName).add(valueToAdd);
          }
        }
        // System.out.println(DatabaseService.getDatabase());
      }
      reader.close();
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
  }

  private static String createTableNameFromFilePath(String filePath) {
    String[] splittedPath = filePath.split("\\\\");
    String relativePath = splittedPath[splittedPath.length - 1];

    return relativePath.split("\\.")[0];
  }
}
