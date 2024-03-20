package com.dant.webproject.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class ParquetReader {

  private static List<String> getFieldValuePairs(Group g) {
    List<String> res = new ArrayList<>();

    int fieldCount = g.getType().getFieldCount();

    for (int field = 0; field < fieldCount; field++) {
      int valueCount = g.getFieldRepetitionCount(field);

      Type fieldType = g.getType().getType(field);
      String fieldName = fieldType.getName();

      for (int i = 0; i < valueCount; i++) {
        // TODO: consider printing only primitives, using fieldType.isPrimitive()
        // System.out.println(fieldName + " " + g.getValueToString(field, i));
        res.add(fieldName + " " + g.getValueToString(field, i));
      }
    }

    return res;
  }

  private static List<String> getValues(Group g) {
    List<String> res = new ArrayList<>();

    int fieldCount = g.getType().getFieldCount();

    for (int field = 0; field < fieldCount; field++) {
      int valueCount = g.getFieldRepetitionCount(field);

      if (valueCount == 0) {
        res.add("-");
      } else {
        res.add(g.getValueToString(field, 0));
      }
    }

    return res;
  }

  private static List<String> getFieldNames(Group g) {
    int fieldCount = g.getType().getFieldCount();

    List<String> res = new ArrayList<>();

    for (int field = 0; field < fieldCount; field++) {
      Type fieldType = g.getType().getType(field);
      String fieldName = fieldType.getName();
      res.add(fieldName);
    }

    return res;
  }

  public static void readParquetFile(String filePath) {
    final Path path = new Path(filePath);
    Configuration config = new Configuration();

    try {
      ParquetMetadata readFooter = ParquetFileReader.readFooter(
        config,
        path,
        ParquetMetadataConverter.NO_FILTER
      );
      MessageType schema = readFooter.getFileMetaData().getSchema();
      ParquetFileReader r = new ParquetFileReader(config, path, readFooter);

      PageReadStore pages = null;

      try {
        while ((pages = r.readNextRowGroup()) != null) {
          final long rows = pages.getRowCount();

          //TODO: remove
          System.out.println("Number of rows: " + rows);

          final MessageColumnIO columnIO = new ColumnIOFactory()
            .getColumnIO(schema);
          final RecordReader<Group> recordReader = columnIO.getRecordReader(
            pages,
            new GroupRecordConverter(schema)
          );

          // TODO: change with i < rows
          for (int i = 0; i < 1; i++) {
            final Group g = (Group) recordReader.read();
            System.out.println(getFieldValuePairs(g));
          }
        }
      } finally {
        r.close();
      }
    } catch (IOException e) {
      System.out.println("Error reading parquet file:\n" + e.getMessage());
      e.printStackTrace();
    }
  }
}
