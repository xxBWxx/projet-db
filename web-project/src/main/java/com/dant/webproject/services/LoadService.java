package com.dant.webproject.services;

import com.dant.webproject.utils.ParquetReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;

public class LoadService implements ILoadService {

  @Autowired
  private static Map<String, Map<String, List<String>>> database;

  @Override
  public void loadDatabase(String filePath) {
    ParquetReader.parseParquetFile(filePath);
  }
}
