package com.dant.webproject.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;

public class DatabaseManagementService implements IDatabaseManagementService {

  @Autowired
  private static Map<String, Map<String, List<String>>> database = new HashMap<>();

  @Override
  public void createTable(String tableName) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException(
      "Unimplemented method 'createTable'"
    );
  }

  @Override
  public void getTable(String tableName) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'getTable'");
  }
}
