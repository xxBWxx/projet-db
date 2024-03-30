package com.dant.webproject.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DatabaseManagementService implements IDatabaseManagementService {


  private Map<String, Map<String, List<String>>> database = new HashMap<>();

  public void createTable(String tableName, List<String> columns) {
    if (database.get(tableName) != null) {
      throw new IllegalArgumentException(
              "La table " + tableName + " existe deja dans la base de donnees"
      );
    }

    Map<String, List<String>> table = new HashMap<>();

    for (String column : columns) {
      table.put(column, new ArrayList<>());
    }

    database.put(tableName, table);
  }

  public Map<String, Map<String, List<String>>> getDatabase(){
    return database;
  }


}
