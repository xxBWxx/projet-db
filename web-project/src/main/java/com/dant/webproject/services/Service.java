package com.dant.webproject.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Service {

  private final Map<String, Map<String, List<String>>> database = new HashMap<>();

  public Map<String, Map<String, List<String>>> getDatabase() {
    return database;
  }
}
