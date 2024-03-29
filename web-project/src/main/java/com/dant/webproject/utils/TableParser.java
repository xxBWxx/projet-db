package com.dant.webproject.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.*;
import org.apache.log4j.chainsaw.Main;

public class TableParser {

  public static Map<String, List<Float>> parseColumnsToFloat(
    Map<String, List<String>> table
  ) {
    final Logger logger = Logger.getLogger(Main.class.getName());

    Map<String, List<Float>> res = new HashMap<>();
    Float parsedValue;
    List<Float> parsedValues;
    String key;
    List<String> values;

    for (Map.Entry<String, List<String>> entry : table.entrySet()) {
      key = entry.getKey();
      values = entry.getValue();

      parsedValues = new ArrayList<>();
      parsedValue = 0f;

      for (String value : values) {
        try {
          parsedValue = Float.parseFloat(value);
        } catch (NumberFormatException e) {
          logger.log(
            Level.WARNING,
            "Cannot parse " + value + ". Replacing it with a 0."
          );
        } finally {
          parsedValues.add(parsedValue);
        }
      }

      res.put(key, parsedValues);
    }

    return res;
  }
}
