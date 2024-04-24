package com.dant.webproject.controllers;

import com.dant.webproject.dbcomponents.Column;
import com.dant.webproject.services.TableModificationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/tableModification")
public class TableModificationController {

    @Autowired
    private TableModificationService tableModificationService;

    @PostMapping("/insertMult")
    public void insertMult(@RequestParam String table, @RequestBody Map<String, Object> requestData) {

        List<String> col_name = (List<String>) (requestData.get("col_name"));
        List<List<String>> value = (List<List<String>>) (requestData.get("value"));
        tableModificationService.insertMult(table, col_name, value);
    }

    @PostMapping("/insert")
    public void insert(@RequestParam String table, @RequestBody Map<String, List<String>> requestData) {

        List<String> col_name = requestData.get("col_name");
        List<String> value = requestData.get("value");
        tableModificationService.insert(table, col_name, value);
    }

    @PostMapping("/updateCol")
    public void update_col(@RequestParam String tableName, @RequestParam String columnName,
            @RequestParam String newData, @RequestParam String conditionColumn, @RequestParam Object conditionValue) {
        tableModificationService.updateColumn(tableName, columnName, newData, conditionColumn, conditionValue);
    }

}
