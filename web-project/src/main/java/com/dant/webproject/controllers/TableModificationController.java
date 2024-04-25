package com.dant.webproject.controllers;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dant.webproject.services.TableModificationService;

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
    public void insert(@RequestParam String tableName, @RequestBody Map<String, List<String>> requestData) {
        List<String> col_name = requestData.get("col_name");
        List<String> value = requestData.get("value");
        tableModificationService.insert(tableName, col_name, value);
    }

    @PostMapping("/updateCol")
    public void update_col(@RequestParam String tableName, @RequestParam String columnName,
            @RequestParam String newData, @RequestParam String conditionColumn, @RequestParam Object conditionValue) {
        tableModificationService.updateColumn(tableName, columnName, newData, conditionColumn, conditionValue);
    }
}
