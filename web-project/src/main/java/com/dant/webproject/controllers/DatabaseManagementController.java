package com.dant.webproject.controllers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dant.webproject.dbcomponents.DataType;
import com.dant.webproject.services.DatabaseManagementService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/databaseManagement")
public class DatabaseManagementController {
    @Autowired
    private DatabaseManagementService databaseManagementService;

    @PostMapping("/createTableCol")
    public void createTableCol(@RequestParam String tableName, @RequestBody Map<String, Object> requestData) {
        List<String> col_name = (List<String>) (requestData.get("col_name"));
        List<String> type_name = (List<String>) (requestData.get("type"));
        List<DataType> typeList = type_name.stream().map(DataType::valueOf).collect(Collectors.toList());
        databaseManagementService.createTableCol(tableName, col_name, typeList);
    }

    @PostMapping("/createTable")
    public void createTable(@RequestParam String tableName) {
        databaseManagementService.createTable(tableName);
    }

    @GetMapping("/showTables")
    public Map<String, List<String>> showTables() {
        Map<String, List<String>> res = new HashMap<>();
        List<String> tables = databaseManagementService.showTables();

        res.put("tables", tables);

        return res;
    }

    @GetMapping("/describeTable")
    public List<Map<String, Object>> describeTable(String tableName) {
        return databaseManagementService.describeTable(tableName);
    }


    @PostMapping("/alterTable")
    public void alterTable(String tableName, String columnName, String typeData) {
        DataType type = DataType.valueOf(typeData);
        databaseManagementService.alterTable(tableName, columnName, type);
    }


}