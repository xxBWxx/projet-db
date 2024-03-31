package com.dant.webproject.controllers;

import java.util.List;

import com.dant.webproject.services.DatabaseManagementService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/databasemanagement")
public class DatabaseManagementController {

    @Autowired
    private DatabaseManagementService databaseManagementService;


    @PostMapping("/createTableCol")
    public void createTableCol(@RequestParam String tableName, @RequestBody List<String> columns) {
        databaseManagementService.createTableCol(tableName, columns);
    }

    @PostMapping("/createTable")
    public void createTable(@RequestParam String tableName) {
        databaseManagementService.createTable(tableName);
    }

    @GetMapping("/showTables")
    public String showTables() {
        List<String> tables = databaseManagementService.showTables();
        StringBuilder response = new StringBuilder("Tables in database:\n");
        for (String table : tables) {
            response.append("- ").append(table).append("\n");
        }
        return response.toString();
    }

}