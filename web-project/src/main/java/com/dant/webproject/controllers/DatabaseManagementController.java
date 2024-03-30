package com.dant.webproject.controllers;

import java.util.List;
import java.util.Map;

import com.dant.webproject.services.DatabaseManagementService;
import com.dant.webproject.services.SelectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.dant.webproject.services.DatabaseService;

@RestController
@RequestMapping("/databasemanagement")
public class DatabaseManagementController {

    @Autowired
    private DatabaseManagementService databaseManagementService;


    @PostMapping("/create")
    public void createTable(@RequestParam String tableName, @RequestBody List<String> columns) {
        databaseManagementService.createTable(tableName, columns);
    }

}