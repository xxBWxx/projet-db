package com.dant.webproject.controllers;

import java.util.List;
import java.util.Map;

import com.dant.webproject.services.DatabaseManagementService;
import com.dant.webproject.services.DistributedService;
import com.dant.webproject.services.SelectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dant.webproject.services.DatabaseService;

@RestController
@RequestMapping("/databasemanagementdistributed")
public class DatabaseManagementDistributedController {

    @Autowired
    private DistributedService distributedService;


    @PostMapping("/create")
    public void createTable(@RequestParam String tableName, @RequestBody List<String> columns) {
        distributedService.createTableDistributed(tableName, columns);
    }

}