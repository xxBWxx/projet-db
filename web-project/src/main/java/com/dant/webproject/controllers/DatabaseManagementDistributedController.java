package com.dant.webproject.controllers;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dant.webproject.dbcomponents.DataType;
import com.dant.webproject.services.DatabaseManagementService;
import com.dant.webproject.services.DistributedService;
import com.dant.webproject.services.SelectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/databaseManagementDistributed")
public class DatabaseManagementDistributedController {

    @Autowired
    private DistributedService distributedService;

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseManagementDistributedController.class);

    @PostMapping("/createTableCol")
    public void createTableCol(@RequestParam String tableName,
            @RequestBody Map<String, Object> requestData) {
        LOGGER.info("Receiving request for the creation of table " + tableName);
        List<String> col_name = (List<String>) (requestData.get("col_name"));
        List<String> type_name = (List<String>) (requestData.get("type"));
        List<DataType> typeList = type_name.stream().map(DataType::valueOf).collect(Collectors.toList());
        distributedService.createTableColDistributed(tableName, col_name, typeList);
    }

    @PostMapping("/createTable")
    public void createTable(@RequestParam String tableName) {
        LOGGER.info("Receiving request for the creation of table " + tableName);
        distributedService.createTableDistributed(tableName);
    }

    @PostMapping("/alterTable")
    public void alterTable(String tableName, String columnName, String typeData) {
        distributedService.alterTableDistributed(tableName, columnName, typeData);
    }

}