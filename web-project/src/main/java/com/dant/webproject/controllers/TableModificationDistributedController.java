package com.dant.webproject.controllers;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dant.webproject.dbcomponents.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dant.webproject.services.DistributedService;

@RestController
@RequestMapping("/tableModificationDistributed")
public class TableModificationDistributedController {

    @Autowired
    private DistributedService distributedService;

    private static final Logger LOGGER = LoggerFactory.getLogger(TableModificationDistributedController.class);

    @PostMapping("/insert")
    public ResponseEntity<String> insert(@RequestParam String tableName, @RequestBody Map<String, Object> requestData) {
        List<String> col_name = (List<String>) (requestData.get("col_name"));
        List<List<String>> value = (List<List<String>>) (requestData.get("value"));
        LOGGER.info("Receiving request to insert values");
        distributedService.insertDistributed(tableName, col_name, value);
        return ResponseEntity.ok("Insertion realise !");
    }

    @PostMapping("/updateCol")
    public void update_col(@RequestParam String tableName, @RequestParam String columnName,
            @RequestParam String newData, @RequestParam String conditionColumn, @RequestParam Object conditionValue) {
        distributedService.updateColumnDistributed(tableName, columnName, newData, conditionColumn, conditionValue);
    }

    @PostMapping("/deleteRow")
    public void delete_row(@RequestParam String tableName, @RequestParam String conditionColumn, @RequestParam Object conditionValue) {
        distributedService.deleteRowDistributed(tableName, conditionColumn, conditionValue);
    }

    @PostMapping("/addColumn")
    public void addColumn(@RequestParam String tableName,
                                                 @RequestBody Map<String, Object> requestData) {
        LOGGER.info("Receiving request for the creation of table " + tableName);
        List<String> col_name = (List<String>) (requestData.get("col_name"));
        List<String> type_name = (List<String>) (requestData.get("type"));
        List<DataType> typeList = type_name.stream().map(DataType::valueOf).collect(Collectors.toList());
        distributedService.addColumnColDistributed(tableName, col_name, typeList);
    }

}