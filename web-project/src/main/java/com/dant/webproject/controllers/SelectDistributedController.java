package com.dant.webproject.controllers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.dant.webproject.dbcomponents.EqOperande;
import com.dant.webproject.dbcomponents.InfOperande;
import com.dant.webproject.dbcomponents.Operande;
import com.dant.webproject.dbcomponents.SupOperande;
import com.dant.webproject.services.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/selectDistributed")
public class SelectDistributedController {

    @Autowired
    private DistributedService distributedService;

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectDistributedController.class);

    @GetMapping("/selectAllFrom")
    public Map<String, List<Object>> selectAll(@RequestParam String tableName) {
        LOGGER.info("Receiving select * from " + tableName + " request");
        return distributedService.selectAllDistributed(tableName);
    }

    @PostMapping("/selectCols")
    public Map<String, List<Object>> selectcols(@RequestParam String tableName, @RequestBody List<String> col_names) {
        LOGGER.info("Receiving select " + col_names.toString() + " from " + tableName + " request");
        return distributedService.select_colsDistributed(tableName, col_names);
    }

   /* @GetMapping("/selectWhereEqFrom")
    public Map<String, List<Object>> selectWhere_eq(@RequestParam String tableName, @RequestParam String colName,
            @RequestParam String val) {
        LOGGER.info("Receiving select * from " + tableName + " WHERE " + colName + "=" + val);
        return distributedService.selectWhere_eqDistributed(tableName, colName, val);
    }

    @GetMapping("/selectWhereSupFrom")
    public Map<String, List<Object>> selectWhere_sup(@RequestParam String tableName, @RequestParam String colName,
            @RequestParam String val) {
        LOGGER.info("Receiving select * from " + tableName + " WHERE " + colName + ">" + val);
        return distributedService.selectWhere_supDistributed(tableName, colName, val);
    }

    @GetMapping("/selectWhereInfFrom")
    public Map<String, List<Object>> selectWhere_inf(@RequestParam String tableName, @RequestParam String colName,
            @RequestParam String val) {
        LOGGER.info("Receiving select * from " + tableName + " WHERE " + colName + "<" + val);
        return distributedService.selectWhere_infDistributed(tableName, colName, val);
    }
    */

   @PostMapping("/select_where")
   public Map<String, List<Object>> selectWhere(@RequestParam String tableName, @RequestBody List<List<String>> op) {

       return distributedService.selectWhereDistributed(tableName, op);
   }

}