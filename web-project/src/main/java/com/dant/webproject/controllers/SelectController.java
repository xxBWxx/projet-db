package com.dant.webproject.controllers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dant.webproject.dbcomponents.Column;
import com.dant.webproject.services.DatabaseManagementService;
import com.dant.webproject.services.SelectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.dant.webproject.services.DatabaseService;

@RestController
@RequestMapping("/select")
public class SelectController {

    @Autowired
    private SelectService selectService;

    @GetMapping("/selectAllFrom")
    public Map<String, List<Object>> selectAll(@RequestParam String tableName) {
        return selectService.selectAll(tableName);
    }

    @PostMapping("/selectCols")
    public Map<String, List<Object>> selectcols(@RequestParam String tableName, @RequestBody List<String> col_names) {
        return selectService.select_cols(tableName, col_names);
    }

    @GetMapping("/selectWhereEqFrom")
    public Map<String, List<Object>> selectWhere_eq(@RequestParam String tableName, @RequestParam String colName,
            @RequestParam String val) {
        return selectService.selectWhere_eq(tableName, colName, val);
    }

    @GetMapping("/selectWhereSupFrom")
    public Map<String, List<Object>> selectWhere_sup(@RequestParam String tableName, @RequestParam String colName,
            @RequestParam String val) {
        return selectService.selectWhere_sup(tableName, colName, val);
    }

    @GetMapping("/selectWhereInfFrom")
    public Map<String, List<Object>> selectWhere_inf(@RequestParam String tableName, @RequestParam String colName,
            @RequestParam String val) {
        return selectService.selectWhere_inf(tableName, colName, val);
    }

}
