package com.dant.webproject.controllers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dant.webproject.dbcomponents.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dant.webproject.services.SelectService;

@RestController
@RequestMapping("/select")
public class SelectController {

    @Autowired
    private SelectService selectService;

    @GetMapping("/selectallfrom")
    public List<Map<String, Object>> selectAll(@RequestParam String tableName) {
        return selectService.selectAll(tableName);
    }

    @PostMapping("/select_opti")
    public List<Map<String, Object>> select_opti(@RequestParam String tableName, @RequestBody Map<String, Object> requestData) {
        List<String> colNames = (List<String>) (requestData.get("col_name"));
        List<List<String>> conditions = (List<List<String>>) (requestData.get("conditions"));
        return selectService.select_opti(tableName, colNames, conditions);
    }

    @PostMapping("/selectcols")
    public Map<String, List<Object>> selectcols(@RequestParam String tableName, @RequestBody List<String> col_names) {
        return selectService.select_cols(tableName, col_names);
    }


    @PostMapping("/select_where")
    public Map<String, List<Object>> selectWhere(@RequestParam String tableName, @RequestBody List<List<String>> op) {
        List<Operande> listop = new ArrayList<>();

        for (List<String> l : op) {
            if (l.get(1).equals("=")) {
                EqOperande newop = new EqOperande(l.get(0), l.get(2));
                listop.add(newop);
            }
            if (l.get(1).equals(">")) {
                SupOperande newop = new SupOperande(l.get(0), l.get(2));
                listop.add(newop);
            }
            if (l.get(1).equals("<")) {
                InfOperande newop = new InfOperande(l.get(0), l.get(2));
                listop.add(newop);
            }
        }
        return selectService.select_where(tableName, listop);
    }
}



