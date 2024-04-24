package com.dant.webproject.controllers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dant.webproject.dbcomponents.*;
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


    @GetMapping("/selectallfrom")
    public Map<String, List<Object>> selectAll(@RequestParam String tableName) {
        return selectService.selectAll(tableName);
    }

    @PostMapping("/selectcols")
    public Map<String, List<Object>> selectcols(@RequestParam String tableName,@RequestBody List<String> col_names) {
        return selectService.select_cols(tableName,col_names);
    }

    /*@GetMapping("/select_where_eq_from")
    public Map<String, List<Object>> selectWhere_eq(@RequestParam String tableName, @RequestParam String colName,@RequestParam String val) {
        return selectService.selectWhere_eq(tableName,colName,val);
    }

    @GetMapping("/select_where_sup_from")
    public Map<String, List<Object>> selectWhere_sup(@RequestParam String tableName, @RequestParam String colName,@RequestParam String val) {
        return selectService.selectWhere_sup(tableName,colName,val);
    }

    @GetMapping("/select_where_inf_from")
    public Map<String, List<Object>> selectWhere_inf(@RequestParam String tableName, @RequestParam String colName,@RequestParam String val) {
        return selectService.selectWhere_inf(tableName,colName,val);
    }
*/
    @PostMapping("/select_where")
    public Map<String, List<Object>> selectWhere(@RequestParam String tableName, @RequestBody List<List<String>> op ) {
        List<Operande> listop=new ArrayList<>();

        for(List<String> l : op){
            if(l.get(1).equals("=")){
                System.out.println("l de 2"+l.get(2));
                EqOperande newop=new EqOperande(l.get(0),l.get(2));
                listop.add(newop);
            }
            if(l.get(1).equals(">")){
                SupOperande newop=new SupOperande(l.get(0),l.get(2));
                listop.add(newop);
            }
            if(l.get(1).equals("<")){
                InfOperande newop=new InfOperande(l.get(0),l.get(2));
                listop.add(newop);
            }
        }
        return selectService.select_where(tableName,listop);
    }

}
