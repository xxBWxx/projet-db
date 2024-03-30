package com.dant.webproject.controllers;

import com.dant.webproject.services.TableModificationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/tablemodification")
public class TableModificationController {

    @Autowired
    private TableModificationService tableModificationService;


    @PostMapping("/insert")
    public void insert(@RequestParam String table, @RequestBody Map<String, String[]> requestData) {
        String[] col_name = requestData.get("col_name");
        String[] value = requestData.get("value");
        tableModificationService.insert(table, col_name, value);
    }

}
