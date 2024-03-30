package com.dant.webproject.controllers;

import com.dant.webproject.services.TableModificationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/tablemodification")
public class TableModificationController {

    @Autowired
    private TableModificationService tableModificationService;


    @PostMapping("/insert")
    public void insert(@RequestParam String table, @RequestBody String[] col_name, @RequestBody String[] value) {
        tableModificationService.insert(table, col_name, value);
    }

}
