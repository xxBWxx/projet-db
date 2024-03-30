package com.dant.webproject.controllers;

import java.util.List;
import java.util.Map;

import com.dant.webproject.services.DatabaseManagementService;
import com.dant.webproject.services.SelectService;
import com.dant.webproject.services.TableModificationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dant.webproject.services.DatabaseService;

@RestController
@RequestMapping("/tablemodification")
public class TableModificationController {

    @Autowired
    private TableModificationService tableModificationService;


    @PostMapping("/insert")
    public void insert(@RequestParam String table, @RequestParam String[] col_name, @RequestParam String[] value) {
        tableModificationService.insert(table, col_name, value);
    }

}
