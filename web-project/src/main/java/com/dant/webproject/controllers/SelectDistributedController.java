package com.dant.webproject.controllers;

import java.util.List;
import java.util.Map;

import com.dant.webproject.services.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/selectdistributed")
public class SelectDistributedController {

    @Autowired
    private DistributedService distributedService;


    @GetMapping("/selectallfrom")
    public List<Map<String, List<String>>> selectAll(@RequestParam String tableName) {
        return distributedService.selectAllDistributed(tableName);
    }

}