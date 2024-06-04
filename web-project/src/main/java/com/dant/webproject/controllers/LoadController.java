package com.dant.webproject.controllers;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dant.webproject.services.LoadService;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/load")
public class LoadController {
    @Autowired
    private LoadService loadService;

    @PostMapping("/loadFile")
    public ResponseEntity<String> loadFileToTable(HttpServletRequest request, @RequestParam String tableName)
            throws IOException {
        return loadService.loadFileToTable(request, tableName);
    }
}
