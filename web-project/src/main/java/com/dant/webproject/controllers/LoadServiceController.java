package com.dant.webproject.controllers;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dant.webproject.services.LoadService;
import com.dant.webproject.utils.ParquetReader;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/load")
public class LoadServiceController {
    @Autowired
    private LoadService loadService;

    @PostMapping("/uploadFile")
    public ResponseEntity<String> uploadFile(HttpServletRequest request) throws IOException {
        String res = ParquetReader.uploadFile(request.getInputStream());
        ParquetReader.parseParquetFile(res);
        return ResponseEntity.ok(res);
    }
}
