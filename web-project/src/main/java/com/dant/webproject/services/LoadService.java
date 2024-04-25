package com.dant.webproject.services;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import jakarta.servlet.http.HttpServletRequest;

@Component
public class LoadService implements ILoadService {
  @Autowired
  private final ParquetService parquetService;

  @Autowired
  public LoadService(ParquetService parquetService) {
    this.parquetService = parquetService;
  }

  public ResponseEntity<String> loadFileToTable(HttpServletRequest request, String tableName) throws IOException {
    String res = parquetService.uploadFile(request.getInputStream());
    parquetService.parseParquetFile(res, tableName);
    parquetService.deleteFile(res);

    return ResponseEntity.ok("File data has been successfully loaded to database.");
  }
}
