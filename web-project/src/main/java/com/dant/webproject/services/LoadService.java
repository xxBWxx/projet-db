package com.dant.webproject.services;

import java.io.IOException;
import java.io.InputStream;

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

  public void loadFileToTable(HttpServletRequest request, String tableName) throws IOException {
    InputStream inputStream = request.getInputStream();
    parquetService.parseParquetFile(inputStream, tableName);
  }
}
