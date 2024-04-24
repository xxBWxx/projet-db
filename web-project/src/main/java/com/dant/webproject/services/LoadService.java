package com.dant.webproject.services;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.dant.webproject.utils.ParquetManager;

import jakarta.servlet.http.HttpServletRequest;

@Component
public class LoadService implements ILoadService {
  @Autowired
  private final SelectService selectService;

  @Autowired
  private final DatabaseManagementService databaseManagementService;

  private final ParquetManager parquetManager;

  @Autowired
  public LoadService(SelectService selectService, DatabaseManagementService databaseManagementService) {
    this.selectService = selectService;
    this.databaseManagementService = databaseManagementService;
    this.parquetManager = ParquetManager.getParquetManager(this.databaseManagementService);
  }

  public ResponseEntity<String> loadFile(HttpServletRequest request) throws IOException {
    String res = parquetManager.uploadFile(request.getInputStream());
    parquetManager.parseParquetFile(res, "table");
    parquetManager.deleteFile(res);

    return ResponseEntity.ok(res + " is successfully loaded to database.");
  }
}
