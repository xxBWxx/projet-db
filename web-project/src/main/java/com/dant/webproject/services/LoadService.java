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

  @Autowired
  private final DistributedService distributedService;

  @Autowired
  private final TableModificationService tableModificationService;

  private final ParquetManager parquetManager;

  @Autowired
  public LoadService(SelectService selectService, DatabaseManagementService databaseManagementService,
      DistributedService distributedService,
      TableModificationService tableModificationService) {
    this.selectService = selectService;
    this.databaseManagementService = databaseManagementService;
    this.distributedService = distributedService;
    this.tableModificationService = tableModificationService;
    this.parquetManager = ParquetManager.getParquetManager(this.databaseManagementService, this.distributedService,
        this.tableModificationService);
  }

  public ResponseEntity<String> loadFileToTable(HttpServletRequest request, String tableName) throws IOException {
    String res = parquetManager.uploadFile(request.getInputStream());
    parquetManager.parseParquetFile(res, tableName);
    parquetManager.deleteFile(res);

    return ResponseEntity.ok("File data has been successfully loaded to database.");
  }
}
