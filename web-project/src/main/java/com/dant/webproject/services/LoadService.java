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

  private final ParquetManager parquetManager = ParquetManager.getParquetManager();

  @Autowired
  public LoadService(SelectService selectService) {
    this.selectService = selectService;
  }

  public ResponseEntity<String> uploadFile(HttpServletRequest request) throws IOException {
    String res = parquetManager.uploadFile(request.getInputStream());
    parquetManager.parseParquetFile(res);

    return ResponseEntity.ok(res);
  }
}
