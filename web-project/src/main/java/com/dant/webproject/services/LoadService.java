package com.dant.webproject.services;

import com.dant.webproject.utils.ParquetReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class LoadService implements ILoadService {

  @Autowired
  private final SelectService selectService;


  @Autowired
  public LoadService(SelectService selectService){
    this.selectService=selectService;
  }


}
