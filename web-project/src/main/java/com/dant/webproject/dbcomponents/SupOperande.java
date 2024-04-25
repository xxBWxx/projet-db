package com.dant.webproject.dbcomponents;

import java.util.List;
import java.util.Map;

public class SupOperande extends Operande {
    public SupOperande(String left, String right) {
        super(left, right);
    }

    @Override
    public boolean eval(Map<String, Column> tableData) {
        DataType columnType = tableData.get(left).getType();
        Object realValue = right;

        List<Object> columnData = tableData.get(left).getValues();
        for (int i = 0; i < columnData.size(); i++) {
            Object cellValue = columnData.get(i);
            if (cellValue != null && compareValues(cellValue, realValue, columnType) > 0) {
                indices.add(i);
            }
        }
        return !indices.isEmpty();
    }
}
