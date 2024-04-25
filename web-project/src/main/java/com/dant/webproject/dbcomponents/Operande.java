package com.dant.webproject.dbcomponents;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class Operande {
    protected String left;
    protected String right;

    protected List<Integer> indices = new ArrayList<>();

    public Operande(String left, String right) {
        this.left = left;
        this.right = right;
    }

    protected int compareValues(Object value1, Object value2, DataType columnType) {
        if (columnType == DataType.INTEGER) {
            Integer intValue1 = Integer.parseInt(value1.toString());
            Integer intValue2 = Integer.parseInt(value2.toString());

            if (intValue1.intValue() == intValue2.intValue()) {
                return 0;
            }

            else if (intValue1.intValue() < intValue2.intValue()) {
                return -1;
            }

            else {
                return 1;
            }
        } else if (columnType == DataType.DOUBLE) {
            Double doubleValue1 = Double.parseDouble(value1.toString());
            Double doubleValue2 = Double.parseDouble(value2.toString());

            if (doubleValue1.doubleValue() == doubleValue2.doubleValue()) {
                return 0;
            }

            else if (doubleValue1.doubleValue() < doubleValue2.doubleValue()) {
                return -1;
            }

            else {
                return 1;
            }
        } else if (columnType == DataType.DATETIME_STRING) {
            return value1.toString().compareTo(value2.toString());
        } else {
            return ((String) value1).compareTo((String) value2);
        }
    }

    public abstract boolean eval(Map<String, Column> tableData);

    public String getLeft() {
        return left;
    }

    public String getRight() {
        return right;
    }

    public List<Integer> getIndice() {
        return indices;
    }
}
