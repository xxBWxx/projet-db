package com.dant.webproject.dbcomponents;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class Operande {
    protected String left;
    protected String right;

    protected List<Integer> indices=new ArrayList<>();

    public Operande(String left, String right) {
        this.left = left;
        this.right = right;
    }
    protected int compareValues(Object value1, Object value2, Type columnType) {
        if (columnType == Type.INTEGER) {
            return ((Integer) value1).compareTo((Integer) value2);
        } else {
            return ((String) value1).compareTo((String) value2);
        }
    }
    public abstract boolean eval(Map<String, Column> tableData);

    public String getLeft(){
        return left;
    }
    public String getRight(){
        return right;
    }

    public List<Integer> getIndice() {
        return indices;
    }
}
