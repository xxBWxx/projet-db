package com.dant.webproject.dbcomponents;

import java.util.*;

public class Column {
    private String name;
    private Type type;
    private List<?> values;

    public Column(String name, Type type) {
        this.name = name;
        this.type = type;
        switch (type) {
            case INTEGER:
                this.values = new ArrayList<Integer>();
                break;
            case STRING:
                this.values = new ArrayList<String>();
                break;
            default:
                throw new IllegalArgumentException("Type de colonne non supporté");
        }
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public void addValue(String value) {
        switch (type) {
            case INTEGER:
                ((List<Integer>) values).add(Integer.parseInt(value));
                break;
            case STRING:
                ((List<String>) values).add(value);
                break;
            default:
                throw new IllegalArgumentException("Type de colonne non supporté");
        }
    }

    public List<?> getValues() {
        return values;
    }
} // Vous pouvez ajouter d'autres méthodes si nécessaire

