package com.dant.webproject.dbcomponents;

import java.util.*;

public class Column {
    private String name;
    private Type type;
    private List<Object> values;

    public Column(String name, Type type) {
        this.name = name;
        this.type = type;
        switch (type) {
            case INTEGER:
                this.values = new ArrayList<>();
                break;
            case STRING:
                this.values = new ArrayList<>();
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
                (values).add(Integer.parseInt(value));
                break;
            case STRING:
                (values).add(value);
                break;
            default:
                throw new IllegalArgumentException("Type de colonne non supporté");
        }
    }

    public List<Object> getValues() {
        return values;
    }
} // Vous pouvez ajouter d'autres méthodes si nécessaire

