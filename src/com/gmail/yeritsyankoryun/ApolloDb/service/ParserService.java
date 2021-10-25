package com.gmail.yeritsyankoryun.ApolloDb.service;

import com.gmail.yeritsyankoryun.ApolloDb.model.Types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class ParserService {
     static Types typeIdentifier(String type) {
        for (Types types : Types.values()) {
            if (type.equals(types.toString()))
                return types;
        }
        return null;
    }

     static Types primitiveTypeParser(String record) {
        if (record.startsWith("'") && record.endsWith("'")) {
            return record.length() == 3 ? Types.CHARACTER : Types.STRING;
        } else {
            if (record.equals("true") || record.equals("false")) {
                return Types.BOOLEAN;
            } else {
                return record.contains(".") ? Types.FLOAT : Types.INT;
            }
        }
    }

     static Object valueParser(Types type, Object value) {
        Object obj = null;
        switch (type) {
            case CHARACTER, STRING -> obj = value;
            case INT -> obj = Integer.parseInt(value.toString());
            case FLOAT -> obj = Float.parseFloat(value.toString());
            case BOOLEAN -> obj = Boolean.parseBoolean(value.toString());
            case T -> obj = UUID.fromString(value.toString());
            case ARRAY -> {obj = new ArrayList<>();}
        }
        return obj;
    }
     static List<Object> getValues(String[] query) {
        String record;
        String[] words;
        List<Object> values = new ArrayList<>();
        for (int i = 2; i < query.length; i++) {
            record = query[i];
            if (record.contains("=")) {
                words = record.split("=");
                record = words[1];
                if (record.startsWith("'") && record.endsWith("'")) {
                    values.add(record.substring(1, record.length() - 1));
                } else {
                    values.add(record);
                }
            } else if (record.contains(":")) {
                words = record.split(":");
                record = words[1];
                if (record.startsWith("[") && record.endsWith("]")) {
                    values.add("");
                    record = record.replace("[", "");
                    record = record.replace("]", "");
                    words = record.split(",");
                    values.addAll(Arrays.asList(words));
                } else {
                    values.add(record);
                }
            }
        }
        return values;
    }
}
