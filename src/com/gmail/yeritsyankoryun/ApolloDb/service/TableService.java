package com.gmail.yeritsyankoryun.ApolloDb.service;

import com.gmail.yeritsyankoryun.ApolloDb.model.Types;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import static com.gmail.yeritsyankoryun.ApolloDb.service.DbService.*;
import static com.gmail.yeritsyankoryun.ApolloDb.service.ParserService.getValues;
import static com.gmail.yeritsyankoryun.ApolloDb.service.ParserService.valueParser;
import static com.gmail.yeritsyankoryun.ApolloDb.service.SchemaService.createSchema;
import static com.gmail.yeritsyankoryun.ApolloDb.service.SchemaService.writeSchema;

public class TableService {
     static void create(String[] query) {
        LinkedHashMap<String, Types> schema = createSchema(query);
        LinkedHashMap<UUID, HashMap<String, Object>> entity = new LinkedHashMap<>();
        if (schema != null) {
            schemas.put(query[1], schema);
            writeSchema();
            UUID id = UUID.randomUUID();
            entity.put(id, createEntity(schema, getValues(query)));
        }
        tables.put(query[1], entity);
        writeDB();
    }


     static void insert(String[] query) {
        if (query.length <= 2) {
            System.out.println("Insertion Error");
            return;
        }
        LinkedHashMap<String, Types> schema = schemas.get(query[1]);
        if (schema == null) {
            schema = createSchema(query);
            schemas.put(query[1], schema);
            writeSchema();
        }

        tables.get(query[1]).put(UUID.randomUUID(), createEntity(schema, getValues(query)));
        writeDB();
    }

     static void update(String[] query) {
        List<Object> newValues = getValues(query);

        LinkedHashMap<String, Types> schema = createSchema(query);
        for (UUID key : selectedElements) {
            int i = 0;
            HashMap<String, Object> curr = tables.get(query[1]).get(key);
            for (String sKey : schema.keySet()) {
                curr.replace(sKey, valueParser(schema.get(sKey), newValues.get(i)));
            }
        }
        writeDB();
    }

     static void drop() {
        try {
            Files.deleteIfExists(Paths.get(dbPath + current + ".txt"));
            Files.deleteIfExists(Paths.get(dbPath+"schemas\\" + current + "Schema.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

     static void deletion(String[] query) {
        if (query[0].equals("clear")) {
            if (query.length == 1) {
                tables.clear();
                schemas.clear();
            } else {
                tables.remove(query[1]);
                schemas.remove(query[1]);
            }
            writeDB();
            writeSchema();
        } else if (query[0].equals("remove")) {
            for (UUID key : select(query).keySet()) {
                tables.get(query[1]).remove(key);
            }
            writeDB();
            selectedElements = null;
        } else System.out.println("Invalid Delete cmd");
    }

     static LinkedHashMap<UUID, HashMap<String, Object>> select(String[] query) {
        LinkedHashMap<UUID, HashMap<String, Object>> allEntities = tables.get(query[1]);
        if (query.length > 2) {
            LinkedHashMap<UUID, HashMap<String, Object>> entities = new LinkedHashMap<>();
            if (query.length == 3 && query[2].split("=")[0].equals("id")) {
                UUID id  = UUID.fromString(query[2].split("=")[1]);
                entities.put(id, allEntities.get(id));
                System.out.println(entities);
                return entities;
            }
            LinkedHashMap<String, Types> schema = createSchema(query);
            List<Object> values = getValues(query);
            HashMap<String, Object> selQuery = createEntity(schema, values);
            boolean contains;
            HashMap<String, Object> entity;
            for (UUID key : allEntities.keySet()) {
                entity = allEntities.get(key);
                contains = true;
                for (String qKey : selQuery.keySet()) {
                    if (entity.get(qKey).equals(selQuery.get(qKey))) continue;
                    contains = false;
                    break;
                }
                if (contains) {
                    entities.put(key, entity);
                }
            }
            selectedElements = allEntities.keySet();
            System.out.println(entities);
            return entities;
        }
        selectedElements = allEntities.keySet();
        System.out.println(allEntities);
        return allEntities;
    }

}
