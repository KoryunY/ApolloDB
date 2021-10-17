package com.gmail.yeritsyankoryun.ApolloDb.service;

import com.gmail.yeritsyankoryun.ApolloDb.model.Types;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;


public class DBMS {
    final static String dbPath = "src\\com\\gmail\\yeritsyankoryun\\ApolloDb\\db\\";
    final static List<String> dbList = retrieveDbList();
    static String current;
    static TreeMap<String, LinkedHashMap<String, Types>> schemas;

    //<users,<id,<k,v>>>
    static TreeMap<String, LinkedHashMap<UUID, HashMap<String, Object>>> DB;

    public static void start() {
        try {
            Repl.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createDb(String dbName) {
        try {
            new File(dbPath + dbName + ".txt").createNewFile();
            System.out.println("Created DB with name:" + dbName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<String> retrieveDbList() {
        String[] dbs = new File(dbPath).list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".txt");
            }
        });
        for (int i = 0; i < dbs.length; i++) {
            dbs[i] = dbs[i].substring(0, dbs[i].length() - 4);
        }
        return Arrays.asList(dbs);
    }

    public static void setCurrent(String[] query) {
        if (query.length != 2)
            System.out.println("Wrong query:");
        else if (dbList.contains(query[1])) {
            current = query[1];
            System.out.println("Current Db set to:" + current);
            schemas = retrieveSchemas();
            DB = retrieveDB();
            writeDB();
        } else {
            createDb(query[1]);
        }
    }

    public static LinkedHashMap<String, Types> createSchema(String[] query) {
        if (query.length == 2)
            return null;
        String record;
        String[] words;
        LinkedHashMap<String, Types> schema = new LinkedHashMap<>();
        for (int i = 2; i < query.length; i++) {
            String name = null;
            Types type = null;
            record = query[i];
            if (record.contains("=")) {
                words = record.split("=");
                name = words[0];
                record = words[1];
                if (record.startsWith("'") && record.endsWith("'")) {
                    type = record.length() == 3 ? Types.CHARACTER : Types.STRING;
                } else {
                    if (record.equals("true") || record.equals("false")) {
                        type = Types.BOOLEAN;
                    } else {
                        type = record.contains(".") ? Types.FLOAT : Types.INT;
                    }
                }
            } else if (record.contains(":")) {
                words = record.split(":");
                name = words[0];
                record = words[1];
                if (record.startsWith("[") && record.endsWith("]")) {
                    type = Types.ARRAY;
                } else {
                    type = Types.T;
                }
            }
            schema.put(name, type);
        }
        return schema;
    }

    public static List<String> getValues(String[] query) {
        String record;
        String[] words;
        List<String> values = new ArrayList<>();
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
                    values.add("YaniiARRAY");
                } else {
                    values.add("YANITTIP");
                }
            }
        }
        return values;
    }

    public static HashMap<String, Object> createEntity(LinkedHashMap<String, Types> schema, List<String> values) {
        System.out.println(schema);
        HashMap<String, Object> entity = new HashMap<>();
        Set<String> keys = schema.keySet();
        int i = 0;
        Types type;
        for (String key : keys) {
            type = schema.get(key);
            switch (type) {
                case CHARACTER -> entity.put(key, values.get(i++).charAt(0));
                case STRING -> entity.put(key, values.get(i++));
                case INT -> entity.put(key, Integer.parseInt(values.get(i++)));
                case FLOAT -> entity.put(key, Float.parseFloat(values.get(i++)));
                case BOOLEAN -> entity.put(key, Boolean.parseBoolean(values.get(i++)));
                case T -> entity.put(key, (i++) + "YanimT");
                case ARRAY -> entity.put(key, (i++) + "YanimARRR");
            }
        }
        return entity;
    }

    private static void writeSchema() {
        try {
            new File(dbPath + current + "Schema.txt").createNewFile();
            Files.write(Paths.get(dbPath + current + "Schema.txt"), schemas.toString().getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static TreeMap<String, LinkedHashMap<String, Types>> retrieveSchemas() {
        String temp = null;
        String[] temps = null;
        String[] temmps = null;
        TreeMap<String, LinkedHashMap<String, Types>> schemasList = new TreeMap<>();
        try {
            temp = Files.readAllLines(Paths.get(dbPath + current + "Schema.txt")).toString();
            temps = temp.split("[}{]");
            for (int i = 1; i < temps.length - 2; i += 4) {

                temp = temps[i].substring(0, temps[i].length() - 1);
                temmps = temps[i + 1].replace(" ", "").split("[=,]");
                LinkedHashMap<String, Types> types = new LinkedHashMap<>();

                for (int j = 0; j < temmps.length; j += 2) {
                    types.put(temmps[j], typeIdentifier(temmps[j + 1]));
                }
                schemasList.put(temp, types);
            }
        } catch (IOException e) {
            System.out.println("Schemas null");
        }
        return schemasList;
    }

    private static TreeMap<String, LinkedHashMap<UUID, HashMap<String, Object>>> retrieveDB() {
        TreeMap<String, LinkedHashMap<UUID, HashMap<String, Object>>> DBS = new TreeMap<>();
        LinkedHashMap<UUID, HashMap<String, Object>> entities = null;
        String temp;
        String[] temps;
        String[] teeemps;
        String[] temeeepeees;
        String[] tempilyo;
        try {
            temp = Files.readAllLines(Paths.get(dbPath + current + ".txt")).toString();
            temps = temp.split(";};");
            String name = null;
            LinkedHashMap<String, Types> schema = null;
            for (int i = 0; i < temps.length - 1; i++) {
                teeemps = temps[i].split("[{}]");
                int j = 0;
                if (teeemps.length >= 3) {
                    if (teeemps.length == 4) j++;
                    name = teeemps[j].substring(1, teeemps[j].length() - 1);
                    schema = schemas.get(name);
                    j++;
                    entities = new LinkedHashMap<>();
                }

                UUID uuid = UUID.fromString(teeemps[j].substring(0, teeemps[j].length() - 1));
                j++;
                temeeepeees = teeemps[j].split(";");
                HashMap<String, Object> keyValues = new HashMap<>();
                for (String tekoi : temeeepeees) {
                    tempilyo = tekoi.split("[=:]");
                    String naemish = tempilyo[0];
                    Types type = schema.get(tempilyo[0]);
                    switch (type) {
                        case CHARACTER -> keyValues.put(naemish, tempilyo[1].charAt(1));
                        case STRING -> keyValues.put(naemish, tempilyo[1].substring(1, tempilyo[1].length() - 1));
                        case INT -> keyValues.put(naemish, Integer.parseInt(tempilyo[1]));
                        case FLOAT -> keyValues.put(naemish, Float.parseFloat(tempilyo[1]));
                        case BOOLEAN -> keyValues.put(naemish, Boolean.parseBoolean(tempilyo[1]));
                        case T -> keyValues.put(naemish, "YanimT");
                        case ARRAY -> keyValues.put(naemish, "YanimARRR");
                    }
                }
                entities.put(uuid, keyValues);
                DBS.put(name, entities);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return DBS;
    }

    private static void writeDB() {
        StringBuilder dbTxt = new StringBuilder();
        Set<String> tables = DB.keySet();
        Set<UUID> entities;
        LinkedHashMap<UUID, HashMap<String, Object>> tValues;
        HashMap<String, Object> values;
        Set<String> keys;
        LinkedHashMap<String, Types> schema;
        Types type;
        for (String tName : tables) {
            dbTxt.append(tName).append(":{");
            tValues = DB.get(tName);
            entities = tValues.keySet();
            schema = schemas.get(tName);
            for (UUID id : entities) {
                dbTxt.append(id).append(":{");
                values = tValues.get(id);
                keys = values.keySet();
                for (String key : keys) {
                    dbTxt.append(key);
                    type = schema.get(key);
                    if (type == Types.STRING || type == Types.CHARACTER) {
                        dbTxt.append("='").append(values.get(key)).append("';");
                    } else if (type == Types.ARRAY) {
                        dbTxt.append(":[").append(values.get(key)).append("];");
                    } else if (type == Types.T) {
                        dbTxt.append(":").append(values.get(key)).append(";");
                    } else {
                        dbTxt.append("=").append(values.get(key)).append(";");
                    }
                }
                dbTxt.append("};");
            }
            dbTxt.append("};");
        }
        System.out.println(dbTxt);
        try {
            Files.writeString(Paths.get(dbPath + current + ".txt"), dbTxt.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void crt(String[] query) {
        LinkedHashMap<String, Types> schema = createSchema(query);
        LinkedHashMap<UUID, HashMap<String, Object>> entity = new LinkedHashMap<>();
        if (schema != null) {
            schemas.put(query[1], schema);
            writeSchema();
            UUID id = UUID.randomUUID();
            entity.put(id, createEntity(schema, getValues(query)));
            DB.put(query[1], entity);
        } else {
            DB.put(query[1], entity);
        }
        writeDB();
    }


    public static void insert(String[] query) {
        if (query.length <= 2) {
            System.out.println("Err");
            return;
        }
        LinkedHashMap<String, Types> schema = schemas.get(query[1]);
        if (schema == null) {
            schema = createSchema(query);
            schemas.put(query[1], schema);
            writeSchema();
        }
        DB.get(query[1]).put(UUID.randomUUID(), createEntity(schema, getValues(query)));
        writeDB();
    }

    //update

    public static void drop() {
        try {
            Files.deleteIfExists(Paths.get(dbPath + current + ".txt"));
            Files.deleteIfExists(Paths.get(dbPath + current + "Schema.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deletion(String[] query) {
        if (query[0].equals("clear")) {
            if (query.length == 1) {
                DB.clear();
                writeDB();
            } else {
                DB.remove(query[1]);
                writeDB();
            }
        } else if (query[0].equals("remove")) {
            //tobedannn
        } else System.out.println("Invalid Delete cmd");
    }

    public static LinkedHashMap<UUID, HashMap<String, Object>> select(String[] query) {
        LinkedHashMap<UUID, HashMap<String, Object>> allEntities = allEntities = DB.get(query[1]);

        if (query.length > 2) {
            LinkedHashMap<UUID, HashMap<String, Object>> entities=new LinkedHashMap<>();
            if(query.length==3){
                UUID id=UUID.fromString(query[2].split("=")[1]);
                entities.put(id,allEntities.get(id));
                return entities;
            }
            LinkedHashMap<String, Types> schema=createSchema(query);
            Set<String> keys=schema.keySet();
            List<String> values=getValues(query);
            for(String key:keys){
                Types type=schema.get(key);
               // allEntities.values()
            }
             return entities;
        }
        return allEntities;
    }

    private static Types typeIdentifier(String type) {
        for (Types types : Types.values()) {
            if (type.equals(types.toString()))
                return types;
        }
        return null;
    }


}

