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
        } else {
            createDb(query[1]);
        }
        schemas = retrieveSchemas();
        DB = retrieveDB();
        writeDB();
    }


    public static void createSchema(String[] query) {
        if (query.length == 2)
            createRecord(null, null, query[1]);

        else {
            String record;
            String[] words;
            List<String> values = new ArrayList<>();
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
                        values.add(record.substring(1, record.length() - 1));
                    } else {
                        if (record.equals("true") || record.equals("false")) {
                            type = Types.BOOLEAN;
                        } else {
                            type = record.contains(".") ? Types.FLOAT : Types.INT;
                        }
                        values.add(record.substring(0, record.length()));
                    }
                } else if (record.contains(":")) {
                    words = record.split(":");
                    name = words[0];
                    record = words[1];
                    if (record.startsWith("[") && record.endsWith("]")) {
                        type = Types.ARRAY;
                        values.add("YaniiARRAY");
                    } else {
                        type = Types.T;
                        values.add("YANITTIP");
                    }
                }
                schema.put(name, type);
            }
            schemas.put(query[1], schema);
            writeSchema();
            createRecord(schema, values, query[1]);
        }
    }


    public static void createRecord(LinkedHashMap<String, Types> schema, List<String> values, String name) {
        StringBuilder text = new StringBuilder(name + ":{");
        if (schema != null) {
            text.append(UUID.randomUUID().toString()).append(":{");
            Set<String> names = schema.keySet();
            Types type;
            int i = 0;
            for (String val : names) {
                type = schema.get(val);
                text.append(val);
                if (type == Types.STRING || type == Types.CHARACTER) {
                    text.append("='").append(values.get(i++)).append("';");
                } else if (type == Types.ARRAY) {
                    text.append(":[").append(values.get(i++)).append("];");
                } else if (type == Types.T) {
                    text.append(":").append(values.get(i++)).append(";");
                } else {
                    text.append("=").append(values.get(i++)).append(";");
                }
            }
            text.append("};");
        }
        text.append("};");
        try {
            Files.write(Paths.get(dbPath + current + ".txt"), text.toString().getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
//        try {
//            Files.writeString(Paths.get(dbPath + current + ".txt"), dbTxt.toString());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public  static void crt(String[] query){

    }


    public static void insert(String[] query) {
        if (query.length <= 2) {
            System.out.println("Err");
            return;
        }
        LinkedHashMap<String, Types> schema = schemas.get(query[1]);
        for (int i = 2; i < query.length; i++) {
            System.out.println(query[i] + " ");
        }

    }

    //update

    //delete

    //select

    private static Types typeIdentifier(String type) {
        for (Types types : Types.values()) {
            if (type.equals(types.toString()))
                return types;
        }
        return null;
    }


}

