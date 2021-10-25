package com.gmail.yeritsyankoryun.ApolloDb.service;

import com.gmail.yeritsyankoryun.ApolloDb.model.Types;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static com.gmail.yeritsyankoryun.ApolloDb.service.ParserService.valueParser;
import static com.gmail.yeritsyankoryun.ApolloDb.service.SchemaService.retrieveSchemas;

public class DbService {
    final static String dbPath = "src\\com\\gmail\\yeritsyankoryun\\ApolloDb\\db\\";
    final static List<String> dbList = retrieveDbList();
    static String current;
    static TreeMap<String, LinkedHashMap<String, Types>> schemas;
    static TreeMap<String, LinkedHashMap<UUID, HashMap<String, Object>>> tables;
    static Set<UUID> selectedElements;

    public static void start() {
        try {
            Repl.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void createDb(String dbName) {
        try {
            new File(dbPath + dbName + ".txt").createNewFile();
            System.out.println("Created DB with name:" + dbName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static List<String> retrieveDbList() {
        String[] dbs = new File(dbPath).list((dir, name) -> name.toLowerCase().endsWith(".txt"));
        for (int i = 0; i < dbs.length; i++) {
            dbs[i] = dbs[i].substring(0, dbs[i].length() - 4);
        }
        return Arrays.asList(dbs);
    }

    static void setCurrent(String[] query) {
        if (query.length != 2) {
            System.out.println("Wrong query:");
            return;
        }
        if (!dbList.contains(query[1]))
            createDb(query[1]);
        current = query[1];
        System.out.println("Current Db set to:" + current);
        schemas = retrieveSchemas();
        tables = retrieveDB();
        System.out.println(tables);
    }

    static HashMap<String, Object> createEntity(LinkedHashMap<String, Types> schema, List<Object> values) {
        HashMap<String, Object> entity = new HashMap<>();
        Set<String> keys = schema.keySet();
        int i = 0;
        Types type;
        for (String key : keys) {
            type = schema.get(key);
            if (key.endsWith("$"))
                ((List<Object>) entity.get(key.substring(0, key.length() - 2))).add(valueParser(type, values.get(i++).toString()));
            else
                entity.put(key, valueParser(type, values.get(i++)));
        }
        return entity;
    }

    private static TreeMap<String, LinkedHashMap<UUID, HashMap<String, Object>>> retrieveDB() {
        TreeMap<String, LinkedHashMap<UUID, HashMap<String, Object>>> DBS = new TreeMap<>();
        LinkedHashMap<UUID, HashMap<String, Object>> entities = null;
        String temp;
        String[] temps;
        String[] temps1;
        String[] temps2;
        String[] temps3;
        try {
            temp = Files.readAllLines(Paths.get(dbPath + current + ".txt")).toString();
            temps = temp.split(";};");
            String name = null;
            LinkedHashMap<String, Types> schema = null;
            for (int i = 0; i < temps.length - 1; i++) {
                temps1 = temps[i].split("[{}]");
                int j = 0;
                if (temps1.length >= 3) {
                    if (temps1.length == 4) j++;
                    name = temps1[j].substring(1, temps1[j].length() - 1);
                    schema = schemas.get(name);
                    j++;
                    entities = new LinkedHashMap<>();
                }
                UUID uuid = UUID.fromString(temps1[j].substring(0, temps1[j].length() - 1));
                j++;
                temps2 = temps1[j].split(";");
                HashMap<String, Object> keyValues = new HashMap<>();
                for (String temp4 : temps2) {
                    temps3 = temp4.split("[=:]");
                    String name2 = temps3[0];
                    Types type = schema.get(temps3[0]);
                    if(type==Types.ARRAY){
                        List<Object> tempArr= ((List<Object>)valueParser(type, ""));
                        temps3[1]=temps3[1].replace("[[","");
                        temps3[1]=temps3[1].replace("]]","");
                        temps3[1]=temps3[1].replace(" ","");
                        temps3=temps3[1].split(",");
                        int t=0;
                        for(String x:temps3){
                            type=schema.get(name2+t+"$");
                            tempArr.add(valueParser(type,x));
                            t++;
                        }
                        keyValues.put(name2, tempArr);
                    }
                    else
                        keyValues.put(name2, valueParser(type, temps3[1]));
                }
                entities.put(uuid, keyValues);
                DBS.put(name, entities);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return DBS;
    }

    static void writeDB() {
        StringBuilder dbTxt = new StringBuilder();
        Set<String> tables = DbService.tables.keySet();
        Set<UUID> entities;
        LinkedHashMap<UUID, HashMap<String, Object>> tValues;
        HashMap<String, Object> values;
        Set<String> keys;
        LinkedHashMap<String, Types> schema;
        Types type;
        for (String tName : tables) {
            dbTxt.append(tName).append(":{");
            tValues = DbService.tables.get(tName);
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
                        dbTxt.append("=").append(values.get(key)).append(";");
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
}

