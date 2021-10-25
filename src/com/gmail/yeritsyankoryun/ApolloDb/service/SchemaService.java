package com.gmail.yeritsyankoryun.ApolloDb.service;

import com.gmail.yeritsyankoryun.ApolloDb.model.Types;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.TreeMap;

import static com.gmail.yeritsyankoryun.ApolloDb.service.DbService.dbPath;
import static com.gmail.yeritsyankoryun.ApolloDb.service.DbService.schemas;
import static com.gmail.yeritsyankoryun.ApolloDb.service.ParserService.primitiveTypeParser;
import static com.gmail.yeritsyankoryun.ApolloDb.service.ParserService.typeIdentifier;

public class SchemaService {
     static LinkedHashMap<String, Types> createSchema(String[] query) {
        if (query.length == 2)
            return null;
        String record;
        String[] words;
        LinkedHashMap<String, Types> schema = new LinkedHashMap<>();
        for (int i = 2; i < query.length; i++) {
            String name ;
            Types type ;
            record = query[i];
            if (record.contains("=")) {
                words = record.split("=");
                name = words[0];
                record = words[1];
                type = primitiveTypeParser(record);
                schema.put(name, type);
            } else if (record.contains(":")) {
                words = record.split(":");
                name = words[0];
                record = words[1];
                if (record.startsWith("[") && record.endsWith("]")) {
                    type = Types.ARRAY;
                    schema.put(name, type);
                    record = record.replace("[", "");
                    record = record.replace("]", "");
                    words = record.split(",");
                    int j = 0;
                    for (String k : words) {
                        type = primitiveTypeParser(k);
                        schema.put(name + j + "$", type);
                        j++;
                    }
                } else {
                    type = Types.T;
                    schema.put(name, type);
                }
            }
        }
        return schema;
    }

     static void writeSchema() {
        try {
            new File(dbPath+"schemas\\" + DbService.current + "Schema.txt").createNewFile();
            Files.write(Paths.get(dbPath+"schemas\\" + DbService.current + "Schema.txt"), schemas.toString().getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

     static TreeMap<String, LinkedHashMap<String, Types>> retrieveSchemas() {
        String temp;
        String[] temps;
        String[] temps2;
        TreeMap<String, LinkedHashMap<String, Types>> schemasList = new TreeMap<>();
        try {
            temp = Files.readAllLines(Paths.get(dbPath+"schemas\\" + DbService.current + "Schema.txt")).get(0);
            temps = temp.split("[{}]");
            for (int i = 1; i < temps.length ; i += 2) {
                temp = temps[i].substring(0, temps[i].length() - 1);
                temp=temp.replace(",","");
                temp=temp.replace(" ","");
                temps2 = temps[i + 1].replace(" ", "").split("[=,]");
                LinkedHashMap<String, Types> types = new LinkedHashMap<>();
                for (int j = 0; j < temps2.length; j += 2) {
                    types.put(temps2[j], typeIdentifier(temps2[j + 1]));
                }
                schemasList.put(temp, types);
            }
        } catch (IOException e) {
            System.out.println("Schemas null");
        }
        return schemasList;
    }
}
