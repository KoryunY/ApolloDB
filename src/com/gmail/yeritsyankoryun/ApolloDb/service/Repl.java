package com.gmail.yeritsyankoryun.ApolloDb.service;

import com.gmail.yeritsyankoryun.ApolloDb.model.Types;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Repl {
    public static void start() throws IOException {

        System.out.println("Welcome to ApolloDb!\n" +
                "           Type <help> for further info.");
        boolean isActive=true;
        String query;
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (isActive){
            query=in.readLine();
            String[] words=query.split(" ");
            if(words.length==1){switch (query) {
                case "current"-> {
                    if (DBMS.current==null) {
                        System.out.println("Not selected");
                    } else {
                        System.out.println("Current  " + DBMS.current);
                    }
                }
                case "list"-> System.out.println(DBMS.dbList);
                case "help" -> printHelp();
                case "exit" -> isActive = false;
                default -> System.out.println("Invalid command");
            }}
            else switch (words[0]){
                case "use"-> DBMS.setCurrent(words);//
                case  "drop"-> System.out.println("Deletion");//

                case "create"-> DBMS.createSchema(words);
                case "insert"-> DBMS.insert(words);
                case "select"-> System.out.println("do nothing");
                case "update"-> System.out.println("do nothing");
                case "delete"-> System.out.println("do nothing");
                default -> System.out.println("Invalid command");
            }
        }
    }

    static void printHelp(){
        System.out.println("<help>-for help");
        System.out.println("<exit> for exit");
    }
}
