package com.gmail.yeritsyankoryun.ApolloDb.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Repl {
     static void start() throws IOException {

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
                    if (DbService.current==null) {
                        System.out.println("Not selected");
                    } else {
                        System.out.println("Current  " + DbService.current);
                    }
                }
                case "list"-> System.out.println(DbService.dbList);
                case "help" -> printHelp();
                case "exit" -> isActive = false;
                case  "drop"-> TableService.drop();
                case "clear"-> TableService.deletion(words);
                default -> System.out.println("Invalid command");
            }}
            else switch (words[0]){
                case "use"-> DbService.setCurrent(words);
                case "create"-> TableService.create(words);
                case "insert"-> TableService.insert(words);
                case "select"-> TableService.select(words);
                case "update"-> TableService.update(words);
                case "clear","remove"-> TableService.deletion(words);
                default -> System.out.println("Invalid command");
            }
        }
    }

    private static void printHelp(){
        System.out.println("<help> - get commands info ");
        System.out.println("<list> - print Db name's list");
        System.out.println("<use>+<dbName> - use existing or create new db by 'dbName'");
        System.out.println("<current> - return current selected dbName");
        System.out.println("<drop>+<dbName> - remove current db files from source");
        System.out.println("<create> + <tableName> + 'Schema'(Optional) - create by tableName (+keeping Schema and inserts item) ");
        System.out.println("Ex: create users name='John' age=40 gender='m' vaccinated=true pets:(UUID) hobbies:['swim','run',141,true]");
        System.out.println("<insert> + <tableName> + <Schema> - inserts to existing table by tableName and dbNameSchema");
        System.out.println("<select> +<tableName> + 'Schema'(Optional) - select all table items(or where k=v...)");
        System.out.println("<update> + <tableName> + 'Schema'(Optional) - update current selected table Items by provided schema");
        System.out.println("<clear> +<tableName>(Optional) - clear full Db(or table by name) ");
        System.out.println("<remove>+<tableName>+<UUID> - clear table item by id");
        System.out.println("<exit>-exit application");
    }
}
