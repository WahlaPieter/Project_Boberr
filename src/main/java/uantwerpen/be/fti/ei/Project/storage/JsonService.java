package uantwerpen.be.fti.ei.Project.storage;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JsonService {
    private static final String FILE_PATH = "nodes.json";
    private static final Logger logger = Logger.getLogger(JsonService.class.getName());

    public static void saveToJson(Map<Integer, String> nodeMap) {
        try (FileWriter file = new FileWriter(FILE_PATH)) {
            file.write("{\n");
            int count = 0;
            for (Map.Entry<Integer, String> entry : nodeMap.entrySet()) {
                file.write("  \"" + entry.getKey() + "\": \"" + entry.getValue() + "\"");
                count++;
                if (count < nodeMap.size()) {
                    file.write(",");
                }
                file.write("\n");
            }
            file.write("}\n");
            System.out.println("Data stored in " + FILE_PATH);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error with saving JSON-file", e);
        }
    }

    public static void saveStoredFiles(Map<String, Set<String>> storedFiles) {
        try (FileWriter file = new FileWriter("stored_files.json")) {
            Gson gson = new Gson();
            file.write(gson.toJson(storedFiles));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, Set<String>> loadStoredFiles() {
        try (BufferedReader reader = new BufferedReader(new FileReader("stored_files.json"))) {
            Gson gson = new Gson();
            return gson.fromJson(reader, new TypeToken<Map<String, Set<String>>>() {}.getType());
        } catch (IOException e) {
            return new HashMap<>();
        }
    }


    public static Map<Integer, String> loadFromJson() {
        Map<Integer, String> nodeMap = new java.util.TreeMap<>();
        File file = new File(FILE_PATH);

        if (!file.exists() || file.length() == 0) {
            System.out.println("No existing JSON-File found, loaded empty map.");
            return nodeMap;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(FILE_PATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("\"")) {
                    String[] parts = line.split(":");
                    if (parts.length == 2) {
                        int key = Integer.parseInt(parts[0].replace("\"", "").trim());
                        String value = parts[1].replace("\"", "").replace(",", "").trim();
                        nodeMap.put(key, value);
                    }
                }
            }
            System.out.println("Data loaded out " + FILE_PATH);
        } catch (IOException | NumberFormatException e) {
            logger.log(Level.WARNING, "Error with loading JSON-file", e);
        }
        return nodeMap;
    }
}
