package uantwerpen.be.fti.ei.Project.storage;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;

import java.io.*;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JsonService {
    private static final String FILE_PATH = "nodes.json";
    private static final Logger logger = Logger.getLogger(JsonService.class.getName());
    private static final Gson gson = new Gson();

    public static void saveToJson(Map<Integer, Node> nodeMap) {
        try (FileWriter file = new FileWriter(FILE_PATH)) {
            gson.toJson(nodeMap, file); // Properly serialize the entire nodeMap
            System.out.println("Data stored in " + FILE_PATH);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error saving JSON file", e);
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

    public static Map<Integer, Node> loadFromJson() {
        File file = new File(FILE_PATH);
        if (!file.exists() || file.length() == 0) {
            return new TreeMap<>();
        }
        try (Reader reader = new FileReader(FILE_PATH)) {
            Type type = new TypeToken<TreeMap<Integer, Node>>(){}.getType();
            return gson.fromJson(reader, type); // Deserialize into Node objects
        } catch (IOException e) {
            logger.log(Level.WARNING, "Error loading JSON file", e);
            return new TreeMap<>();
        }
    }
}
