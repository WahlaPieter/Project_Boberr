package uantwerpen.be.fti.ei.Project.storage;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;

import java.io.*;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class JsonService {
    private static final String NODES = "nodes.json";
    private static final String FILES = "stored_files.json";
    private static final Gson gson = new Gson();

    public static void saveToJson(Map<Integer, Node> map) {
        try (Writer w = new FileWriter(NODES)) {
            gson.toJson(map, w);
        } catch (IOException e) { e.printStackTrace(); }
    }

    public static TreeMap<Integer, Node> loadFromJson() {
        File f = new File(NODES);
        if (!f.exists()) return new TreeMap<>();
        try (Reader r = new FileReader(f)) {
            Type type = new TypeToken<TreeMap<Integer, Node>>(){}.getType();
            return gson.fromJson(r, type);
        } catch (IOException e) { e.printStackTrace(); return new TreeMap<>(); }
    }

    public static void saveStoredFiles(Map<String, Set<String>> files) {
        try (Writer w = new FileWriter(FILES)) {
            gson.toJson(files, w);
        } catch (IOException e) { e.printStackTrace(); }
    }

    public static Map<String, Set<String>> loadStoredFiles() {
        File f = new File(FILES);
        if (!f.exists()) return new TreeMap<>();
        try (Reader r = new FileReader(f)) {
            Type type = new TypeToken<TreeMap<String, Set<String>>>(){}.getType();
            return gson.fromJson(r, type);
        } catch (IOException e) { e.printStackTrace(); return new TreeMap<>(); }
    }
}