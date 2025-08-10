package com.ibcai.common;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@SuppressWarnings("unchecked")
public class ConfigLoader {

    public static Map<String, Object> load(String[] args) {
        String cfgPath = "./config.yml";
        for (String a : args) {
            if (a.startsWith("--config=")) cfgPath = a.substring("--config=".length());
        }
        final Map<String, Object> cfg = new HashMap<>();
        try (InputStream in = Files.newInputStream(Path.of(cfgPath))) {
            Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
            Object root = yaml.load(in);
            if (root instanceof Map) cfg.putAll((Map<String, Object>) root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config.yml at " + cfgPath + ": " + e.getMessage(), e);
        }

        Map<String, String> env = System.getenv();
        env.forEach((k,v) -> {
            if (k.startsWith("CFG__")) {
                String path = k.substring(5);
                String[] keys = path.split("__");
                apply(cfg, Arrays.asList(keys), v);
            }
        });
        return cfg;
    }

    private static void apply(Map<String, Object> root, List<String> path, String value) {
        Map<String, Object> cur = root;
        for (int i = 0; i < path.size(); i++) {
            String key = toCamel(path.get(i));
            if (i == path.size() - 1) {
                cur.put(key, coerce(value));
            } else {
                Object nxt = cur.get(key);
                if (!(nxt instanceof Map)) {
                    nxt = new LinkedHashMap<String, Object>();
                    cur.put(key, nxt);
                }
                cur = (Map<String, Object>) nxt;
            }
        }
    }

    private static Object coerce(String v) {
        if (v == null) return null;
        String s = v.trim();
        if ("true".equalsIgnoreCase(s)) return true;
        if ("false".equalsIgnoreCase(s)) return false;
        try { return Integer.parseInt(s); } catch (Exception ignore) {}
        try { return Long.parseLong(s); } catch (Exception ignore) {}
        try { return Double.parseDouble(s); } catch (Exception ignore) {}
        return s;
    }

    private static String toCamel(String key) {
        String k = key.toLowerCase(Locale.ROOT);
        String[] parts = k.split("_");
        if (parts.length == 1) return key;
        StringBuilder sb = new StringBuilder(parts[0]);
        for (int i=1;i<parts.length;i++) {
            String p = parts[i];
            if (p.isEmpty()) continue;
            sb.append(Character.toUpperCase(p.charAt(0)));
            if (p.length() > 1) sb.append(p.substring(1));
        }
        return sb.toString();
    }
}
