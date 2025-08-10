package com.ibcai.common;
import java.util.Map;

public class Cfg {
    @SuppressWarnings("unchecked")
    public static <T> T get(Map<String,Object> map, String path, T defVal) {
        String[] parts = path.split("\\.");
        Object cur = map;
        for (String p : parts) {
            if (!(cur instanceof Map)) return defVal;
            cur = ((Map<String,Object>)cur).get(p);
            if (cur == null) return defVal;
        }
        try { return (T) cur; } catch (Exception e) { return defVal; }
    }
}
