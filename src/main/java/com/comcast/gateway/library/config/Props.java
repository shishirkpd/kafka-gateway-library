package com.comcast.gateway.library.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Props {

    private final Map<String, Object> map;

    public static Props emptyProps() {
        return new Props();
    }

    public static Props propsFrom(Map<String, Object> m) {
        return new Props(m);
    }

    public static Props propsFrom(Properties p) {
        return new Props(p);
    }

    public Props() {
        this(new HashMap<>());
    }

    public Props(Map<String, Object> m) {
        this.map = new HashMap<>(m);
    }
    public Props(Properties p){
        this.map = toMap(p);
    }

    public static Map<String, Object> toMap(Properties p) {
        return p.entrySet().stream().collect(Collectors.toMap(e -> (String)e.getKey(), Map.Entry::getValue));
    }

    public static Properties toProperties(Map<String, Object> m) {
        Properties p = new Properties();
        p.putAll(m);
        return p;
    }

    public <T> Props withProperty(String key, Supplier<T> s) {
        Optional.ofNullable(s.get()).ifPresent(v -> map.put(key, v));
        return this;
    }

    public Map<String, Object> toStringObjectMap(){
        return this.map;
    }

    public Properties toProperties() {
        return toProperties(this.map);
    }
}
