package com.gzy.custom.cache.localcache;

import java.util.Collection;
import java.util.Map;

public interface InnerMap {
    Object getValue(Object key);

    Map getAllValues(Collection keys);

    void putValue(Object key, Object value);

    void putAllValues(Map map);

    boolean removeValue(Object key);

    boolean putIfAbsentValue(Object key, Object value);

    void removeAllValues(Collection keys);
}
