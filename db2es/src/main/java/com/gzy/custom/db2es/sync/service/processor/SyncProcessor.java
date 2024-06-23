package com.gzy.custom.db2es.sync.service.processor;

import com.gzy.custom.db2es.sync.exception.IndexSyncException;

import java.util.List;
import java.util.Map;

public interface SyncProcessor {

    default String getName() throws IndexSyncException {
        throw new IndexSyncException("SyncProcessor name can not be empty");
    }

    default List<Map<String, Object>> bulkget(int Pagesize, int pageIndex) {
        throw new IndexSyncException("未实现方法bulkgetData");
    }

    default List<Map<String, Object>> getAll() {
        throw new IndexSyncException("未实现方法getAll");
    }

    default Long size() {
        throw new IndexSyncException("未实现方法size");
    }

    /**
     * 获取全量的主键ID列表（用于对比ES和DB的数据量）
     * @return
     */
    default List<String> getIds() {
        throw new IndexSyncException("未实现方法getIds");
    }

    default List<Map<String, Object>> getAddByTime(String lastUpdateTime) {
        throw new IndexSyncException("未实现方法getAddByTime");
    }

    default List<Map<String, Object>> getAddByIds(List<String> ids) {
        throw new IndexSyncException("未实现方法ggetAddByIds");
    }

    default List<String> getIdsByDelTime(String delTime) {
        throw new IndexSyncException("未实现方法getIdsByDelTime");
    }

}
