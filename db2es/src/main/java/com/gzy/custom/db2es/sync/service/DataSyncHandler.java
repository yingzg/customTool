package com.gzy.custom.db2es.sync.service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.exceptions.ExceptionUtil;
import cn.hutool.core.util.StrUtil;
import com.gzy.custom.db2es.sync.service.processor.SyncProcessor;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.env.Environment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.stereotype.Service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.TimeInterval;
import cn.hutool.extra.spring.SpringUtil;
import lombok.extern.slf4j.Slf4j;


@Service
@Slf4j
public class DataSyncHandler {
    private ThreadPoolExecutor threadPoolExecutor;
    private static  final  int ES_BULK_SIZE=1000;
    private final EsService esService;
    private final ProcessorFactory processorFactory;
    @Autowired
    private Environment env;
    @Autowired
    private RestHighLevelClient esClient;
    @Autowired
    private ElasticsearchRestTemplate elasticsearchRestTemplate;


    @Value("${elasticsearch.thread:10}")
    public void setThreads(Integer thread) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("db2es-%d").build();
        threadPoolExecutor = new ThreadPoolExecutor(thread, thread, 60,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>(), threadFactory);
        log.info("db2es同步线程池线程数为[{}]", thread);
    }

    public DataSyncHandler(EsService esService, ProcessorFactory factory) {
        this.esService = esService;
        this.processorFactory = factory;
    }

    /**
     * 获取索引名（document设置的别名，有可能是真名有可能是别名）
     * @param clazz
     * @return
     */
    public String getIndexName(Class<?> clazz) {
        return esService.getIndexName(clazz, elasticsearchRestTemplate);
    }

    /**
     * 获取索引文档type
     * @param clazz
     * @return
     */
    public String getindexType(Class<?> clazz) {
        return esService.getIndexType(clazz, elasticsearchRestTemplate);
    }
    
    public Long syncAllDataByPage(String tempIndexName, IndexEnum indexEnum, StringBuilder errMsg) {
        SyncProcessor processor = processorFactory.getProcessor(indexEnum);
        Long dbSize = processor.size();

        long totalPage = dbSize / indexEnum.getPageSize() + 1;
        String indexName = getIndexName(indexEnum.getClazz());
        log.info("[{}] DbSize: {}, totalPage: {}", indexName, dbSize, totalPage);
        List<Integer> errorPages = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Void>> cfList = new ArrayList<>();
        for (int i = 0; i < totalPage; i++) {
            final int page = i;
            CompletableFuture<Void> cf = CompletableFuture.runAsync(() -> {
                try {
                    bulkInsertByPage(tempIndexName, indexEnum, processor, page);
                } catch (Exception e) {
                    log.error("{} 数据全量同步,部分失败 page = {}, error : {}", indexName, page, e.getMessage(), e);
                    errorPages.add(page);
                }
            }, threadPoolExecutor);
            cfList.add(cf);
        }
        // 等待所有任务执行完毕
        CompletableFuture.allOf(cfList.toArray(new CompletableFuture[0])).join();
        try {
            // 失败的page重试
            for (Integer errorPage : errorPages) {
                log.info("[{}] 重试写入errorPage : page: {}", indexName, errorPage);
                bulkInsertByPage(tempIndexName, indexEnum, processor, errorPage);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            errMsg.append(StrUtil.format("{},重试同步errorPage,error:{}", indexName, ExceptionUtil.getMessage(e)));
        }
        return dbSize;
    }

    /**
     * 检查索引是否存在
     * @param indexName
     * @return
     * @throws IOException
     */
    public boolean checkIndex(String indexName) throws IOException {
        return esService.checkIndex(indexName, esClient);
    }
    
    private void bulkInsertByPage(String tempIndexName, IndexEnum indexEnum, SyncProcessor processor, int page)
        throws Exception {
        long start = System.currentTimeMillis();
        String indexName = getIndexName(indexEnum.getClazz());
        String indexType = getindexType(indexEnum.getClazz());
        List<Map<String, Object>> list = processor.bulkget(indexEnum.getPageSize(), page);
        long end1 = System.currentTimeMillis();
        if (!list.isEmpty()) {
            esService.bulkInsert(tempIndexName, indexType, indexEnum, list, esClient);
            long end2 = System.currentTimeMillis();
            log.info("[{}] 同步ES : page: {}, size: {}, read sql cost : {}, sync es cost : {}", indexName, page,
                list.size(), end1 - start, end2 - end1);
            list.clear();
        }
    }

    public Long syncAllData(String tempIndexName, IndexEnum indexEnum, StringBuilder errMsg) {
        SyncProcessor processor = processorFactory.getProcessor(indexEnum);
        String indexName = getIndexName(indexEnum.getClazz());
        String indexType = getindexType(indexEnum.getClazz());
        try {
            long start = System.currentTimeMillis();
            List<Map<String, Object>> list = processor.getAll();
            int size = syncList(tempIndexName, indexType, indexEnum, indexName, start, list, errMsg);
            return (long)size;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            errMsg.append(StrUtil.format("{},全量数据同步异常,error:{}", indexName, ExceptionUtil.getMessage(e)));
        }
        return null;
    }

    private int syncList(String indexTrueName, String indexType, IndexEnum indexEnum, String indexName, long start,
        List<Map<String, Object>> list, StringBuilder errMsg) {
        int size = list.size();
        long end1 = System.currentTimeMillis();
        if (!list.isEmpty()) {
            List<List<Map<String, Object>>> split = ListUtil.split(list, ES_BULK_SIZE);
            log.info("[{}] 查询数据成功,数据量为 [{}],分[{}]page插入ES", indexName, size, split.size());
            List<CompletableFuture<Void>> cfList = new ArrayList<>();
            for (int i = 0; i < split.size(); i++) {
                int page = i;
                CompletableFuture<Void> cf = CompletableFuture.runAsync(() -> {
                    try {
                        long es1 = System.currentTimeMillis();
                        esService.bulkInsert(indexTrueName, indexType, indexEnum, split.get(page), esClient);
                        long es2 = System.currentTimeMillis();
                        log.info("[{}] 同步ES ：page: {}, size: {}, es cost : {}", indexName, page, split.get(page).size(),
                            es2 - es1);
                    } catch (Exception e) {

                        String err = StrUtil.format("{} 数据同步,部分失败 page = {}, error : {}", indexName, page,
                            ExceptionUtil.getMessage(e));
                        log.error(err);
                        errMsg.append(err);
                    }
                }, threadPoolExecutor);
                cfList.add(cf);
            }
            // 等待所有任务执行完毕
            CompletableFuture.allOf(cfList.toArray(new CompletableFuture[0])).join();
            long end2 = System.currentTimeMillis();
            log.info("[{}] 同步ES read sql cost : {},sync es cost : {}", indexName, end1 - start, end2 - end1);
        }
        return size;
    }

    public float checkDataCount(Long dataSize, String indexName) {
        long tempSize = esService.getDocCount(indexName, esClient);
        log.info("[{}]数据库数据量为[{}],[{}]索引数据量为[{}]", indexName, dataSize, indexName, tempSize);
        return (float)Math.abs(dataSize - tempSize) / dataSize;
    }

    public long checkDataCount(String indexName, IndexEnum indexEnum) {
        SyncProcessor processor = processorFactory.getProcessor(indexEnum);
        long esSize = esService.getDocCount(indexName, esClient);
        Long dataSize = processor.size();
        log.info("[{}]数据库数据量为[{}],[{}]索引数据量为[{}]", indexName, dataSize, indexName, esSize);
        return dataSize - esSize;
    }
    
    public boolean syncByTime(IndexEnum indexEnum, String lastUpdatedTime, String indexTrueName, StringBuilder msg) {
        SyncProcessor processor = processorFactory.getProcessor(indexEnum);
        try {
            long start = System.currentTimeMillis();
            String indexName = getIndexName(indexEnum.getClazz());
            String indexType = getindexType(indexEnum.getClazz());
            List<Map<String, Object>> addIds = processor.getAddByTime(lastUpdatedTime);
            if (!addIds.isEmpty()) {
                syncList(indexTrueName, indexType, indexEnum, indexName, start, addIds, msg);
                String info = StrUtil.format("[{}] 数据增量成功,新增和更新共[{}]条数据", indexName, addIds.size());
                log.info(info);
                msg.append(info);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            msg.append(StrUtil.format("[{}] syncByTime同步异常,ex:", indexTrueName, ExceptionUtil.getMessage(e)));
            return false;
        }
        return true;
    }

    public boolean delByTime(IndexEnum indexEnum, String delCycleTime, String indexTrueName, StringBuilder msg) {
        SyncProcessor processor = processorFactory.getProcessor(indexEnum);
        try {
            List<String> cnDelIds = processor.getIdsByDelTime(delCycleTime);
            if (CollectionUtil.isNotEmpty(cnDelIds)) {
                String info = StrUtil.format("[{}] 数据从[{}]删除，共[{}]条数据", indexTrueName, delCycleTime, cnDelIds.size());
                log.info(info);
                msg.append(info);
                String indexType = getindexType(indexEnum.getClazz());
                return delByIds(indexTrueName, indexType, cnDelIds);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            msg.append(StrUtil.format("[{}] delByTime 同步异常,ex:[{}]", indexTrueName, ExceptionUtil.getMessage(e)));
            return false;
        }
        return true;
    }

    public boolean delByDiffer(IndexEnum indexEnum, String indexTrueName,StringBuilder msg) {
        TimeInterval timer = DateUtil.timer();
        String indexName = getIndexName(indexEnum.getClazz());
        String indexType = getindexType(indexEnum.getClazz());
        log.info("[{}] 对比索引id与数据库的差异开始...", indexName);
        SyncProcessor processor = processorFactory.getProcessor(indexEnum);
        List<String> esIds = esService.getIdsByIndexName(indexEnum, indexName,esClient);
        long o1 = timer.intervalSecond();
        log.info("[{}] 查询ES的索引id所用时间为[{}]秒", indexName, o1);
        List<String> dbIds = processor.getIds();
        log.info("[{}] 查询DB的索引id所用时间为[{}]秒", indexName, timer.intervalSecond() - o1);
        List<String> esOverIds = getDifferListByMap(esIds, dbIds);
        return delByIds(indexTrueName, indexType, esOverIds);
    }

    private List<String> getDifferListByMap(List<String> list, List<String> otherList) {
        if (CollectionUtil.isEmpty(list)) {
            return new ArrayList<>();
        }
        if (CollectionUtil.isEmpty(otherList)) {
            return list;
        }
        Map<String, String> tempMap = otherList.stream()
            .collect(Collectors.toMap(Function.identity(), Function.identity(), (oldData, newData) -> newData));
        return list.stream().filter(str -> !tempMap.containsKey(str)).collect(Collectors.toList());
    }
    
    private boolean delByIds(String indexTrueName, String type, List<String> delIds) {
        try {
            esService.bulkDelete(indexTrueName, type, delIds, esClient);
            log.info("[{}] 删除了[{}]条数据", indexTrueName.substring(0, indexTrueName.lastIndexOf("_")), delIds.size());
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return false;
        }
        return true;
    }


    public boolean syncIndexById(IndexEnum indexEnum, List<String> ids, String indexTrueName, StringBuilder msg) {
        Long timeStart = System.currentTimeMillis();
        SyncProcessor processor = processorFactory.getProcessor(indexEnum);
        String indexName = getIndexName(indexEnum.getClazz());
        String indexType = getindexType(indexEnum.getClazz());
        try {
            List<Map<String, Object>> list = processor.getAddByIds(ids);
            if (!list.isEmpty()) {
                esService.bulkInsert(indexTrueName, indexType, indexEnum, list, esClient);
            }
            Long timeEnd = System.currentTimeMillis();
            String info = StrUtil.format("[{}] 数据指定id同步，同步数据量为[{}],同步所用时间为[{}]秒", indexName,
                CollectionUtil.isNotEmpty(list) ? list.size() : 0, (timeEnd - timeStart) / 1000);
            msg.append(info);
            log.info(info);
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            msg.append(StrUtil.format("[{}] syncIndexById 同步异常,ex:[{}]", indexTrueName, ExceptionUtil.getMessage(e)));
            return false;
        }
    }

    public boolean delIndexById(IndexEnum indexEnum, List<String> ids, String indexTrueName) {
        Long timeStart = System.currentTimeMillis();
        String indexName = getIndexName(indexEnum.getClazz());
        String indexType = getindexType(indexEnum.getClazz());
        try {
            if (!ids.isEmpty()) {
                esService.bulkDelete(indexTrueName, indexType, ids, esClient);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
        Long timeEnd = System.currentTimeMillis();
        log.info("[{}] 数据指定id删除结束,所用时间为[{}]秒", indexName, (timeEnd - timeStart) / 1000);
        return true;
    }
    
    public boolean createIndex(String tempIndexName, String mappingSource) {
        return esService.createIndex(tempIndexName, mappingSource, esClient);
    }

    public boolean createIndex(Class<?> clazz, String indexName) {
        return esService.createIndex(clazz, indexName, elasticsearchRestTemplate);
    }

    public boolean updateIndexSetting(String tempIndexName, IndexEnum indexEnum) {
        Integer numberOfReplicas = 0;
        if ("dev".equals(SpringUtil.getActiveProfile())) {
            numberOfReplicas = indexEnum.getNumberOfReplicas();
        }
        String indexName = getIndexName(indexEnum.getClazz());
        Integer refreshInterval = indexEnum.getRefreshInterval();
        log.info("[{}] 更新索引的副本数为[{}],刷新间隔为[{}]", indexName, numberOfReplicas, refreshInterval);
        return esService.updateIndexSetting(tempIndexName, numberOfReplicas, refreshInterval, esClient);
    }

    public String getIndexNameByAlias(IndexEnum indexEnum) throws IOException {
        String indexName = getIndexName(indexEnum.getClazz());
        return esService.getCurrentIndexNameByAlias(indexName, esClient);
    }

    public boolean aliasIndex(String newIndexName, String preIndexName, IndexEnum indexEnum) {
        String indexName = getIndexName(indexEnum.getClazz());
        return esService.aliasIndex(newIndexName, indexName, preIndexName, esClient);
    }

    public boolean addAlias(String indexName, String indexTrueName) {
        return esService.addAliasForIndex(indexName, indexTrueName, esClient);
    }
    
    public Set<String> getAlias(String indexName) throws IOException {
        return esService.getAlias(indexName, esClient);
    }
    
    public boolean deleteIndex(String indexTrueName) {
        return esService.deleteIndex(indexTrueName, esClient);
    }
    
    public Map<String, List<String>> checkDifferIdsByIndexName(IndexEnum indexEnum) {
        TimeInterval timer = DateUtil.timer();
        String indexName = getIndexName(indexEnum.getClazz());
        log.info("[{}] 对比索引id与数据库的差异开始...", indexName);
        SyncProcessor processor = processorFactory.getProcessor(indexEnum);
        CompletableFuture<List<String>> futureEsIds =
            CompletableFuture.supplyAsync(() -> esService.getIdsByIndexName(indexEnum, indexName, esClient))
                .whenComplete((r, e) -> log.info("[{}] 查询ES的索引id所用时间为[{}]秒", indexName, timer.intervalSecond()));
        CompletableFuture<List<String>> futureDbIds = CompletableFuture.supplyAsync(processor::getIds)
            .whenComplete((r, e) -> log.info("[{}] 查询DB的索引id所用时间为[{}]秒", indexName, timer.intervalSecond()));
        List<String> esIdList = futureEsIds.join();
        List<String> dbIdList = futureDbIds.join();
        Map<String, List<String>> map = new HashMap<>(2);
        List<String> dbOverIds = getDifferListByMap(dbIdList, esIdList);
        map.put("db-es", dbOverIds);
        log.info("[{}] 数据库比ES多的id为[{}]", indexName, dbOverIds);
        List<String> esOverIds = getDifferListByMap(esIdList, dbIdList);
        map.put("es-db", esOverIds);
        log.info("[{}] ES比数据库多的id为[{}]", indexName, esOverIds);
        log.info("[{}] 对比id所用总时间为[{}]秒", indexName, timer.intervalSecond());
        return map;
    }



    public long getCurrentDocCount(String indexName) {
        return esService.getDocCount(indexName, esClient);
    }

}
