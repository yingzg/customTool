package com.gzy.custom.db2es.sync.service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.io.resource.ResourceUtil;
import com.gzy.custom.db2es.sync.exception.IndexSyncException;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.data.elasticsearch.annotations.Setting;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentEntity;
import org.springframework.stereotype.Service;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;


@Service
@Slf4j
public class EsService {
    private static final int PAGE_SIZE = 1000;

    public boolean checkIndex(String indexName, RestHighLevelClient restHighLevelClient) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        return restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
    }

    /**
     * 切换别名
     * @param newIndexName
     * @param aliasName
     * @param preIndexName
     * @param restHighLevelClient
     * @return
     */
    public boolean aliasIndex(String newIndexName, String aliasName, String preIndexName, RestHighLevelClient restHighLevelClient) {
        try {
            IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
            IndicesAliasesRequest.AliasActions add = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD);
            add.alias(aliasName);
            add.index(newIndexName);
            indicesAliasesRequest.addAliasAction(add);
            if (StringUtils.isNotBlank(preIndexName) && checkIndex(preIndexName, restHighLevelClient)) {
                IndicesAliasesRequest.AliasActions remove = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE);
                remove.alias(aliasName);
                remove.index(preIndexName);
                indicesAliasesRequest.addAliasAction(remove);
            }
            AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
            return acknowledgedResponse.isAcknowledged();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    public boolean addAliasForIndex(String aliasName, String indexTrueName, RestHighLevelClient restHighLevelClient) {
        try {
            boolean isExists = checkIndex(indexTrueName, restHighLevelClient);
            if (!isExists) {
                return false;
            }
            IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
            if (StringUtils.isNotBlank(indexTrueName)) {
                IndicesAliasesRequest.AliasActions add =
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD);
                add.alias(aliasName);
                add.index(indexTrueName);
                indicesAliasesRequest.addAliasAction(add);
            }
            AcknowledgedResponse acknowledgedResponse =
                restHighLevelClient.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
            return acknowledgedResponse.isAcknowledged();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    /**
     * 根据别名查询索引真名
     * @param indexName 别名
     * @param restHighLevelClient
     * @return 索引真名
     * @throws IOException
     */
    public String getCurrentIndexNameByAlias(String indexName, RestHighLevelClient restHighLevelClient) throws IOException {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest(indexName);
        GetAliasesResponse getAliasesResponse = restHighLevelClient.indices().getAlias(getAliasesRequest, RequestOptions.DEFAULT);
        Set<String> aliases = getAliasesResponse.getAliases().keySet();
        return CollUtil.getLast(new ArrayList<>(aliases));
    }


    public boolean updateIndexSetting(String indexName, int replicas, int refreshInterval, RestHighLevelClient restHighLevelClient) {
        try {
            UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest();
            updateSettingsRequest.indices(indexName);
            JSONObject settings = new JSONObject();
            settings.set("number_of_replicas", replicas);
            settings.set("refresh_interval", refreshInterval + "s");
            settings.set("index.translog.durability", "async");
            updateSettingsRequest.settings(settings);
            AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
            return acknowledgedResponse.isAcknowledged();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    public boolean createIndex(String indexName, String source, RestHighLevelClient restHighLevelClient) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.source(source, XContentType.JSON);
        try {
            CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            return createIndexResponse.isAcknowledged();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return false;
    }

    public void bulkInsert(String indexName, String indexType, IndexEnum indexEnum, List<Map<String, Object>> list,
        RestHighLevelClient restHighLevelClient) throws Exception {
        BulkRequest request = new BulkRequest();
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (Map<String, Object> map : list) {
            Object Id = map.get(indexEnum.getEsIdField());
            if (Id == null) {
                log.error("[{}]索引批量同步，docId值为空", indexName);
                continue;
            }
            request.add(new IndexRequest(indexName).type(indexType).id(Id.toString()).source(JSONUtil.toJsonStr(map),
                XContentType.JSON));
        }
        BulkResponse bulkResponse = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        if (bulkResponse.hasFailures()) {
            log.error(bulkResponse.buildFailureMessage());
            throw new IndexSyncException("批量写入ES存在异常");
        }
    }

    public void bulkDelete(String indexName, String type, List<String> ids, RestHighLevelClient restHighLevelClient)
        throws IOException {
        if (CollUtil.isNotEmpty(ids)) {
            BulkRequest request = new BulkRequest();
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (String id : ids) {
                request.add(new DeleteRequest(indexName, type, id));
            }
            BulkResponse bulkResponse = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                log.error(bulkResponse.buildFailureMessage());
            }
        }
    }

    public boolean deleteIndex(String indexName, RestHighLevelClient restHighLevelClient) {
        try {
            AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
            return acknowledgedResponse.isAcknowledged();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    public long getDocCount(String indexName, RestHighLevelClient restHighLevelClient) {
        if (indexName == null) {
            return 0;
        }
        CountRequest countRequest = new CountRequest(indexName);
        try {
            CountResponse countResponse = restHighLevelClient.count(countRequest, RequestOptions.DEFAULT);
            return countResponse.getCount();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return 0;
    }


    public List<String> getIdsByIndexName(IndexEnum indexEnum,String indexName, RestHighLevelClient restHighLevelClient) {
        List<String> list = CollUtil.newArrayList();
        //构造查询条件
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //设置查询超时时间
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        builder.query(QueryBuilders.matchAllQuery());
        builder.storedField(indexEnum.getEsIdField());
        //设置最多一次能够取出1000笔数据，从第1001笔数据开始，将开启滚动查询  PS:滚动查询也属于这一次查询，只不过因为一次查不完，分多次查
        builder.size(PAGE_SIZE);
        searchRequest.source(builder);
        //将滚动放入
        searchRequest.scroll(scroll);
        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("滚动查询失败[{}]", e.getMessage(), e);
        }
        if (searchResponse == null || searchResponse.getHits() == null) {
            log.info("[{}] searchResponse is null", indexName);
            return null;
        }
        SearchHits hits = searchResponse.getHits();
        //记录要滚动的ID
        String scrollId = searchResponse.getScrollId();
        //滚动查询部分，将从第10001笔数据开始取
        SearchHit[] hitsScroll = hits.getHits();
        collectIds(list, hitsScroll);
        while (ArrayUtil.isNotEmpty(hitsScroll)) {
            //构造滚动查询条件
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll(scroll);
            try {
                //响应必须是上面的响应对象，需要对上一层进行覆盖
                searchResponse = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("滚动查询失败[{}]", e.getMessage(), e);
            }
            scrollId = searchResponse.getScrollId();
            hits = searchResponse.getHits();
            hitsScroll = hits.getHits();
            collectIds(list, hitsScroll);
        }
        //清除滚动，否则影响下次查询
        cleanScroll(restHighLevelClient, scrollId);
        return list;
    }

    private void cleanScroll(RestHighLevelClient restHighLevelClient, String scrollId) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = null;
        try {
            clearScrollResponse = restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("滚动查询清除失败[{}]", e.getMessage(), e);
        }
        //清除滚动是否成功
        assert clearScrollResponse != null;
        if (!clearScrollResponse.isSucceeded()) {
            log.error("滚动查询清除失败");
        }
    }

    public String getIndexName(Class<?> clazz, ElasticsearchRestTemplate elasticsearchRestTemplate) {
        return elasticsearchRestTemplate.getPersistentEntityFor(clazz).getIndexName();
    }

    public String getIndexType(Class<?> clazz, ElasticsearchRestTemplate elasticsearchRestTemplate) {
        return elasticsearchRestTemplate.getPersistentEntityFor(clazz).getIndexType();
    }

    public boolean createIndex(Class<?> clazz, String indexName, ElasticsearchRestTemplate elasticsearchRestTemplate) {
        boolean hasAnnotationSetting = false;
        boolean isSuccess = false;
        if (clazz.isAnnotationPresent(Setting.class)) {
            String settingPath = ((Setting)clazz.getAnnotation(Setting.class)).settingPath();
            if (org.springframework.util.StringUtils.hasText(settingPath)) {
                String settings = ResourceUtil.readUtf8Str(settingPath);
                if (org.springframework.util.StringUtils.hasText(settings)) {
                    hasAnnotationSetting = true;
                    isSuccess = elasticsearchRestTemplate.createIndex(indexName, settings);
                }

            } else {
                log.info("settingPath in @setting hash to be defined. using default instead.");
            }
        }
        Map setting = null;
        if (!hasAnnotationSetting && isSuccess) {
            setting = getDefaultSetting(elasticsearchRestTemplate.getPersistentEntityFor(clazz));
            isSuccess = elasticsearchRestTemplate.createIndex(indexName, setting);
        }
        if (isSuccess) {
            elasticsearchRestTemplate.putMapping(indexName, getIndexType(clazz, elasticsearchRestTemplate), clazz);
        }
        return isSuccess;
    }
    
    private <T> Map getDefaultSetting(ElasticsearchPersistentEntity<T> persistentEntity) {
        return (Map)(persistentEntity.isUseServerConfiguration() ? new HashMap<>()
            : (new MapBuilder<>()).put("index.number_of_shards", String.valueOf(persistentEntity.getShards()))
                .put("index.number_of_replicas", String.valueOf(persistentEntity.getReplicas()))
                .put("index.refresh_Interval", String.valueOf(persistentEntity.getRefreshInterval()))
                .put("index.store.type", String.valueOf(persistentEntity.getIndexStoreType())).map());
    }
    
    /**
     * 对结果集的处理
     */
    private void collectIds(List<String> list, SearchHit[] hitsScroll) {
        for (SearchHit hit : hitsScroll) {
            list.add(hit.getId());
        }
    }

    /**
     * 获取索引别名
     * @param indexName 索引名
     * @param restHighLevelClient
     * @return
     * @throws IOException
     */
    public Set<String> getAlias(String indexName, RestHighLevelClient restHighLevelClient) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        GetIndexResponse response = restHighLevelClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        Map<String, List<AliasMetadata>> aliasMap = response.getAliases();
        Set<String> alias = new HashSet<>();
        if (CollectionUtil.isNotEmpty(aliasMap) && CollectionUtil.isNotEmpty(aliasMap.values())) {
            aliasMap.values().forEach(item -> {
                alias.addAll(item.stream().map(AliasMetadata::getAlias).collect(Collectors.toList()));
            });
        }
        return alias;
    }

}
