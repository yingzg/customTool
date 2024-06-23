package com.gzy.custom.db2es.sync.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.exceptions.ExceptionUtil;
import cn.hutool.core.util.IdUtil;
import com.alibaba.fastjson2.JSON;
import com.gzy.custom.db2es.sync.exception.IndexSyncException;
import com.gzy.custom.db2es.sync.model.TaskRecord;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Validator;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.extra.mail.MailAccount;
import cn.hutool.extra.mail.MailUtil;
import cn.hutool.extra.spring.SpringUtil;
import lombok.extern.slf4j.Slf4j;


@Service
@Slf4j
public class SyncService {
    private static final String PROD_ACTIVE = "prod";

    /**
     * 防止对同一个index重复重建
     */
    private static final Set<IndexEnum> INDEX_DOING_SET = new CopyOnWriteArraySet<>();

    public static Set<IndexEnum> getIndexDoingSet() {
        return INDEX_DOING_SET;
    }

    private final DataSyncHandler dataSyncHandler;
    private final TaskRecordService taskRecordService;
    // 检测同步是否成功前需要等待 数据刷进os cache
    private final static int TIN_WAIT_MILLIS = 2 * 1000;
    private final static int LONG_WAIT_MILLIS = 10 * 1000;

    public SyncService(DataSyncHandler dataSyncHandler, TaskRecordService taskRecordService) {
        this.dataSyncHandler = dataSyncHandler;
        this.taskRecordService = taskRecordService;
    }
    
    @Value("${spring.profiles.active}")
    private String activeEnv;

    private TaskRecord addRecord(String inputParm, String eventType, String indexName, String operater) {
        inputParm = StringUtils.isEmpty(inputParm) ? "" : inputParm;
        long Id = IdUtil.getSnowflake(1, 10).nextId();
        String relateId = IdUtil.getSnowflake(1, 10).nextIdStr();
        TaskRecord taskRecord = TaskRecord.builder().indexName(indexName).id(Id).relateId(relateId).eventType(eventType)
            .status(TaskRecord.RECORD_STATUS_CREATE).inputJson(inputParm.length() >= 4096 ? "" : inputParm)
            .createTime(DateUtil.format(DateTime.now(), "yyyy-MM-dd HH:mm:ss"))
            .updateTime(DateUtil.format(DateTime.now(), "yyyy-MM-dd HH:mm:ss")).operater(operater).build();
        taskRecordService.saveOrUpdate(taskRecord);
        log.info("[{}] 新增记录,relateId={}", indexName, relateId);
        return taskRecord;
    }
    
    public void updateRecord(TaskRecord record, String status, String msg) {
        if (StringUtils.isEmpty(status) && StringUtils.isEmpty(msg)) {
            return;
        }
        if (StringUtils.isNotEmpty(status)) {
            record.setStatus(status);
        }
        if (StringUtils.isNotEmpty(msg)) {
            StringBuilder builder = new StringBuilder();
            if (StringUtils.isNotEmpty(record.getRemark())) {
                builder.append(record.getRemark()).append("/n").append(msg);
            } else {
                builder.append(msg);
            }
            record.setRemark(builder.toString().length() >= 8192 ? "" : builder.toString());
        }
        taskRecordService.saveOrUpdate(record);
    }
    
    public void rebuildIndex(IndexEnum indexEnum, String operater) {
        Long timeStart = System.currentTimeMillis();
        String indexName = dataSyncHandler.getIndexName(indexEnum.getClazz());
        TaskRecord taskRecord = addRecord(null, TaskRecord.TASK_EVENT_REBUILD_INDEX, indexName, operater);

        if (INDEX_DOING_SET.contains(indexEnum)) {
            recordError(indexEnum, indexName, taskRecord, "正在进行同步,请确认", true, false);
        }
        INDEX_DOING_SET.add(indexEnum);
        updateRecord(taskRecord, TaskRecord.RECORD_STATUS_DOING, null);

        log.info("开始同步全量 [{}] 数据", indexName);
        String tempIndexName = indexName + "_" + DateUtil.format(DateUtil.date(), DatePattern.PURE_DATETIME_PATTERN);
        log.info("[{}]: 临时索引名为: [{}]", indexName, tempIndexName);
        // 1.get mappingSource 2.create index
        prepareWork(indexEnum, tempIndexName, taskRecord);
        // 2.sync data
        boolean isPage = indexEnum.getPageSize() != null;
        Long dbSize;
        dbSize = doSync(indexEnum, indexName, taskRecord, tempIndexName, isPage);
        // 3.update setting
        boolean isUpdateSettingSuccess = dataSyncHandler.updateIndexSetting(tempIndexName, indexEnum);
        if (!isUpdateSettingSuccess) {
            recordError(indexEnum, indexName, taskRecord, "更新setting失败", true, true);
        }
        // 4.check indexSize 5.change indexAlias
        shiftIndex(indexEnum, indexName, taskRecord, tempIndexName, isPage, dbSize);

        Long timeEnd = System.currentTimeMillis();
        recordSuccess(indexEnum, indexName, taskRecord,
            StrUtil.format("数据全量同步成功,用时[{}]s", (timeEnd - timeStart) / 1000));
    }

    private void prepareWork(IndexEnum indexEnum, String indexName, TaskRecord taskRecord) {
        // 1.get mappingSource
        Class<?> clazz = indexEnum.getClazz();
        if (clazz == null) {
            recordError(indexEnum, indexName, taskRecord, "mapping不存在", true, true);
        }
        // 2.create index
        boolean isCreateSuccess = dataSyncHandler.createIndex(clazz, indexName);
        if (!isCreateSuccess) {
            recordError(indexEnum, indexName, taskRecord, "索引创建失败", true, true);
        }
    }

    private Long doSync(IndexEnum indexEnum, String indexName, TaskRecord taskRecord, String tempIndexName,
        boolean isPage) {
        Long dbSize;
        StringBuilder errorMsg = new StringBuilder();
        if (isPage) {
            // 分页方式同步
            dbSize = dataSyncHandler.syncAllDataByPage(tempIndexName, indexEnum, errorMsg);
            if (dbSize == null) {
                recordError(indexEnum, indexName, taskRecord, "数据全量同步失败-分页方式", true, true);
            }
        } else {
            // 直接查询同步
            dbSize = dataSyncHandler.syncAllData(tempIndexName, indexEnum, errorMsg);
            if (dbSize == null) {
                recordError(indexEnum, indexName, taskRecord, "数据全量同步失败-不分页方式", true, true);
            }
        }
        return dbSize;
    }

    private void shiftIndex(IndexEnum indexEnum, String indexName, TaskRecord taskRecord, String tempIndexName,
        boolean isPage, Long dbSize) {
        // 数据量大的话需要多等待时间 然后才能验证数量
        if (isPage) {
            ThreadUtil.sleep(LONG_WAIT_MILLIS);
        } else {
            ThreadUtil.sleep(TIN_WAIT_MILLIS);
        }
        StringBuilder errMsg = new StringBuilder();
        boolean isSuccess = checkDataCount(indexEnum, indexName, tempIndexName, dbSize, errMsg);
        if (!isSuccess) {
            recordError(indexEnum, indexName, taskRecord, errMsg.toString(), true, true);
        }
        // 索引别名切换索引
        isSuccess = shiftIndex(indexName, tempIndexName, indexEnum, errMsg);
        if (!isSuccess) {
            recordError(indexEnum, indexName, taskRecord, errMsg.toString(), true, true);
        }
    }

    private boolean shiftIndex(String indexName, String tempIndexName, IndexEnum indexEnum, StringBuilder errMsg) {
        try {
            String indexTrueName = dataSyncHandler.getIndexNameByAlias(indexEnum);
            if (StringUtils.isEmpty(indexTrueName)) {
                dataSyncHandler.deleteIndex(indexName);
                dataSyncHandler.addAlias(indexName, tempIndexName);
            } else {
                // 当前索引名为别名
                dataSyncHandler.aliasIndex(tempIndexName, indexTrueName, indexEnum);
                // 删除旧索引
                dataSyncHandler.deleteIndex(indexTrueName);
            }
        } catch (IOException e) {
            String err = StrUtil.format("[{}]:shiftIndex切换索引异常，临时索引{}，当前索引{}，ex:{}", indexName, tempIndexName,
                indexName, ExceptionUtil.getMessage(e));
            log.error(err);
            errMsg.append(err);
            return false;
        }
        return true;
    }
    private boolean checkDataCount(IndexEnum indexEnum, String indexName, String tempIndexName, Long dbSize,
        StringBuilder msg) {
        String pattern = indexEnum.getDeviationPattern()[0];
        String countDiffStr = indexEnum.getDeviationPattern()[1];
        // 按照百分比误差比较同比数据误差
        if (pattern.equals("1")) {
            float differ = dataSyncHandler.checkDataCount(dbSize, tempIndexName);
            // 数据库数据量比ES少没有问题，可能是同步过程中有增量被索引了
            // ES比数据库数据量缺少量数据可能是 同步过程中有减量被索引了
            // 但是如果ES比数据库少很多就有问题
            float countDiffFlag = Float.parseFloat(countDiffStr);
            if (differ > countDiffFlag) {
                log.info("[{}]：等待[{}]ms,再次检测", indexName, TIN_WAIT_MILLIS);
                ThreadUtil.sleep(TIN_WAIT_MILLIS);
                differ = dataSyncHandler.checkDataCount(dbSize, tempIndexName);
                if (differ > countDiffFlag) {
                    msg.append("数据量差异较大，不切换索引。[(db-es)/db]误差百分比为：" + differ);
                    return false;
                }
            }
        }
        // 按照条数误差比较同步数据误差
        if (pattern.equals("2")) {
            float differ = dataSyncHandler.checkDataCount(dbSize, tempIndexName);
            long countDiffFlag = Long.parseLong(countDiffStr);
            if (differ > countDiffFlag) {
                log.info("[{}]：等待[{}]ms,再次检测", indexName, TIN_WAIT_MILLIS);
                ThreadUtil.sleep(TIN_WAIT_MILLIS);
                differ = dataSyncHandler.checkDataCount(tempIndexName, indexEnum);
                if (differ > countDiffFlag) {
                    msg.append("数据量差异较大，不切换索引。[db-es]误差条数为：" + differ);
                    return false;
                }
            }
        }
        return true;
    }

    public boolean delRedundencyData(IndexEnum indexEnum, String operater) {
        Long timeStart = System.currentTimeMillis();
        String indexName = dataSyncHandler.getIndexName(indexEnum.getClazz());
        TaskRecord taskRecord = addRecord(null, TaskRecord.TASK_EVENT_DEL_INDEX_REDUANT_DATA, indexName, operater);
        if (indexEnum.isOnlyRebuild()) {
            recordError(indexEnum, indexName, taskRecord, "该索引仅支持重建同步", false, true);
            return false;
        }
        if (INDEX_DOING_SET.contains(indexEnum)) {
            recordError(indexEnum, indexName, taskRecord, "正在进行同步,请确认", false, true);
            return false;
        }
        INDEX_DOING_SET.add(indexEnum);
        log.info("同步开始，删除冗余ES[{}] 数据", indexName);
        // 更新记录状态
        updateRecord(taskRecord, TaskRecord.RECORD_STATUS_DOING, null);
        // 1.检查索引是否存在
        boolean isExists = false;
        try {
            isExists = dataSyncHandler.checkIndex(indexName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            recordError(indexEnum, indexName, taskRecord, "checkIndex检查索引失败,ex:" + ExceptionUtil.getMessage(e), false,
                true);
            return false;
        }
        if (!isExists) {
            recordError(indexEnum, indexName, taskRecord, "索引不存在", false, true);
            return false;
        }
        // 2.获取索引名（有真名用真名，没有则用别名）
        String indexTrueName = null;
        try {
            indexTrueName = dataSyncHandler.getIndexNameByAlias(indexEnum);
        } catch (Exception e) {
            log.warn("获取索引名失败" + e.getMessage(), e);
        }
        if (StringUtils.isEmpty(indexTrueName)) {
            indexTrueName = indexName;
        }
        long currentDocCount = dataSyncHandler.getCurrentDocCount(indexName);
        StringBuilder msg = new StringBuilder();
        String info = StrUtil.format("[{}]:索引当前数据量为[{}]", indexName, currentDocCount);
        log.info(info);
        msg.append(info).append("/n");
        // 3.sync data
        boolean isSyncSuccess = dataSyncHandler.delByDiffer(indexEnum, indexTrueName, msg);
        // 更新记录状态
        updateRecord(taskRecord, null, msg.toString());
        if (!isSyncSuccess) {
            recordError(indexEnum, indexName, taskRecord, "删除ES冗余数据失败", false, true);
            return false;
        }
        log.info("[{}]:删除冗余数据成功", indexName);
        Long timeEnd = System.currentTimeMillis();
        recordSuccess(indexEnum, indexName, taskRecord,
            StrUtil.format("删除冗余数据成功,用时[{}]s", (timeEnd - timeStart) / 1000));
        return true;
    }
    
    public boolean delIndexByTime(IndexEnum indexEnum, String lastUpdateTime, String operater) {
        Long timeStart = System.currentTimeMillis();
        String indexName = dataSyncHandler.getIndexName(indexEnum.getClazz());
        TaskRecord taskRecord = addRecord(null, TaskRecord.TASK_EVENT_DEL_INDEX_DATA_BY_TIME, indexName, operater);
        if (indexEnum.isOnlyRebuild()) {
            recordError(indexEnum, indexName, taskRecord, "该索引仅支持重建同步", false, true);
            return false;
        }
        if (INDEX_DOING_SET.contains(indexEnum)) {
            recordError(indexEnum, indexName, taskRecord, "正在进行同步,请确认", false, true);
            return false;
        }
        INDEX_DOING_SET.add(indexEnum);
        log.info("按时间，删除指定 [{}] 数据", indexName);
        // 更新记录状态
        updateRecord(taskRecord, TaskRecord.RECORD_STATUS_DOING, null);
        // 1.检查索引是否存在
        boolean isExists = false;
        try {
            isExists = dataSyncHandler.checkIndex(indexName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            recordError(indexEnum, indexName, taskRecord, "checkIndex检查索引失败,ex:" + ExceptionUtil.getMessage(e), false,
                true);
            return false;
        }
        if (!isExists) {
            recordError(indexEnum, indexName, taskRecord, "索引不存在", false, true);
            return false;
        }
        // 2.获取索引名（有真名用真名，没有则用别名）
        String indexTrueName = null;
        try {
            indexTrueName = dataSyncHandler.getIndexNameByAlias(indexEnum);
        } catch (Exception e) {
            log.warn("获取索引名失败" + e.getMessage(), e);
        }
        if (StringUtils.isEmpty(indexTrueName)) {
            indexTrueName = indexName;
        }
        long currentDocCount = dataSyncHandler.getCurrentDocCount(indexName);
        StringBuilder msg = new StringBuilder();
        String info = StrUtil.format("[{}]:索引当前数据量为[{}],数据从[{}]时间删除指定数据", indexName, currentDocCount, lastUpdateTime);
        log.info(info);
        msg.append(info).append("/n");
        // 3.sync data
        boolean isSyncSuccess = dataSyncHandler.delByTime(indexEnum, lastUpdateTime, indexTrueName, msg);
        // 更新记录状态
        updateRecord(taskRecord, null, msg.toString());
        if (!isSyncSuccess) {
            recordError(indexEnum, indexName, taskRecord, "按时间删除数据同步失败", false, true);
            return false;
        }
        log.info("[{}]:删除指定数据同步成功，删除时间为[{}]", indexName, lastUpdateTime);
        Long timeEnd = System.currentTimeMillis();
        recordSuccess(indexEnum, indexName, taskRecord,
            StrUtil.format("删除指定数据同步成功，删除时间为[{}],用时[{}]s", lastUpdateTime, (timeEnd - timeStart) / 1000));
        return true;
    }
    
    public boolean syncIndexByTime(IndexEnum indexEnum, String lastUpdateTime, String operater) {
        Long timeStart = System.currentTimeMillis();
        String indexName = dataSyncHandler.getIndexName(indexEnum.getClazz());
        TaskRecord taskRecord = addRecord(null, TaskRecord.TASK_EVENT_SYNC_INDEX_BY_TIME, indexName, operater);
        if (indexEnum.isOnlyRebuild()) {
            recordError(indexEnum, indexName, taskRecord, "该索引仅支持重建同步", false, true);
            return false;
        }
        if (INDEX_DOING_SET.contains(indexEnum)) {
            recordError(indexEnum, indexName, taskRecord, "正在进行同步,请确认", false, true);
            return false;
        }
        INDEX_DOING_SET.add(indexEnum);
        log.info("按时间，开始同步增量 [{}] 数据", indexName);
        // 更新记录状态
        updateRecord(taskRecord, TaskRecord.RECORD_STATUS_DOING, null);
        // 1.检查索引是否存在
        boolean isExists = false;
        try {
            isExists = dataSyncHandler.checkIndex(indexName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            recordError(indexEnum, indexName, taskRecord, "checkIndex检查索引失败,ex:" + ExceptionUtil.getMessage(e), false,
                true);
            return false;
        }
        if (!isExists) {
            recordError(indexEnum, indexName, taskRecord, "索引不存在", false, true);
            return false;
        }
        // 2.获取索引名（有真名用真名，没有则用别名）
        String indexTrueName = null;
        try {
            indexTrueName = dataSyncHandler.getIndexNameByAlias(indexEnum);
        } catch (Exception e) {
            log.warn("获取索引名失败" + e.getMessage(), e);
        }
        if (StringUtils.isEmpty(indexTrueName)) {
            indexTrueName = indexName;
        }
        long currentDocCount = dataSyncHandler.getCurrentDocCount(indexName);
        StringBuilder msg = new StringBuilder();
        String info = StrUtil.format("[{}]:索引当前数据量为[{}],数据从[{}]时间更新或新增", indexName, currentDocCount, lastUpdateTime);
        log.info(info);
        msg.append(info).append("/n");
        // 3.sync data
        boolean isSyncSuccess = dataSyncHandler.syncByTime(indexEnum, lastUpdateTime, indexTrueName, msg);
        // 更新记录状态
        updateRecord(taskRecord, null, msg.toString());
        if (!isSyncSuccess) {
            recordError(indexEnum, indexName, taskRecord, "数据量同步失败", false, true);
            return false;
        }
        log.info("[{}]:增量数据同步成功，更新日期为[{}]", indexName, lastUpdateTime);
        Long timeEnd = System.currentTimeMillis();
        recordSuccess(indexEnum, indexName, taskRecord,
            StrUtil.format("数据增量同步成功,用时[{}]s", (timeEnd - timeStart) / 1000));
        return true;
    }

    /**
     * 根据主键ID ，增量同步数据
     * @param indexEnum
     * @param ids
     * @param operater
     * @return
     */
    public boolean syncIndexById(IndexEnum indexEnum, List<String> ids, String operater) {
        Long timeStart = System.currentTimeMillis();
        String indexName = dataSyncHandler.getIndexName(indexEnum.getClazz());
        TaskRecord taskRecord = addRecord(JSON.toJSONString(ids), TaskRecord.TASK_EVENT_SYNC_INDEX_BY_ID, indexName, operater);
        if (indexEnum.isOnlyRebuild()) {
            recordError(indexEnum, indexName, taskRecord, "该索引仅支持重建同步", false, true);
            return false;
        }
        if (INDEX_DOING_SET.contains(indexEnum)) {
            recordError(indexEnum, indexName, taskRecord, "正在进行同步,请确认", false, true);
            return false;
        }
        INDEX_DOING_SET.add(indexEnum);
        log.info("按docID,开始同步增量 [{}] 数据", indexName);
        // 更新记录状态
        updateRecord(taskRecord, TaskRecord.RECORD_STATUS_DOING, null);
        // 1.检查索引是否存在
        boolean isExists = false;
        try {
            isExists = dataSyncHandler.checkIndex(indexName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            recordError(indexEnum, indexName, taskRecord, "checkIndex检查索引失败,ex:" + ExceptionUtil.getMessage(e), false,
                true);
            return false;
        }
        if (!isExists) {
            recordError(indexEnum, indexName, taskRecord, "索引不存在", false, true);
            return false;
        }
        // 2.获取索引名（有真名用真名，没有则用别名）
        String indexTrueName = null;
        try {
            indexTrueName = dataSyncHandler.getIndexNameByAlias(indexEnum);
        } catch (Exception e) {
            log.warn("获取索引名失败" + e.getMessage(), e);
        }
        if (StringUtils.isEmpty(indexTrueName)) {
            indexTrueName = indexName;
        }
        long currentDocCount = dataSyncHandler.getCurrentDocCount(indexName);
        StringBuilder msg = new StringBuilder();
        String info = StrUtil.format("[{}]:索引当前数据量为[{}],按docID同步新增数据", indexName, currentDocCount);
        log.info(info);
        msg.append(info).append("/n");
        // 3.sync data
        boolean isSyncSuccess = dataSyncHandler.syncIndexById(indexEnum, ids, indexTrueName, msg);
        // 更新记录状态
        updateRecord(taskRecord, null, msg.toString());
        if (!isSyncSuccess) {
            recordError(indexEnum, indexName, taskRecord, "指定id同步失败", false, true);
            return false;
        }
        log.info("[{}]:按docId增量数据同步成功", indexName);
        Long timeEnd = System.currentTimeMillis();
        recordSuccess(indexEnum, indexName, taskRecord,
            StrUtil.format("按docId增量数据同步成功,用时[{}]s", (timeEnd - timeStart) / 1000));
        return true;
    }

    /**
     * 根据主键ID，删除ES数据
     * @param indexEnum
     * @param ids
     * @param operater
     * @return
     */
    public boolean delInexById(IndexEnum indexEnum, List<String> ids, String operater) {
        Long timeStart = System.currentTimeMillis();
        String indexName = dataSyncHandler.getIndexName(indexEnum.getClazz());
        TaskRecord taskRecord =
            addRecord(JSON.toJSONString(ids), TaskRecord.TASK_EVENT_DEL_INDEX_DATA_BY_ID, indexName, operater);
        if (indexEnum.isOnlyRebuild()) {
            recordError(indexEnum, indexName, taskRecord, "该索引仅支持重建同步", false, true);
            return false;
        }
        if (INDEX_DOING_SET.contains(indexEnum)) {
            recordError(indexEnum, indexName, taskRecord, "正在进行同步,请确认", false, true);
            return false;
        }
        INDEX_DOING_SET.add(indexEnum);
        log.info("开始指定docID删除 [{}] 数据", indexName);
        // 更新记录状态
        updateRecord(taskRecord, TaskRecord.RECORD_STATUS_DOING, null);
        // 1.检查索引是否存在
        boolean isExists = false;
        try {
            isExists = dataSyncHandler.checkIndex(indexName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            recordError(indexEnum, indexName, taskRecord, "checkIndex检查索引失败,ex:" + ExceptionUtil.getMessage(e), false,
                true);
            return false;
        }
        if (!isExists) {
            recordError(indexEnum, indexName, taskRecord, "索引不存在", false, true);
            return false;
        }
        // 2.获取索引名（有真名用真名，没有则用别名）
        String indexTrueName = null;
        try {
            indexTrueName = dataSyncHandler.getIndexNameByAlias(indexEnum);
        } catch (Exception e) {
            log.warn("获取索引名失败" + e.getMessage(), e);
        }
        if (StringUtils.isEmpty(indexTrueName)) {
            indexTrueName = indexName;
        }
        long currentDocCount = dataSyncHandler.getCurrentDocCount(indexName);
        StringBuilder msg = new StringBuilder();
        String info = StrUtil.format("[{}]:索引当前数据量为[{}]", indexName, currentDocCount);
        log.info(info);
        msg.append(info).append("/n");
        // 3.sync data
        boolean isSyncSuccess = dataSyncHandler.delIndexById(indexEnum, ids, indexTrueName);
        // 更新记录状态
        updateRecord(taskRecord, null, msg.toString());
        if (!isSyncSuccess) {
            recordError(indexEnum, indexName, taskRecord, "删除指定ids失败", false, true);
            return false;
        }
        log.info("[{}]:删除指定id成功", indexName);
        Long timeEnd = System.currentTimeMillis();
        recordSuccess(indexEnum, indexName, taskRecord,
            StrUtil.format("删除指定id成功,用时[{}]s", (timeEnd - timeStart) / 1000));
        return true;
    }

    public Map<String, List<String>> checkIndexDifferIds(IndexEnum indexEnum) {
        return dataSyncHandler.checkDifferIdsByIndexName(indexEnum);
    }
    
    public boolean deleteIndex(String indexName) {
        return dataSyncHandler.deleteIndex(indexName);
    }
    
    private void recordError(IndexEnum indexEnum, String indexName, TaskRecord taskRecord, String errorMsg,
        boolean isStop, boolean isReleaseExclusive) {
        String errorInfo = StrUtil.format("ES 索引同步异常--[{}] [{}]", indexName, errorMsg);
        log.error(errorInfo);
        if (isReleaseExclusive) {
            INDEX_DOING_SET.remove(indexEnum);
        }
        taskRecord.setStatus(TaskRecord.RECORD_STATUS_FAILED);
        StringBuilder msg = new StringBuilder();
        if (StringUtils.isNotEmpty(taskRecord.getRemark())) {
            msg.append(taskRecord.getRemark()).append("/n").append(errorInfo);
        } else {
            msg.append(errorInfo);
        }
        taskRecord.setRemark(msg.toString().length() >= 8192 ? "" : msg.toString());
        taskRecord.setUpdateTime(DateUtil.format(DateTime.now(), "yyyy-MM-dd HH:mm:ss"));
        taskRecordService.saveOrUpdate(taskRecord);
        if (isStop) {
            throw new IndexSyncException(errorInfo);
        }
    }


    private void recordSuccess(IndexEnum indexEnum, String indexName, TaskRecord taskRecord, String successMsg) {
        log.info("索引[{}]: [{}]", indexName, successMsg);
        INDEX_DOING_SET.remove(indexEnum);

        taskRecord.setStatus(TaskRecord.RECORD_STATUS_SUCCESS);
        StringBuilder msg = new StringBuilder();
        if (StringUtils.isNotEmpty(taskRecord.getRemark())) {
            msg.append(taskRecord.getRemark()).append("/n").append(successMsg);
        } else {
            msg.append(successMsg);
        }
        taskRecord.setRemark(msg.toString().length() >= 8192 ? "" : msg.toString());
        taskRecord.setUpdateTime(DateUtil.format(DateTime.now(), "yyyy-MM-dd HH:mm:ss"));
        taskRecordService.saveOrUpdate(taskRecord);
    }
}