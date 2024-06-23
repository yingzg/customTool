package com.gzy.custom.db2es.sync.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TaskRecord {

    public static final String RECORD_STATUS_CREATE = "CREATE";
    public static final String RECORD_STATUS_DOING = "DOING";
    public static final String RECORD_STATUS_SUCCESS = "SUCCESS";
    public static final String RECORD_STATUS_FAILED = "FAILED";

    public static final String TASK_EVENT_REBUILD_INDEX = "REBUILD_INDEX";
    public static final String TASK_EVENT_SYNC_INDEX_BY_TIME = "SYNC_INDEX_BY_TIME";
    public static final String TASK_EVENT_SYNC_INDEX_BY_ID = "SYNC_INDEX_BY_ID";
    public static final String TASK_EVENT_DEL_INDEX_DATA_BY_TIME = "DEL_INDEX_DATA_BY_TIME";
    public static final String TASK_EVENT_DEL_INDEX_REDUANT_DATA = "DEL_INDEX_REDUANT_DATA";
    public static final String TASK_EVENT_DEL_INDEX_DATA_BY_ID = "TASK_EVENT_DEL_INDEX_DATA_BY_ID";

    private long id;
    private String relateId;
    private String indexName;
    private String operater;
    private String createTime;
    private String updateTime;
    private String eventType;
    private String status;
    private String inputJson;
    private String remark;
    private String deviation;
    
}
