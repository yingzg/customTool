package com.gzy.custom.db2es.sync.service;

import com.gzy.custom.db2es.sync.mapper.TaskRecordMapper;
import com.gzy.custom.db2es.sync.model.TaskRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TaskRecordService {
    
    @Autowired
    private TaskRecordMapper taskRecordMapper;
    
    public void saveOrUpdate(TaskRecord taskRecord) {
        TaskRecord recordDb =
            taskRecordMapper.findRecordByRelateId(taskRecord.getRelateId(), taskRecord.getEventType());
        if (recordDb == null) {
            save(taskRecord);
        } else {
            taskRecordMapper.updateRecord(taskRecord);
        }
    }

    public void save(TaskRecord record) {
        taskRecordMapper.saveRecord(record);
    }
    
}
