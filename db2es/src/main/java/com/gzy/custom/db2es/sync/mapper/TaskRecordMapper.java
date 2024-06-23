package com.gzy.custom.db2es.sync.mapper;

import com.gzy.custom.db2es.sync.model.TaskRecord;
import org.mapstruct.Mapper;
import org.springframework.data.repository.query.Param;

@Mapper
public interface TaskRecordMapper {

    void saveRecord(@Param("record") TaskRecord record);

    void updateRecord(@Param("record") TaskRecord record);

    TaskRecord findRecordByRelateId(@Param("relateId") String relateId, @Param("eventType") String ecentType);

    TaskRecord findRecordById(@Param("Id") String Id);

}
