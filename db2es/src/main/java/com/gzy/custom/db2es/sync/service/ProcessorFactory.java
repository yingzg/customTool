package com.gzy.custom.db2es.sync.service;

import cn.hutool.core.collection.CollectionUtil;
import com.gzy.custom.db2es.Util.SpringContextUtil;
import com.gzy.custom.db2es.sync.service.processor.SyncProcessor;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ProcessorFactory {
    
    private List<SyncProcessor> processorList;

    @PostConstruct
    public void init() {
        Map<String, SyncProcessor> map = SpringContextUtil.getApplicationContext().getBeansOfType(SyncProcessor.class);
        if (CollectionUtil.isNotEmpty(map)) {
            processorList = new ArrayList<>(map.values());
        }
    }
    
    public SyncProcessor getProcessor(IndexEnum indexEnum) {
        if (CollectionUtil.isEmpty(processorList)) {
            return null;
        }
        return processorList.stream().filter(e -> e.getName().equals(indexEnum.getProcessorName())).findFirst()
            .orElse(null);
    }


}
