package com.gzy.custom.db2es.sync.service;

import com.gzy.custom.db2es.TestEsEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum IndexEnum {


    DEFAULT_INDEX(TestEsEntity.class, "", 1000, new String[] {"1", "0.001"}, false, "id", 30, 1);

    /**
     * 用于反射获取mapping
     */
    Class<?> clazz;

    String processorName;

    Integer pageSize;

    String[] deviationPattern;

    boolean isOnlyRebuild;

    String esIdField;

    Integer refreshInterval;

    Integer numberOfReplicas;




}
