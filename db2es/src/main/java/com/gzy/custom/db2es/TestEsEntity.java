package com.gzy.custom.db2es;


import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import org.springframework.data.elasticsearch.annotations.Setting;

import lombok.Data;

@Data
@Document(indexName = "es_test_entity",type ="es_test_entity" )
@Setting(settingPath = "templates/es_test_entity.json")
public class TestEsEntity {

    @Id
    private String Id;

    @Field(type = FieldType.Keyword)
    private String field1;

    @Field(type = FieldType.Double)
    private double field2;

    @Field(type = FieldType.Keyword)
    private String field3;


}
