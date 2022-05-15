package com.pgt.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pgt.es.bean.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;


import java.io.IOException;

public class EsTest_Doc_Insert {
    public static void main(String[] args) throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost",9200,"http")));
        //插入数据
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index("user").id("1001");

        User user = new User("张三", "男", 30);
        //向ES插入数据，必须将数据转为Json格式
        ObjectMapper objectMapper = new ObjectMapper();
        String productJson = objectMapper.writeValueAsString(user);
        indexRequest.source(productJson, XContentType.JSON);

        IndexResponse index = esClient.index(indexRequest, RequestOptions.DEFAULT);

        System.out.println(index.getResult());

        //关闭ES客户端连接
        esClient.close();
    }
}
