package com.pgt.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;

import java.io.IOException;

public class EsTest_Index_Delete {
    public static void main(String[] args) throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost",9200,"http")));
        //创建索引 index
        DeleteIndexRequest request = new DeleteIndexRequest("user");
        AcknowledgedResponse delete = esClient.indices().delete(request, RequestOptions.DEFAULT);
        //响应状态

        System.out.println(delete);
        //关闭ES客户端连接
        esClient.close();
    }
}
