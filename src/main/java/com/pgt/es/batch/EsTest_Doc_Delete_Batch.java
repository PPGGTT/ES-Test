package com.pgt.es.batch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class EsTest_Doc_Delete_Batch {
    public static void main(String[] args) throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost",9200,"http")));
        //批量删除数据
        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest("user").id("1001"));
        request.add(new DeleteRequest("user").id("1002"));
        request.add(new DeleteRequest("user").id("1003"));

        BulkResponse bulk = esClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println(bulk.getTook());
        System.out.println(bulk.getItems());


        //关闭ES客户端连接
        esClient.close();
    }
}
