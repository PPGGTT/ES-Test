package com.pgt.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.engine.Engine;

import java.io.IOException;

public class EsTest_Doc_Delete {
    public static void main(String[] args) throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost",9200,"http")));
        //删除数据
        DeleteRequest request = new DeleteRequest();
        request.index("user").id("1001");
        DeleteResponse delete = esClient.delete(request, RequestOptions.DEFAULT);


        System.out.println(delete.toString());
        //关闭ES客户端连接
        esClient.close();
    }
}
