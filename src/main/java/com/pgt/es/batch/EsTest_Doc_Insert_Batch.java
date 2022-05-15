package com.pgt.es.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pgt.es.bean.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class EsTest_Doc_Insert_Batch {
    public static void main(String[] args) throws IOException {
        //创建ES客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost",9200,"http")));
        //批量插入数据
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest().index("user").id("1001").source(XContentType.JSON,"name","zhangsan","age",30,"sex","男"));
        request.add(new IndexRequest().index("user").id("1002").source(XContentType.JSON,"name","wanwu","age",30,"sex","男"));
        request.add(new IndexRequest().index("user").id("1004").source(XContentType.JSON,"name","lishi","age",30,"sex","男"));
        request.add(new IndexRequest().index("user").id("1005").source(XContentType.JSON,"name","lishi","age",30,"sex","男"));
        request.add(new IndexRequest().index("user").id("1006").source(XContentType.JSON,"name","lishi","age",30,"sex","男"));
        request.add(new IndexRequest().index("user").id("1007").source(XContentType.JSON,"name","lishi","age",30,"sex","男"));
        request.add(new IndexRequest().index("user").id("1008").source(XContentType.JSON,"name","lishi","age",30,"sex","男"));


        BulkResponse bulk = esClient.bulk(request,RequestOptions.DEFAULT);
        System.out.println(bulk.getTook());
        System.out.println(bulk.getItems());
        //关闭ES客户端连接
        esClient.close();
    }
}
