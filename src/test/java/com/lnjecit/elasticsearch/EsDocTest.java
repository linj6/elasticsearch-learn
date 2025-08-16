package com.lnjecit.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lnjecit.elasticsearch.domain.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class EsDocTest {
    private RestHighLevelClient client;
    public ObjectMapper objectMapper;
    private final static String USER_INDEX = "user_test";

    @Before
    public void setUp() {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        System.out.println("connect es success");

        objectMapper = new ObjectMapper();
    }

    @After
    public void tearDown() throws IOException {
        if (client != null) {
            client.close();
            System.out.println("close es success");
        }
    }

    @Test
    public void testCreateDoc() throws Exception {
        IndexRequest request = new IndexRequest(USER_INDEX);
        Long id = 1L;
        request.id(id.toString());

        User user = new User();
        user.setId(id);
        user.setName("王林");
        user.setAge(28);
        user.setSex("男");

        String json = objectMapper.writeValueAsString(user);
        request.source(json, XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println("创建文档:" + objectMapper.writeValueAsString(response));
    }

    @Test
    public void testGetDoc() throws IOException {
        GetRequest request = new GetRequest(USER_INDEX, "1");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println("查询文档结果:" + response.getSourceAsString());
    }

    @Test
    public void testUpdateDoc() throws IOException {
        Long id = 1L;
        User user = new User();
        user.setId(id);
        user.setName("王林");
        user.setAge(32);
        user.setSex("男");

        UpdateRequest request = new UpdateRequest(USER_INDEX, id.toString());
        request.doc(objectMapper.writeValueAsString(user), XContentType.JSON);

        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println("更新文档结果:" + response);
    }

    @Test
    public void testDeleteDoc() throws IOException {
        DeleteRequest request = new DeleteRequest(USER_INDEX, "1");
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println("删除文档结果:" + response);
    }

}
