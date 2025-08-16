package com.lnjecit.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class EsIndexTest {

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
    public void testCreateIndex() throws Exception {
        boolean exists = client.indices().exists(new GetIndexRequest(USER_INDEX), RequestOptions.DEFAULT);
        if (exists) {
            // 删除索引
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(USER_INDEX);
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            System.out.println("索引已存在，先删除索引。删除索引:" + objectMapper.writeValueAsString(deleteIndexResponse.isAcknowledged()));
        }
        // 创建索引
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(USER_INDEX);
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println("创建索引:" + objectMapper.writeValueAsString(createIndexResponse.isAcknowledged()));

        // 查询索引
        GetIndexRequest getIndexRequest = new GetIndexRequest(USER_INDEX);
        GetIndexResponse getIndexResponse = client.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println("查询索引,aliases:" + objectMapper.writeValueAsString(getIndexResponse.getAliases()));
        System.out.println("查询索引,mappings:" + objectMapper.writeValueAsString(getIndexResponse.getMappings()));
        System.out.println("查询索引,settings:" + getIndexResponse.getSettings());

        // 删除索引
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(USER_INDEX);
        AcknowledgedResponse deleteIndexResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println("删除索引:" + objectMapper.writeValueAsString(deleteIndexResponse.isAcknowledged()));
    }


}
