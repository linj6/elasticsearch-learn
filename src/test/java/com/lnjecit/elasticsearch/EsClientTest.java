package com.lnjecit.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;

public class EsClientTest {

    @Test
    public void testConnectEs() throws IOException {
        // 创建es客户端
        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(clientBuilder);
        // 关闭es客户端
        client.close();
    }

}
