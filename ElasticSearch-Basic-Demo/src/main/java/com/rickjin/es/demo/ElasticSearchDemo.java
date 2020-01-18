package com.rickjin.es.demo;


import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ElasticSearchDemo {

    public static void main(String[] args) throws UnknownHostException {
        // 先构建
        Settings settings = Settings.builder()
                                    .put("cluster.name", "es")
                                    .build();
        // 创建 TransportClient 对象
        TransportClient transportClient = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        transportClient.close();
    }
}
