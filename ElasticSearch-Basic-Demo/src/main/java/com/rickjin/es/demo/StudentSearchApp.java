package com.rickjin.es.demo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * 学生搜索应用程序
 */
public class StudentSearchApp {

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                                    .put("cluster.name", "elasticsearch")
                                    .build();
        TransportClient transportClient = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        // 准备数据
//        prepareData(transportClient);
        // 搜索数据
        executeSearch(transportClient);
        transportClient.close();
    }

    /**
     * 执行搜索操作
     * @param client
     */
    private static void executeSearch(TransportClient client) {
        SearchResponse searchResponse = client.prepareSearch("school")
                                                .setTypes("student")
                                                .setQuery(QueryBuilders.matchQuery("sex", "女"))
                                                .setPostFilter(QueryBuilders.rangeQuery("age").from(20).to(25))
                                                .setFrom(0)
                                                .setSize(1)
                                                .get();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsString());
        }
    }

    /**
     * 准备数据
     * @param client
     */
    private static void prepareData(TransportClient client) throws Exception {
        client.prepareIndex("school", "student", "1")
              .setSource(XContentFactory.jsonBuilder()
              .startObject()
              .field("name", "小红")
              .field("age", 18)
              .field("sex", "女")
              .endObject())
              .get();

        client.prepareIndex("school", "student", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "小明")
                        .field("age", 22)
                        .field("sex", "女")
                        .endObject())
                .get();

        client.prepareIndex("school", "student", "3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "小华")
                        .field("age", 24)
                        .field("sex", "男")
                        .endObject())
                .get();

        client.prepareIndex("school", "student", "4")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "小杨")
                        .field("age", 26)
                        .field("sex", "女")
                        .endObject())
                .get();
    }
}
