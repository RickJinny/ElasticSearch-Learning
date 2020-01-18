package com.rickjin.es.demo;


import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

public class StudentDemo {

    public static void main(String[] args) throws Exception {
        // 先构建
        Settings settings = Settings.builder()
                                    .put("cluster.name", "elasticsearch")
                                    .build();
        // 创建 TransportClient 对象
        TransportClient transportClient = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        // 创建一个学生信息
//        createStudent(transportClient);
        // 获取学生信息
//        getStudent(transportClient);
        // 更新学生信息
//        updateStudent(transportClient);
        // 删除员工信息
        deleteStudent(transportClient);
        transportClient.close();
    }

    /**
     * 创建学生信息（创建一个 document）
     * @param client
     */
    private static void createStudent(TransportClient client) throws Exception {
        IndexResponse response = client.prepareIndex("school", "student", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "Tom")
                        .field("age", 29)
                        .field("sex", "男")
                        .endObject())
                .get();
        System.out.println(response.getResult());
    }

    /**
     * 获取员工信息
     * @param client
     * @throws Exception
     */
    private static void getStudent(TransportClient client) throws Exception {
        GetResponse response = client.prepareGet("school", "student", "1").get();
        System.out.println(response.getSourceAsString());
    }

    /**
     * 修改员工信息
     * @param client
     * @throws Exception
     */
    private static void updateStudent(TransportClient client) throws Exception {
        UpdateResponse response = client.prepareUpdate("school", "student", "1")
                                                    .setDoc(XContentFactory.jsonBuilder()
                                                    .startObject()
                                                    .field("age", 34)
                                                    .endObject())
                                                    .get();
        System.out.println(response.getResult());
    }

    /**
     * 删除员工信息
     * @param client
     * @throws Exception
     */
    private static void deleteStudent(TransportClient client) throws Exception {
        DeleteResponse response = client.prepareDelete("school", "student", "1").get();
        System.out.println(response.getResult());
    }
}
