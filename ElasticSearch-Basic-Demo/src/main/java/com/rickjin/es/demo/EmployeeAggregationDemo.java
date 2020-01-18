package com.rickjin.es.demo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

/**
 * 员工聚合分析
 */
public class EmployeeAggregationDemo {

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        TransportClient transportClient = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        searchEmployee(transportClient);

        transportClient.close();
    }

    private static void searchEmployee(TransportClient client) throws Exception {
        SearchResponse searchResponse = client.prepareSearch("company")
                .addAggregation(AggregationBuilders.terms("group_by_country").field("country")
                        .subAggregation(AggregationBuilders
                                .dateHistogram("group_by_join_date")
                                .field("join_date")
                                .dateHistogramInterval(DateHistogramInterval.YEAR)
                                .subAggregation(AggregationBuilders.avg("avg_salary").field("salary"))))
                .execute().actionGet();

        Map<String, Aggregation> aggregationMap = searchResponse.getAggregations().asMap();
        StringTerms groupByCountry = (StringTerms) aggregationMap.get("group_by_country");
        Iterator<Terms.Bucket> iterator = groupByCountry.getBuckets().iterator();
        while (iterator.hasNext()) {
            Terms.Bucket bucket = iterator.next();
            System.out.println(bucket.getKey() + ":" + bucket.getDocCount());
            Histogram groupByJoinDate = (Histogram) bucket.getAggregations().asMap().get("group_by_join_date");
            Iterator<? extends Histogram.Bucket> groupByJoinDateBucketIterator = groupByJoinDate.getBuckets().iterator();
            while (groupByJoinDateBucketIterator.hasNext()) {
                Histogram.Bucket histogramBucket = groupByJoinDateBucketIterator.next();
                System.out.println(histogramBucket.getKey() + ":" + histogramBucket.getDocCount());
                Avg avg = (Avg) histogramBucket.getAggregations().asMap().get("avg_salary");
                System.out.println(avg.getValue());
            }
        }
    }
}
