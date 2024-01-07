package com.github.cody_wolf.utils.spark.rdd;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.*;

@Slf4j
class RddJoinSketchBalancerTest {

    @Test
    @SneakyThrows
    void testJoinCorrectness() {
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Random rand = new Random();

        List<Integer> leftData = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < i; j++) {
                leftData.add(i);
                leftData.add(-i);
                leftData.add(rand.nextInt(100) + 1000);
            }
        }
        leftData.add(100);

        List<Integer> rightData = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            for (int j = 0; j < i; j++) {
                rightData.add(i);
                leftData.add(rand.nextInt(100) + 1000);
            }
        }

        JavaPairRDD<String, String> leftRDD = sc.parallelize(leftData)
                .mapToPair(key -> Tuple2.apply(key.toString(), "test"))
                .setName("left");
        JavaPairRDD<String, String> rightRDD = sc.parallelize(rightData)
                .mapToPair(key -> Tuple2.apply(key.toString(), "test"))
                .setName("right");

        RddJoinSketchBalancer balancer = RddJoinSketchBalancer.builder()
                .enableCache(true)
                .build();

        balancer.sketch(leftRDD);
        balancer.sketch(rightRDD);
        List<String> balanceKeys = balancer.leftOuterJoin(leftRDD, rightRDD)
                .map(Tuple2::_1)
                .collect();

        Map<String, Integer> balanceCounter = new HashMap<>();
        balanceKeys.forEach(key -> balanceCounter.merge(key, 1, Integer::sum));

        List<String> keys = leftRDD.leftOuterJoin(rightRDD)
                .map(Tuple2::_1)
                .collect();
        Map<String, Integer> counter = new HashMap<>();
        keys.forEach(key -> counter.merge(key, 1, Integer::sum));

        Assertions.assertEquals(counter, balanceCounter);

        spark.stop();
    }

    @Test
    void leftJoin() {
    }
}