package com.github.cody_wolf.utils.spark.rdd;

import com.google.common.collect.Iterators;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
class RddJoinSketchBalancerTest {

    static SparkSession spark;
    static JavaSparkContext sc;
    static Random rand = new Random(System.currentTimeMillis());
    static RddJoinSketchBalancer defaultBalancer;

    @BeforeAll
    public static void init() {
        spark = SparkSession.builder().master("local[100]").getOrCreate();
        sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        defaultBalancer = RddJoinSketchBalancer.builder()
                .enableCache(true)
                .build();
    }

    @AfterAll
    public static void close() {
        spark.stop();
    }

    private static double calculateGiniCoefficient(List<Integer> partitionSizes) {
        int n = partitionSizes.size();
        double sumOfAbsoluteDifferences = 0.0;
        double totalSum = 0.0;

        for (int size : partitionSizes) {
            totalSum += size;
        }

        double mean = totalSum / n;

        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                sumOfAbsoluteDifferences += Math.abs(partitionSizes.get(i) - partitionSizes.get(j));
            }
        }

        return sumOfAbsoluteDifferences / (2.0 * n * n * mean);
    }

    private boolean testJoinCorrectness(List<String> leftData, List<String> rightData, RddJoinSketchBalancer balancer) throws InterruptedException {
        JavaPairRDD<String, String> leftRDD = sc.parallelize(leftData)
                .mapToPair(key -> Tuple2.apply(key, "test"))
                .setName("left");
        JavaPairRDD<String, String> rightRDD = sc.parallelize(rightData)
                .mapToPair(key -> Tuple2.apply(key, "test"))
                .setName("right");


        long balanceStart = System.currentTimeMillis();

        balancer.sketch(leftRDD);
        balancer.sketch(rightRDD);
        List<String> balanceKeys = balancer.leftOuterJoin(leftRDD, rightRDD)
                .map(Tuple2::_1)
                .collect();
        long balanceCost = System.currentTimeMillis() - balanceStart;

        Map<String, Integer> balanceCounter = new HashMap<>();
        balanceKeys.forEach(key -> balanceCounter.merge(key, 1, Integer::sum));

        long normalStart = System.currentTimeMillis();
        List<String> normalKeys = leftRDD.leftOuterJoin(rightRDD)
                .map(Tuple2::_1)
                .collect();
        Map<String, Integer> normalCounter = new HashMap<>();
        long normalCost = System.currentTimeMillis() - normalStart;

        normalKeys.forEach(key -> normalCounter.merge(key, 1, Integer::sum));

        List<Integer> normalPartitionKeyCount = leftRDD.leftOuterJoin(rightRDD)
                .mapPartitions(iter -> {
                    int count = 0;
                    while (iter.hasNext()) {
                        count++;
                        iter.next();
                    }
                    return Iterators.singletonIterator(count);
                })
                .collect()
                .stream()
                .sorted()
                .collect(Collectors.toList());
        double normalGini = calculateGiniCoefficient(normalPartitionKeyCount);
        long normalMaxSize = normalPartitionKeyCount.get(normalPartitionKeyCount.size() - 1);
        long normalSumSize = normalPartitionKeyCount.stream().mapToLong(Long::valueOf).sum();


        List<Integer> balancePartitionKeyCount = balancer.leftOuterJoin(leftRDD, rightRDD)
                .mapPartitions(iter -> {
                    int count = 0;
                    while (iter.hasNext()) {
                        count++;
                        iter.next();
                    }
                    return Iterators.singletonIterator(count);
                })
                .collect()
                .stream()
                .sorted()
                .collect(Collectors.toList());
        double balanceGini = calculateGiniCoefficient(balancePartitionKeyCount);
        long balanceMaxSize = balancePartitionKeyCount.get(balancePartitionKeyCount.size() - 1);
        long balanceSumSize = balancePartitionKeyCount.stream().mapToLong(Long::valueOf).sum();

        Tuple2<JavaPairRDD<String, String>, JavaPairRDD<String, String>> balanced = balancer.balanceJoinRDDs(leftRDD, leftRDD.name(), rightRDD, rightRDD.name());
        long balanceBeforeJoinSize = balanced._1().union(balanced._2()).count();

        log.info("\n\nNormal join:\n  used = {}ms. Gini coefficient = {}, Max partition size = {}. Total size = {}. Before join size = {}",
                normalCost, normalGini, normalMaxSize, normalSumSize, leftData.size() + rightData.size());
        log.info("Quantiles: ");
        for (int i = 1; i < 10; i++) {
            log.info("  p{} = {}", i * 10, normalPartitionKeyCount.get(normalPartitionKeyCount.size() * i / 10));
        }

        log.info("\n\nBalance join:\n  used = {}ms. Gini coefficient = {}. Max partition size = {}. Total size = {}.  Before join size = {}",
                balanceCost, balanceGini, balanceMaxSize, balanceSumSize, balanceBeforeJoinSize);
        log.info("Quantiles: ");

        for (int i = 1; i < 10; i++) {
            log.info("  p{} = {}", i * 10, balancePartitionKeyCount.get(balancePartitionKeyCount.size() * i / 10));
        }


        return normalCounter.equals(balanceCounter);
    }

    @Test
    @SneakyThrows
    void testBigDataJoin() {
        List<Integer> leftData = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            for (int j = 0; j < i; j++) {
                leftData.add(i);
                leftData.add(-i);
                leftData.add(rand.nextInt(100) + 10000);
            }
        }

        List<Integer> rightData = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            for (int j = 0; j < i; j++) {
                rightData.add(i);
                leftData.add(rand.nextInt(100) + 10000);
            }
        }

        Assertions.assertTrue(
                testJoinCorrectness(
                        leftData.stream().map(String::valueOf).collect(Collectors.toList()),
                        rightData.stream().map(String::valueOf).collect(Collectors.toList()),
                        defaultBalancer
                )
        );
    }

    @Test
    @SneakyThrows
    void testBigDataLowTargetJoin() {
        List<Integer> leftData = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            for (int j = 0; j < i; j++) {
                leftData.add(i);
                leftData.add(-i);
                leftData.add(rand.nextInt(100) + 10000);
            }
        }

        List<Integer> rightData = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            for (int j = 0; j < i; j++) {
                rightData.add(i);
                leftData.add(rand.nextInt(100) + 10000);
            }
        }

        RddJoinSketchBalancer balancer = RddJoinSketchBalancer.builder()
                .enableCache(true)
                .balanceTarget(0.01f)
                .build();

        Assertions.assertTrue(
                testJoinCorrectness(
                        leftData.stream().map(String::valueOf).collect(Collectors.toList()),
                        rightData.stream().map(String::valueOf).collect(Collectors.toList()),
                        balancer
                )
        );
    }

    @Test
    @SneakyThrows
    void testBigDataExtremeLowTargetJoin() {
        List<Integer> leftData = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            for (int j = 0; j < i; j++) {
                leftData.add(i);
                leftData.add(-i);
                leftData.add(rand.nextInt(100) + 10000);
            }
        }

        List<Integer> rightData = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            for (int j = 0; j < i; j++) {
                rightData.add(i);
                leftData.add(rand.nextInt(100) + 10000);
            }
        }

        RddJoinSketchBalancer balancer = RddJoinSketchBalancer.builder()
                .enableCache(true)
                .balanceTarget(0.00001f)
                .build();

        Assertions.assertTrue(
                testJoinCorrectness(
                        leftData.stream().map(String::valueOf).collect(Collectors.toList()),
                        rightData.stream().map(String::valueOf).collect(Collectors.toList()),
                        balancer
                )
        );
    }

    @Test
    @SneakyThrows
    void testRandomJoin() {
        List<Integer> leftData = new ArrayList<>();
        for (int i = 0; i < 500000; i++) {
            leftData.add(rand.nextInt(100000));
        }

        List<Integer> rightData = new ArrayList<>();
        for (int i = 0; i < 500000; i++) {
            rightData.add(rand.nextInt(100000));
        }


        Assertions.assertTrue(
                testJoinCorrectness(
                        leftData.stream().map(String::valueOf).collect(Collectors.toList()),
                        rightData.stream().map(String::valueOf).collect(Collectors.toList()),
                        defaultBalancer
                )
        );
    }

    @Test
    @SneakyThrows
    void testNormalSkewJoin() {
        List<Integer> leftData = new ArrayList<>();
        for (int i = 0; i < 50000; i++) {
            leftData.add(rand.nextInt(10000));
            leftData.add(rand.nextInt(1000));
        }

        List<Integer> rightData = new ArrayList<>();
        for (int i = 0; i < 50000; i++) {
            rightData.add(rand.nextInt(10000));
            rightData.add(rand.nextInt(1000));
        }

        Assertions.assertTrue(
                testJoinCorrectness(
                        leftData.stream().map(String::valueOf).collect(Collectors.toList()),
                        rightData.stream().map(String::valueOf).collect(Collectors.toList()),
                        defaultBalancer
                )
        );
    }

    @Test
    @SneakyThrows
    void testHardSkewJoin() {
        List<Integer> leftData = new ArrayList<>();
        for (int i = 0; i < 50000; i++) {
            leftData.add(rand.nextInt(10000));
            leftData.add(rand.nextInt(100));
        }

        List<Integer> rightData = new ArrayList<>();
        for (int i = 0; i < 50000; i++) {
            rightData.add(rand.nextInt(10000));
            rightData.add(rand.nextInt(100));
        }

        Assertions.assertTrue(
                testJoinCorrectness(
                        leftData.stream().map(String::valueOf).collect(Collectors.toList()),
                        rightData.stream().map(String::valueOf).collect(Collectors.toList()),
                        defaultBalancer
                )
        );
    }

    @Test
    @SneakyThrows
    void testExtremeSkewJoin() {
        List<Integer> leftData = new ArrayList<>();
        for (int i = 0; i < 50000; i++) {
            leftData.add(rand.nextInt(10000));
        }
        for (int i = 0; i < 10000; i++) {
            leftData.add(rand.nextInt(10));
        }


        List<Integer> rightData = new ArrayList<>();
        for (int i = 0; i < 50000; i++) {
            rightData.add(rand.nextInt(10000));
        }
        for (int i = 0; i < 10000; i++) {
            rightData.add(rand.nextInt(10));
        }

        Assertions.assertTrue(
                testJoinCorrectness(
                        leftData.stream().map(String::valueOf).collect(Collectors.toList()),
                        rightData.stream().map(String::valueOf).collect(Collectors.toList()),
                        defaultBalancer
                )
        );
    }

}