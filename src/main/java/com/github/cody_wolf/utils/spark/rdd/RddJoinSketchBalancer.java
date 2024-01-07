package com.github.cody_wolf.utils.spark.rdd;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Spark RDD balance Tool.
 *
 * @author Cody-Wolf
 **/

@Slf4j
@Builder
public class RddJoinSketchBalancer implements Serializable {

    public static final int DEFAULT_SKETCH_MAX_SIZE = 100000;
    public static final float DEFAULT_BALANCE_TARGET = 0.25f;
    public static final boolean DEFAULT_ENABLE_CACHE = false;
    public static final String DEFAULT_KEY_SPLIT_TAG = "#RJSB";
    private static final Random RANDOM = new Random();

    @Builder.Default
    private final int sketchMaxSize = DEFAULT_SKETCH_MAX_SIZE;
    @Builder.Default
    private final float balanceTarget = DEFAULT_BALANCE_TARGET;
    @Builder.Default
    private final boolean enableCache = DEFAULT_ENABLE_CACHE;
    private final String keySplitTag = DEFAULT_KEY_SPLIT_TAG;

    private final Map<String, Map<String, Double>> rdd2keyScores = new HashMap<>();

    /**
     * Sketch for RDD. Once you have sketched, you can call balance methods to balance your RDD.
     * <p>
     * Using {@link JavaPairRDD#name()} to cache result of sketching
     *
     * @param rdd RDD witch using String as key
     */
    public void sketch(JavaPairRDD<String, ?> rdd) {
        sketch(rdd, rdd.name());
    }

    /**
     * Sketch for RDD. Once you have sketched, you can call balance methods to balance your RDD.
     *
     * @param rdd     RDD witch using String as key
     * @param rddName name to cache result of sketching
     */
    public void sketch(JavaPairRDD<String, ?> rdd, String rddName) {
        if (enableCache) {
            rdd.cache();
        }

        int sketchSizePerPartition = Math.floorDiv(sketchMaxSize, rdd.getNumPartitions());

        long sketchStartTime = System.currentTimeMillis();
        List<SketchInfo> infos = rdd
                .mapPartitionsWithIndex((index, iterator) -> {
                    if (iterator.hasNext()) {
                        long startTime = System.currentTimeMillis();
                        byte[] infoBytes = sketch(iterator, sketchSizePerPartition);
                        log.info("Partition(id = {}) finish key sketching in {}ms", index, System.currentTimeMillis() - startTime);
                        return Iterators.singletonIterator(infoBytes);
                    }
                    return Iterators.emptyIterator();
                }, false)
                .collect()
                .stream()
                .map(bytes -> {
                    try {
                        return SketchInfo.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
        long rddSize = infos
                .stream()
                .map(SketchInfo::getPartitionSize)
                .mapToLong(Long::longValue)
                .sum();
        long sketchTotalSize = infos
                .stream()
                .flatMap(info -> info.getSketchedKeyCountMap().values().stream())
                .mapToLong(Long::longValue)
                .sum();
        log.info("RDD(size = {}) finish sketching {} keys in {}s", rddSize, sketchTotalSize, (System.currentTimeMillis() - sketchStartTime) * 1d / 1000);

        rdd2keyScores.put(rddName, analyzeKeyScores(infos));
    }


    /**
     * Reservoir sampling for iterator of a partition.
     */
    private byte[] sketch(Iterator<? extends Tuple2<String, ?>> iterator, int sketchSize) {
        List<String> reservoir = new ArrayList<>();

        long iterSizeCount = 0;

        while (iterator.hasNext()) {
            iterSizeCount++;
            int replacePosition = RANDOM.nextInt((int) Math.min(iterSizeCount, Integer.MAX_VALUE));
            String key = iterator.next()._1;

            if (reservoir.size() < sketchSize) {
                reservoir.add(key);
            } else if (replacePosition < reservoir.size()) {
                reservoir.set(replacePosition, key);
            }
        }

        SketchInfo.Builder resultBuilder = SketchInfo.newBuilder().setPartitionSize(iterSizeCount);
        reservoir.forEach(key -> resultBuilder.putSketchedKeyCount(key, 1));

        return resultBuilder.build().toByteArray();
    }

    private Map<String, Double> analyzeKeyScores(List<SketchInfo> infos) {
        Map<String, Double> keyScores = new HashMap<>();

        infos.forEach(info -> {
            long sketchCount = info
                    .getSketchedKeyCountMap()
                    .values()
                    .stream()
                    .mapToLong(Long::longValue)
                    .sum();
            double sketchRate = 1d * sketchCount / info.getPartitionSize();

            info.getSketchedKeyCountMap().forEach((key, count) -> {
                keyScores.merge(key, 1d * count / sketchRate, Double::sum);
            });
        });

        return keyScores;
    }

    static <T extends Number> T quickFindKth(Collection<T> data, int kth) {
        Preconditions.checkArgument(kth <= data.size());

        int left = 0, right = data.size() - 1, subKth = kth;
        List<T> copy = new ArrayList<>(data);

        while (left < right) {
            T mid = copy.get((left + right) / 2);
            int l = left, r = right;
            while (l < r) {
                while (l < r && copy.get(l).doubleValue() < mid.doubleValue()) {
                    l++;
                }
                while (l < r && copy.get(r).doubleValue() > mid.doubleValue()) {
                    r--;
                }
                if (l < r) {
                    T temp = copy.get(l);
                    copy.set(l, copy.get(r));
                    copy.set(r, temp);
                    l++;
                    r--;
                }
            }
            if (subKth <= r - left + 1) { // subKth in left
                right = r;
            } else {
                subKth -= r - left + 1;
                left = r + 1;
            }
        }
        return copy.get(left);
    }

    private Map<String, Double> multiply(Map<String, Double> keyScore1, Map<String, Double> keyScore2) {
        Map<String, Double> result = new HashMap<>(keyScore2);
        keyScore1.forEach((key, score) -> {
            result.merge(key, score, (d1, d2) -> d1 * d2);
        });
        return result;
    }

    private <V> JavaPairRDD<String, V> expand(JavaPairRDD<String, V> rdd, Map<String, Double> keyScore, String suffix) {
        double waterLine = quickFindKth(keyScore.values(), Math.round(keyScore.size() * balanceTarget));

        return rdd.flatMapToPair(tuple2 -> {
            int suffixIndex = tuple2._1().indexOf(keySplitTag);
            suffixIndex = suffixIndex != -1 ? suffixIndex : tuple2._1().length();

            String originalKey = tuple2._1().substring(0, suffixIndex);
            if (keyScore.containsKey(originalKey) && keyScore.get(originalKey) > waterLine) {
                int bucketNum = (int) Math.ceil(keyScore.get(originalKey) / waterLine);
                List<Tuple2<String, V>> expanded = new ArrayList<>();
                for (int i = 0; i < bucketNum; i++) {
                    expanded.add(Tuple2.apply(tuple2._1() + suffix + "_" + i, tuple2._2()));
                }
                return expanded.iterator();
            }

            return Iterators.singletonIterator(tuple2);
        });
    }

    private <V> JavaPairRDD<String, V> flatten(JavaPairRDD<String, V> rdd, Map<String, Double> keyScore, String suffix) {
        double waterLine = quickFindKth(keyScore.values(), Math.round(keyScore.size() * balanceTarget));

        return rdd.mapToPair(tuple2 -> {
            int suffixIndex = tuple2._1().indexOf(keySplitTag);
            suffixIndex = suffixIndex != -1 ? suffixIndex : tuple2._1().length();

            String originalKey = tuple2._1().substring(0, suffixIndex);
            if (keyScore.containsKey(originalKey) && keyScore.get(originalKey) > waterLine) {
                int bucketNum = (int) Math.ceil(keyScore.get(originalKey) / waterLine);
                return Tuple2.apply(tuple2._1() + suffix + "_" + RANDOM.nextInt(bucketNum), tuple2._2());
            }

            return tuple2;
        });
    }


    /**
     * Similar to {@link JavaPairRDD#leftOuterJoin(JavaPairRDD)} but using sketched key distribution to balance join.
     * <p>
     * if there is rdd that didn't sketch before, then method won't consider the skew of the RDD.
     * Using {@link JavaPairRDD#name()} to find result of sketching.
     */
    public <V, W> JavaPairRDD<String, Tuple2<V, Optional<W>>> leftOuterJoin(JavaPairRDD<String, V> left, JavaPairRDD<String, W> right) {
        return leftOuterJoin(left, left.name(), right, right.name());
    }

    /**
     * Similar to {@link JavaPairRDD#leftOuterJoin(JavaPairRDD)} but using sketched key distribution to balance join.
     * <p>
     * if there is rdd that didn't sketch before, then method won't consider the skew of the RDD.
     *
     * @param leftName  Using cached sketched result for balancing.
     * @param rightName Using cached sketched result for balancing.
     */
    public <V, W> JavaPairRDD<String, Tuple2<V, Optional<W>>> leftOuterJoin(JavaPairRDD<String, V> left, String leftName, JavaPairRDD<String, W> right, String rightName) {
        JavaPairRDD<String, V> leftBalanced = left;
        JavaPairRDD<String, W> rightBalanced = right;

        if (rdd2keyScores.containsKey(leftName)) {
            log.info("Balancing RDD for join. flatten = {}, expand = {}", leftName, rightName);
            leftBalanced = flatten(leftBalanced, rdd2keyScores.get(leftName), keySplitTag + "&left");
            rightBalanced = expand(rightBalanced, rdd2keyScores.get(leftName), keySplitTag + "&left");
        }

        if (rdd2keyScores.containsKey(rightName)) {
            log.info("Balancing RDD for join. flatten = {}, expand = {}", rightName, leftName);
            leftBalanced = expand(leftBalanced, rdd2keyScores.get(rightName), keySplitTag + "&right");
            rightBalanced = flatten(rightBalanced, rdd2keyScores.get(rightName), keySplitTag + "&right");
        }

        JavaPairRDD<String, Tuple2<V, Optional<W>>> result = leftBalanced.leftOuterJoin(rightBalanced);

        if (rdd2keyScores.containsKey(rightName)) {
            result = result.filter(tuple2 -> tuple2._2()._2().isPresent() || !tuple2._1().contains(keySplitTag + "&right"));
        }

        return result.mapToPair(tuple2 -> {
            int suffixIndex = tuple2._1().length();
            int leftSuffixIndex = tuple2._1().indexOf(keySplitTag + "&left");
            int rightSuffixIndex = tuple2._1().indexOf(keySplitTag + "&right");
            suffixIndex = Math.min(suffixIndex, leftSuffixIndex != -1 ? leftSuffixIndex : suffixIndex);
            suffixIndex = Math.min(suffixIndex, rightSuffixIndex != -1 ? rightSuffixIndex : suffixIndex);
            String originalKey = tuple2._1().substring(0, suffixIndex);

            return Tuple2.apply(originalKey, tuple2._2());
        });

    }
}
