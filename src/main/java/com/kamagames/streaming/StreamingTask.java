package com.kamagames.streaming;

import ch.qos.logback.classic.Level;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author Denis Gabaydulin
 * @since 09/10/2015
 */
public class StreamingTask implements Serializable {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Logger log = LoggerFactory.getLogger(StreamingTask.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test");
        //conf.setMaster("spark://smallkiller:7077");
        conf.setMaster("local[*]");

        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger("org")).setLevel(Level.OFF);
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger("akka")).setLevel(Level.OFF);

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(2000));

        /*JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, "localhost:2181", "streaming", ImmutableMap.of("logs", 1));

        JavaDStream<String> lines = messages.map(Tuple2<String, String>::_2);

        JavaPairDStream<String, Integer> pairs = lines.mapToPair(s -> new Tuple2<>(s, s.length()));

        pairs.print();

        jssc.start();
        jssc.awaitTermination();*/


        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("spark.streaming.kafka.maxRatePerPartition", "10");

        //OffsetRange range = new OffsetRange("logs", 0, 0, 1);
        Map<TopicAndPartition, Long> fromOffsets = new HashMap<>();
        fromOffsets.put(new TopicAndPartition("logs", 0), 100000L);

        JavaPairDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                ImmutableSet.of("logs")
        );

        messages.foreachRDD(
                stringStringJavaPairRDD -> {
                    stringStringJavaPairRDD.values().foreach(
                            s -> {
                                log.info("{}", s);
                            }
                    );
                    return null;
                }
        );

        /*JavaInputDStream<String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParams,
                fromOffsets,
                MessageAndMetadata<String, String>::message
        );

        messages.foreachRDD(
                stringJavaRDD -> {
                    stringJavaRDD.foreach(
                            s -> {
                                log.info("{}", s);
                            }
                    );
                    return null;
                }
        );*/

        jssc.start();
        jssc.awaitTermination();
    }
}
