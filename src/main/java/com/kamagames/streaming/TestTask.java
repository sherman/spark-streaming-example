package com.kamagames.streaming;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.stat.Statistics;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * @author Denis Gabaydulin
 * @since 09/10/2015
 */
public class TestTask implements Serializable {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Logger log = LoggerFactory.getLogger(TestTask.class);

    static class Request implements Serializable {
        String id;
        DateTime dateTime;
        boolean sql;

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("id", id)
                    .add("dateTime", dateTime)
                    .add("sql", sql)
                    .toString();
        }
    }

    static class RequestTime implements Serializable {
        String id;
        long mills;
        int sql;

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("id", id)
                    .add("mills", mills)
                    .add("sql", sql)
                    .toString();
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test");
        //conf.setMaster("spark://smallkiller:7077");
        conf.setMaster("local[*]");

        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger("org")).setLevel(ch.qos.logback.classic.Level.OFF);
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger("akka")).setLevel(ch.qos.logback.classic.Level.OFF);

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/home/sherman/webapp_logs/webapp.log");

        JavaRDD<Request> requests = lines.map(
                s -> {
                    Request request = new Request();

                    if (s.contains("Preparing:")) {
                        request.sql = true;
                    }

                    String requestId = FluentIterable.from(Splitter.on(" ").split(s)).filter(
                            input -> input.startsWith("reqId:")
                    ).transform(
                            input -> input.replace("reqId:", "")
                    ).first().orNull();

                    if (requestId != null) {
                        request.id = requestId;

                        String pattern = "yyyy-MM-dd HH:mm:ss,SSS";
                        ImmutableList<String> parts = FluentIterable.from(Splitter.on(" ").split(s)).toList();

                        DateTime dateTime = null;
                        try {
                            dateTime = DateTime.parse(parts.get(0) + " " + parts.get(1), DateTimeFormat.forPattern(pattern));
                        } catch (Exception e) {
                            return request;
                        }

                        request.dateTime = dateTime;

                        return request;
                    } else {
                        return request;
                    }
                }
        ).filter(
                request -> request.id != null
        ).filter(request -> true);

        JavaPairRDD<String, Iterable<Request>> requestLines = requests.groupBy(request -> request.id);

        JavaRDD<RequestTime> requestTimeRdd = requestLines.map(
                stringIterableTuple2 -> {
                    Request min = null, max = null;
                    int sql = 0;
                    for (Request r : stringIterableTuple2._2) {
                        if (r.sql) {
                            ++sql;
                        }

                        if (r.dateTime == null) {
                            continue;
                        }

                        if (min == null || r.dateTime.isBefore(min.dateTime.getMillis())) {
                            min = r;
                        }

                        if (max == null || r.dateTime.isAfter(max.dateTime.getMillis())) {
                            max = r;
                        }
                    }

                    RequestTime rt = new RequestTime();
                    rt.id = min.id;
                    rt.mills = max.dateTime.getMillis() - min.dateTime.getMillis();
                    rt.sql = sql;

                    return rt;
                }
        ).filter(
                requestTime -> requestTime.mills < 30000 && requestTime.sql < 30
        );

        JavaPairRDD<Double, Double> pairs = requestTimeRdd.
                filter(
                        requestTime -> requestTime.sql > 0
                )
                .mapToPair(
                        requestTime -> new Tuple2((double) requestTime.mills, (double) requestTime.sql)
                );

        double corr = Statistics.corr(pairs.keys(), pairs.values(), "pearson");

        log.info("corr: {}", corr);
    }
}
