package com.kamagames.streaming;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ComparisonChain.start;

/**
 * @author Denis Gabaydulin
 * @since 25/10/2015
 */
public class SessionPartitionerTask implements Serializable {
    private static final Pattern SESSION = Pattern.compile("session=([0-9A-Z]{32})");
    private static final Logger log = LoggerFactory.getLogger(TestTask.class);

    static class Request implements Serializable {
        String id;
        DateTime dateTime;
        String sessionId;

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("id", id)
                    .add("dateTime", dateTime)
                    .add("sessionId", sessionId)
                    .toString();
        }
    }

    static class SessionBasedRequest implements Serializable {
        String sessionId;
        List<Request> requests;
        DateTime min;

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("sessionId", sessionId)
                    .add("requests", requests)
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

        JavaRDD<Request> requestRdd = lines.map(
                s -> {
                    Request request = new Request();

                    Matcher sessionMatcher = SESSION.matcher(s);
                    if (sessionMatcher.find()) {
                        request.sessionId = sessionMatcher.group(1).trim();
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
        )
                .filter(request -> request.id != null)
                .filter(request -> true);

        JavaPairRDD<String, Iterable<Request>> requestLines = requestRdd.groupBy(request -> request.id);

        requestLines.map(
                requests -> {
                    SessionBasedRequest sessionBasedRequest = new SessionBasedRequest();
                    sessionBasedRequest.requests = new ArrayList<>();

                    Request min = null;

                    for (Request r : requests._2) {
                        if (r.sessionId != null) {
                            sessionBasedRequest.sessionId = r.sessionId;
                        }

                        if (min == null || r.dateTime.isBefore(min.dateTime.getMillis())) {
                            min = r;
                        }

                        sessionBasedRequest.requests.add(r);
                    }

                    if (sessionBasedRequest.sessionId == null) {
                        sessionBasedRequest.sessionId = "n/a";
                    }

                    sessionBasedRequest.min = min.dateTime;

                    return sessionBasedRequest;
                }
        )
                .mapToPair(sessionBasedRequest -> new Tuple2<>(new Tuple2<>(sessionBasedRequest.sessionId, sessionBasedRequest.min), sessionBasedRequest))
                .repartitionAndSortWithinPartitions(new SessionPartitioner(2), new DateTimeComparator())
                .foreach(stringSessionBasedRequestTuple2 -> log.info("{}", stringSessionBasedRequestTuple2._2));
    }

    private static class SessionPartitioner extends Partitioner implements Serializable {

        private final int partitions;

        public SessionPartitioner(int partitions) {
            this.partitions = partitions;
        }

        @Override
        public int numPartitions() {
            return partitions;
        }

        @Override
        public int getPartition(Object o) {
            Tuple2<String, DateTime> sessionAndDateTime = (Tuple2<String, DateTime>) o;
            if (null != o) {
                return Hashing.consistentHash(StringUtils.splitPreserveAllTokens(sessionAndDateTime._1)[0].hashCode(), partitions);
            } else {
                return 0;
            }
        }
    }

    private static class DateTimeComparator implements Comparator<Tuple2<String, DateTime>>, Serializable {
        @Override
        public int compare(Tuple2<String, DateTime> o1, Tuple2<String, DateTime> o2) {
            return start()
                    .compare(o1._2, o2._2)
                    .result();
        }
    }
}
