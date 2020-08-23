package com.skip.kafka_spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class SparkApplication {

    private static final long DURATION_IN_SECS = 10;

    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("TweetApp").setMaster("local[2]");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(DURATION_IN_SECS));
        streamingContext.checkpoint("./.checkpoint");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        kafkaParams.put("key.deserializer", LongDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList("SAN_FRANCISCO");
        JavaInputDStream<ConsumerRecord<Long, String>> consumerRecordJavaInputDStream = KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams));

        JavaDStream<String> tweets = consumerRecordJavaInputDStream.map(ConsumerRecord::value).window(Durations.seconds(DURATION_IN_SECS), Durations.seconds(DURATION_IN_SECS));

        // Window of 2 minutes, slides by 2 minute
        JavaDStream<Long> countStream = tweets.count();
                //.countByWindow(Durations.seconds(5),Durations.seconds(5));

        System.out.println("-----------Total tweets in last 5 seconds -----------------");
        countStream.print();

        // Split each tweet into words
        JavaDStream<List<String>> wordsOfLine = tweets.map(x -> Arrays.asList(x.split(" ")));

        // Count each word in each batch
        //JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Long> wordCounts = wordsOfLine.flatMap(List::iterator).countByValue();
        JavaPairDStream<Long,String> swappedWordCounts = wordCounts.mapToPair(Tuple2::swap);
        JavaPairDStream<Long,String> sortedStream = swappedWordCounts.transformToPair(s -> s.sortByKey(false));
        wordCounts = sortedStream.mapToPair(Tuple2::swap);
          //      pairs.reduceByKey((i1, i2) -> i1 + i2);

        // Print the first ten elements of each RDD generated in this DStream to the console
        System.out.println("----------- Most frequent words in last 5 seconds -----------------");
        wordCounts.print();

        //noinspection unchecked
        final Dataset<Row>[] tweetsDataset = new Dataset[]{null};

        StructType schema = new StructType(new
                StructField[]{ new StructField(
                "words", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });

        SparkSession spark = JavaSparkSessionSingleton.getInstance(sparkConf);


        FPGrowth fpGrowth = new FPGrowth()
                .setItemsCol("words")
                .setMinSupport(0.1)
                .setMinConfidence(0.1);

        tweets.foreachRDD(rdd -> {

            JavaRDD<Row> row = rdd.map(r -> RowFactory.create(new String[][] {r.split(" ")}));
            Dataset<Row> dataset = spark.createDataFrame(row, schema);
            FPGrowthModel fpGrowthModel = fpGrowth.fit(dataset);

            // Display frequent itemsets.
            System.out.println("----------- Frequent Itemsets in last 5 seconds -----------------");

            for(Row r : (Row[])fpGrowthModel.freqItemsets().collect()){
                if(r.getList(0).size() >= 3){
                    System.out.println(r.mkString("[", ",", "]") + ", " + r.getString(1));
                }

            };

            // Display generated association rules.
            /**System.out.println("----------- Most associated tweet words in last 2 minutes-----------------");
            fpGrowthModel.associationRules().show(false);**/

            /**if (tweetsDataset[0] != null) {
                tweetsDataset[0] = tweetsDataset[0].unionAll(dataset);
            }else{
                tweetsDataset[0] = dataset;
            }**/
        });

        //tweetsDataset[0].createOrReplaceTempView("tweets");

        streamingContext.start();

        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /** Lazily instantiated singleton instance of SparkSession */
    private static class JavaSparkSessionSingleton {
        private static transient SparkSession instance = null;

        static SparkSession getInstance(SparkConf sparkConf) {
            if (instance == null) {
                instance = SparkSession
                        .builder()
                        .config(sparkConf)
                        .getOrCreate();
            }
            return instance;
        }
    }
}
