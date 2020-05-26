import avro.shaded.com.google.common.collect.Iterators;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;
import twitter4j.Status;


public class Exercise_2 {

    public static void get10MostPopularHashtagsInLast5min(JavaDStream<Status> statuses) {

        /*
         *  Get the stream of hashtags from the stream of tweets
         **/


        JavaDStream<String> words=statuses.filter(status -> status.getLang().equalsIgnoreCase("en") || status.getLang().equalsIgnoreCase("english"))
                .flatMap(status -> Iterators.forArray(status.getText().split(" ")));

        JavaDStream<String> hashtags = words.filter(word -> word.startsWith("#"));

        //hashtags.print();


    /* Count the hashtags over a 5 minute window*/

        JavaPairDStream<String, Integer> tuples=hashtags.mapToPair(hashtag->new Tuple2<>(hashtag,1));

        JavaPairDStream<String, Integer> counts= tuples.reduceByKeyAndWindow(
                (accum, value)->accum+value,
                (x, y)->x-y,
                Durations.minutes(5),
                Durations.seconds(1));

        /*
        counts.print();
        Find the top 10 hashtags based on their counts
        */


        JavaPairDStream<Integer,String> swapped=counts.mapToPair(count->count.swap());
        JavaPairDStream<Integer,String>sorted=swapped.transformToPair(count->count.sortByKey(false));

        sorted.foreachRDD(rdd -> {
            String out = "\nTop 10 hashtags:\n";
            for (Tuple2<Integer, String> t : rdd.take(10)) {
                out = out + t.toString() + "\n";
            }
            System.out.println(out);
        });

    }
}