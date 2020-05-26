

import avro.shaded.com.google.common.collect.Iterators;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import twitter4j.Status;
import java.util.List;
import java.lang.Math;

public class Exercise_4 {

    public static void decayingWindow(JavaDStream<Status> statuses) {
        //Update function
        Function2<List<Integer>, Optional<Double>, Optional<Double>> update_func = (value, state) -> {
            double current_state = state.orElse(0.0);
            double C =  Math.pow(10, -6);
            double result = current_state * (1 - C);
            for (Integer val : value) {
                result += val;
            }
            return (result < 0.25) ? Optional.empty() : Optional.of(result);
        };
        //Filter only English Hashtags
        JavaDStream<String> words = statuses.filter(status -> status.getLang().equalsIgnoreCase("en") || status.getLang().equalsIgnoreCase("english"))
                .flatMap(status -> Iterators.forArray(status.getText().split(" ")));

        JavaPairDStream<String, Integer> tuple_hashtags = words.filter(thashtag -> thashtag.startsWith("#")).mapToPair(thashtag -> new Tuple2<>(thashtag, 1));

        //Appy the function, swap and sort
        JavaPairDStream<String, Double> counts = tuple_hashtags.updateStateByKey(update_func);
        JavaPairDStream<Double, String> swapped = counts.mapToPair(count -> count.swap());
        JavaPairDStream<Double, String> sorted = swapped.transformToPair(count -> count.sortByKey(false));
        //dispaly Median hashtag and top 20 hashtags
        sorted.cache();
        sorted.foreachRDD(rdd -> {
            List<Tuple2<Double, String>> sortedState = rdd.collect();
            System.out.println("Median: " + sortedState.get(sortedState.size() / 2));
        });
        sorted.foreachRDD(rdd -> System.out.println("Top 20 Hashtags: " + rdd.take(20)));
    }

}





