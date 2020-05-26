import com.google.common.collect.Iterators;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import twitter4j.Status;

import java.util.List;
public class Exercise_3 {

    public static void historicalAnalysis(JavaDStream<Status> statuses) {
        JavaDStream<String> words = statuses
                .flatMap(status -> Iterators.forArray(status.getText().split(" ")));

        JavaDStream<String> hashTags = words
                .filter(word -> word.startsWith("#"));

        JavaPairDStream<String, Integer> tuples = hashTags.
                mapToPair(hashtag -> new Tuple2<>(hashtag,1));

        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                (values, state) -> {
                    Integer newSum = state.or(0);
                    for(int i : values) {
                        newSum += i;
                    }
                    return Optional.of(newSum);
                };

        JavaPairDStream<String, Integer> counts = tuples.updateStateByKey(updateFunction);
        JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(count -> count.swap());
        JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(count -> count.sortByKey(false));

        sortedCounts.cache();
        //As no window operation is being used, foreachRDD yields only 1 RDD for the current microbatch
        
        sortedCounts.foreachRDD(rdd -> {
            List<Tuple2<Integer,String>> sortedState = rdd.collect();
            System.out.println("Median: "+sortedState.get(sortedState.size()/2));
        });
        sortedCounts.foreachRDD(rdd -> System.out.println("Top 10: "+rdd.take(10)));
    }

}
