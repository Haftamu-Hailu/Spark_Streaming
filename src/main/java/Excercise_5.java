
import java.util.List;
import com.google.common.collect.Iterators;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import twitter4j.Status;


public class Excercise_5 {

    public static void historicalAnalysiswithmapstate(JavaDStream<Status> statuses) {
        JavaDStream<String> words=statuses.flatMap(tweet -> Iterators.forArray(tweet.getText().split(" ")));

        JavaPairDStream<String,Integer> hashTagscounts=words.filter(word->word.startsWith("#")).mapToPair(hashtag->new Tuple2<>(hashtag,1));

        // Update the cumulative count function
        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> func = (word, optional, state) -> {
                    int currentValue = 1;
//
                    if(state.exists()) {
                        currentValue += state.get();
                    }
                    state.update(currentValue);
                    return new Tuple2<>(word, currentValue);
                };

        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> counts = hashTagscounts
                .mapWithState(StateSpec.function(func));



        JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(count -> count.swap());
        JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(count -> count.sortByKey(false));

         sortedCounts.cache();

        sortedCounts.foreachRDD(rdd -> {
            List<Tuple2<Integer, String>> sortedState = rdd.collect();
            System.out.println("Median: " + sortedState.get(sortedState.size()/2));
        });
        sortedCounts.foreachRDD(rdd -> System.out.println("Top 10: " + rdd.collect()));

    }



}



