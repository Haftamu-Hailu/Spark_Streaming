import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;
import twitter4j.Status;

import java.util.Arrays;
import java.util.List;

public class Exercise_6 {

    public static void decayingWindow2(JavaDStream<Status> statuses) {
        float threshold = (float) 1 / 4;
        double constant = Math.pow(10,-6); // using constant 'C' as 10^6
        JavaPairDStream<String, Integer> hash = statuses.
                flatMap(x -> Arrays.asList(x.getText().split(" ")).iterator()).
                filter(h -> h.startsWith("#")).
                mapToPair(t -> new Tuple2<>(t, 1));

        Function2<List<Integer>, Optional<Double>, Optional<Double>> update_function =
                (values, state) -> {
                    double counter = 0.0;
                    try {
                        counter = state.get() * (1 - constant);
                    } catch (NullPointerException e) {
//                        e.printStackTrace();
                    }

                    for (int i : values) counter += i;
                    return (counter < threshold) ? Optional.empty() : Optional.of(counter);
                };
        JavaPairDStream<String, Double> counts = hash.updateStateByKey(update_function);
        JavaPairDStream<Double, String> swapped = counts.mapToPair(x -> x.swap());
        JavaPairDStream<Double, String> swappedCount = swapped.transformToPair(x -> x.sortByKey(false));

        swappedCount.foreachRDD(rdd -> System.out.println("Top 20: " + rdd.take(20)));
        /*
         Even though we calculated the median, however in a situation where the window size keeps decreasing, the
            probability of every element keeps changing. In our case, we keep decreasing it by multiplying it constant.
            Later, we every time the probability of the element falls below threshold, we discard the old value.
            Thus, in the case where a window is mobile and keeps decreasing(decaying) median is not justifying
            its purpose: giving the central measure of the sample due to dynamic probabilities.
         */
        swappedCount.foreachRDD(rdd -> {
            List<Tuple2<Double, String>> sortedState = rdd.collect();
            if (!sortedState.isEmpty()) {
                System.out.println("Median: " + sortedState.get(sortedState.size() / 2));
            }
        });


    }

}