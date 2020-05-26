package exercise_1;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import twitter4j.Status;

import java.util.List;
import java.util.Set;

public class Exercise_1 {

	public static void sentimentAnalysis(JavaDStream<Status> statuses) {
		Set<String> pos = PositiveWords.getWords();
		Set<String> neg = NegativeWords.getWords();
		List<String> stop = StopWords.getWords();
		JavaPairDStream<Long,String> tweet = statuses.
			filter(text -> LanguageDetector.isEnglish(text.getText()) ).
			filter(text -> (text.getText().contains("coronavirus") || text.getText().contains("COVID"))).
			mapToPair(x-> new Tuple2<>(x.getId(), x.getText())).
			filter(tup -> !tup._2().isEmpty() && tup._1()!=null).
			mapToPair(t-> new Tuple2<>(t._1,t._2().replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase()));


		JavaPairDStream<Long,String> filterTweet = tweet.
				mapToPair(x-> {
					String [] words = x._2().split(" ");
					for(String i: words){
						if(stop.contains(i)) x._2().replaceAll(i,"");
					}
					return  new Tuple2<>(x._1,x._2);
				});
		JavaDStream<Tuple4<Long,String,Float,Float>> pScoreTweet = filterTweet.
				map(x-> {
					String [] words = x._2().split(" ");
					int pScore = 0;
					int nScore = 0;
					int n = words.length;
					for(String i: words){
						if(pos.contains(i)) pScore+=1;
						else if (neg.contains(i)) nScore+=1;
					}
					return  new Tuple4<>(x._1,x._2, ((float) pScore)/n, ((float) nScore)/n );
				});
		JavaDStream<Tuple5<Long,String,Float,Float,String>> finalStruc = pScoreTweet.
				map(x->{
					String verdict;
					if( x._3() > x._4()) verdict = "positive";
					else if(x._3() < x._4()) verdict = "negative";
					else verdict = "neutral";
					return new Tuple5<>(x._1(),x._2(),x._3(),x._4(), verdict);
				});
		finalStruc.print();

	}
}
