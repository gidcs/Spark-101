/**
 * Illustrates a SSHVersion in Java
 */
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import java.util.regex.*;
import org.json.JSONObject;

public class SSHVersion {
    private static Logger logger = Logger.getLogger(SSHVersion.class);

    public static void main(String[] args) throws Exception {
        String inputJson = args[0];
        String outputDir = args[1];

        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("SSHVersion");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputJson);

        // Split up into words.
        JavaRDD<String> words = input
            .flatMap(new FlatMapFunction<String, String>() {
                private Pattern http = Pattern.compile("[Hh][Tt][Tt][Pp]/");
                private Pattern ftp = Pattern.compile("([Ff][Tt][Pp])|(220)");
                public Iterable<String> call(String x) {
                    JSONObject documentObj = null;
                    try {
                        documentObj = new JSONObject(x);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to convert JSON String document into a JSON Object.", e);
                    }

                    String _word;
                    try {
                        _word = documentObj.getJSONObject("data").getJSONObject("ssh").getJSONObject("server_protocol").getString("raw_banner").replace("\n", "").replace("\r", "");

                        // Regex Matcher: find out http and ftp little bug
                        Matcher m = null;

                        m = http.matcher(_word);
                        if (m.find()) {
                            _word = "HTTP";
                            return Arrays.asList(_word);
                        }
                        m = ftp.matcher(_word);
                        if (m.find()) {
                            _word = "FTP";
                            return Arrays.asList(_word);
                        }
                        // else
                        return Arrays.asList(_word);
                    } catch (Exception e) {
                        _word = "invalid";
                        return Arrays.asList(_word);
                    }
                }
            });

        // Transform into <word, one> pair.
        JavaPairRDD<String, Integer> word_one = words
            .mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<>(s, 1);
                }
            }).cache();

        List<Tuple2<String, Integer>> result;

        JavaPairRDD<String, Integer> counts_apporache_1 = word_one
            .reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });
        result = counts_apporache_1.collect();
        counts_apporache_1.saveAsTextFile(outputDir);
        for(Tuple2 r : result)
            logger.info(r);
    }
}
