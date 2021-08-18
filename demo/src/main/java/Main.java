import com.google.protobuf.ByteString;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.example.rawkv.BatchPut;
import scala.Tuple2;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test");

        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> rawCsv = context.textFile("file:///Users/abing/Projects/go/src/bulk-loader/test/0.csv");

        // remove header
        final String header = rawCsv.first();
        rawCsv = rawCsv.filter((Function<String, Boolean>) s -> !s.equals(header));

        JavaPairRDD<ByteString, ByteString> pairs = rawCsv.mapToPair(
                (line) -> {
                    String[] pair = line.split(",");
                    if (pair.length < 2) {
                        return null;
                    }
                    return new Tuple2<>(ByteString.copyFromUtf8(pair[0]), ByteString.copyFromUtf8(pair[1]));
                }
        );

        pairs.foreachPartition(new BatchPut("test"));

        context.close();
    }
}
