package org.example.rawkv;

import com.google.protobuf.ByteString;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BatchPut implements VoidFunction<Iterator<Tuple2<ByteString, ByteString>>> {
    private final String someConfig;

    public BatchPut(String someConfig) {
        this.someConfig = someConfig;
    }

    @Override
    public void call(Iterator<Tuple2<ByteString, ByteString>> iterator) throws Exception {
        List<Tuple2<ByteString, ByteString>> cache = new ArrayList<>();
        while (iterator.hasNext()) {
            cache.add(iterator.next());
            if (cache.size() == 2) {
                StringBuilder builder = new StringBuilder();
                builder.append(this.someConfig).append(": ");
                for (Tuple2<ByteString, ByteString> pair : cache) {
                    builder.append("(").append(pair._1.toStringUtf8()).append(", ").append(pair._2.toStringUtf8()).append(") ");
                }
                cache.clear();
                System.out.println(builder);
            }
        }
        if (cache.size() != 0) {
            StringBuilder builder = new StringBuilder();
            builder.append(this.someConfig).append(": ");
            for (Tuple2<ByteString, ByteString> pair : cache) {
                builder.append("(").append(pair._1.toStringUtf8()).append(", ").append(pair._2.toStringUtf8()).append(") ");
            }
            System.out.println(builder);
        }
    }
}
