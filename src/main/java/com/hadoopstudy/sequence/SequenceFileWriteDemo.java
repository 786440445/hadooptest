package com.hadoopstudy.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by Administrator on 2019/1/22.
 */
public class SequenceFileWriteDemo {
    private static final String[] DATA = {
        "One, two, buckle my shoe",
        "Three, four, shut the door",
        "Five, six, pick up sticks",
        "Seven, eight, lay them straight",
        "Nine, ten, a big fat hen"
    };

    public static void main(String[] args) throws IOException {
        String uri = "./output/sequenceFile/numbers.seq";
        Configuration conf = new Configuration();
        Path path = new Path(uri);

        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;

        try{
            writer = SequenceFile.createWriter(conf, Writer.file(path), Writer.keyClass(IntWritable.class), Writer.valueClass(Text.class), Writer.compression(CompressionType.NONE));
            for(int i = 0;i < 100; i++) {
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);
            }
        }finally {
            IOUtils.closeStream(writer);
        }
    }
}
