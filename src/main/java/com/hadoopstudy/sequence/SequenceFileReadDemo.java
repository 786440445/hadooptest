package com.hadoopstudy.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * Created by Administrator on 2019/1/22.
 */
public class SequenceFileReadDemo {
    public static void main(String[] args) throws IOException{
        String uri = "./output/sequenceFile/numbers.seq";
        Configuration conf = new Configuration();
        Path path = new Path(uri);

        SequenceFile.Reader reader = null;

        try{
            reader = new SequenceFile.Reader(conf, Reader.file(path));
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();
            while(reader.next(key, value)){
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition();
            }
        }finally {
            IOUtils.closeStream(reader);
        }
    }
}
