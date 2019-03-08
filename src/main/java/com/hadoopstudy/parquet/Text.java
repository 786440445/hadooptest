package com.hadoopstudy.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;

public class Text {
    public static void main(String[] args) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
                "message Pair {\n" +
                        "required binary left(UTF8);\n" +
                        "required binary right(UTF8);\n" +
                        "}");

        GroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group group = groupFactory.newGroup().append("left", "L").append("right", "R");

        Configuration conf = new Configuration();
        Path path = new Path("data.parquet");
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(schema, conf);
        ParquetWriter<Group> writer = new ParquetWriter<Group>(path, writeSupport,
                ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                ParquetProperties.WriterVersion.PARQUET_1_0, conf);
        writer.write(group);
        writer.close();
    }

    void reader() throws IOException{
        GroupReadSupport readSupport = new GroupReadSupport();
        Path path = new Path("data.parquet");
        ParquetReader<Group> reader = new ParquetReader<Group>(path, readSupport);
        Group result = reader.read();
    }
}
