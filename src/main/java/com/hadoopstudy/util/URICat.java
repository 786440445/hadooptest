package com.hadoopstudy.util;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

/**
 * Created by Administrator on 2019/1/23.
 */
public class URICat {
    static{
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    public static void main(String[] args) throws Exception{
        InputStream in = null;
        String uri = "hdfs://localhost:9000/matrix/output/wordcount/quangle.txt";
        try{
            in = new URL(uri).openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
