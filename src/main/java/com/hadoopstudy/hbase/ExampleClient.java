package com.hadoopstudy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ExampleClient {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        // create table
        Connection connection = ConnectionFactory.createConnection(config);
        try{
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            // do something
            // 创建表名
            TableName tableName = TableName.valueOf("test1");
            // 得到表描述符
            HTableDescriptor htd = new HTableDescriptor(tableName);
            // 得到簇描述符
            HColumnDescriptor hcd = new HColumnDescriptor("data");
            htd.addFamily(hcd);
            // 创建表
            admin.createTable(htd);
            // 拿到所有的表
            HTableDescriptor[] tables = admin.listTables();
            if(tables.length != 1 &&
                    Bytes.equals(tableName.getName(), tables[0].getTableName().getName())){
                throw new IOException("Failed create of table");
            }

            HTable table = (HTable) connection.getTable(tableName);
            try{
                for (int i = 1;i <= 3; i++){
                    // 设置行key
                    byte[] row = Bytes.toBytes("row" + i);
                    long ts = System.currentTimeMillis();
                    Put put = new Put(row, ts);
                    // 设置列簇
                    byte[] columnFamily = Bytes.toBytes("data");
                    byte[] qualifier = Bytes.toBytes(String.valueOf(i));
                    byte[] value = Bytes.toBytes("value" + i);
                    put.addColumn(columnFamily, qualifier, value);
                    // 往表中添加put
                    table.put(put);
                }
                Get get = new Get(Bytes.toBytes("row1"));
                Result result = table.get(get);
                System.out.println("Get :" + result);

                Scan scan = new Scan();
                ResultScanner scanner = table.getScanner(scan);
                try{
                    for(Result scannerResult: scanner){
                        System.out.println("scan :" + scannerResult);
                    }
                }finally {
                    scanner.close();
                }
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }finally {
                admin.close();//关闭连接
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}
