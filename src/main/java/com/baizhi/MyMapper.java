package com.baizhi;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * mapper
 *     hbase 一行记录 解析 输出 k(age) v(18)
 */
public class MyMapper extends TableMapper<Text, IntWritable> {

    /**
     *
     * @param key rowkey
     * @param value Hbase中的一行记录
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        System.out.println(key);
        byte[] bytes = value.getValue("cf1".getBytes(), "age".getBytes());
        if(bytes != null && bytes.length != 0){
            int age = Bytes.toInt(bytes);
//            输出  key为age，value为 18
            context.write(new Text("age"),new IntWritable(age));
        }
    }
}
