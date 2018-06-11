/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package facebookadanalyzer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author logan
 */
public class FbMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context con)throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] fields = line.split(",");
        
        con.write(new Text(fields[3]), new Text(fields[2]+","+fields[4]+","+fields[5]));
    }
    
}
