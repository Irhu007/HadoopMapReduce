/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package anomaly.detection;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author logan
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    public void map(LongWritable key, Text value, Mapper.Context con)throws IOException, InterruptedException
    {
        String[] fields = value.toString().trim().split(",");
        if(Character.isDigit(fields[0].charAt(0)))
            con.write(new Text(fields[0]), new IntWritable(1));
        
    }
    
}
