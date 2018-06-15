/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package multipleinput;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper2 extends Mapper<Text, Text, Text, IntWritable>
{
    public void map(Text key, Text value, Context con)throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] fields=line.split(",");
        
        for(String temp:fields)
        {
            con.write(new Text(temp),new IntWritable(1));
        }
        
    }
    
}
