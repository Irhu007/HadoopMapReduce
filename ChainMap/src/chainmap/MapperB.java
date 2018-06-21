/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chainmap;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperB extends Mapper<Text, IntWritable, Text, IntWritable>
{
    @Override
    public void map(Text key, IntWritable value, Context con)throws IOException, InterruptedException
    {
        String cname = key.toString().toLowerCase();
        
        con.write(new Text(cname), value);
    }
}
