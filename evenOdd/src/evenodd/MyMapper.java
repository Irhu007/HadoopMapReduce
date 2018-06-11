/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package evenodd;
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
    public void map(LongWritable key, Text value, Context con)throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] fields=line.split(",");
        
        for(String num:fields)
        {
            int x = Integer.parseInt(num);
            
            if(x%2==0)
            {
                con.write(new Text("Even"),new IntWritable(x));
            }
            else
                con.write(new Text("Odd"),new IntWritable(x));
        }
    }
}