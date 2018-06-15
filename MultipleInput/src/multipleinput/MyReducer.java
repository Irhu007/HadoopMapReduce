/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package multipleinput;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends	Reducer<Text, IntWritable, Text, IntWritable> 
	{
	public void reduce(Text word, Iterable<IntWritable> value_list, Context con)throws IOException, InterruptedException
	{
            int sum=0;
            for(IntWritable x:value_list)
            {
                sum+=x.get();
            }
            con.write(word, new IntWritable(sum));
        }
}
