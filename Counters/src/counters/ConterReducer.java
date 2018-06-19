/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package counters;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConterReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
        @Override
	public void reduce(Text store, Iterable<IntWritable> value, Context con)throws IOException, InterruptedException
	{
            int total_revenue=0;
            
            for(IntWritable data:value)
            {
                total_revenue+=data.get();
            }
            con.write(store, new IntWritable(total_revenue));
        }   
}
