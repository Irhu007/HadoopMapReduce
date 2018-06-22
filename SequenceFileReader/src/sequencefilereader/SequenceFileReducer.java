/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sequencefilereader;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SequenceFileReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{	
        @Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)	throws IOException, InterruptedException
	{
		int sum = 0;
		if(key.equals("ODD"))
		{
			for (IntWritable value : values)
			{
				sum += value.get();
			}
		}
		else
		{
			for (IntWritable value : values)
			{
				sum += value.get();
		}
		
		}
		context.write(key,new IntWritable(sum));;
	}
}
