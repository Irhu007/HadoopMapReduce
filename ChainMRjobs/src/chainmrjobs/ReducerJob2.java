/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chainmrjobs;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJob2 extends Reducer<Text, IntWritable, Text, IntWritable> 
{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context c)throws IOException, java.lang.InterruptedException
    {

	int characterCount = 0;
	for (IntWritable count : values)
	{
	    characterCount += count.get();
	}
	/* emit total count for words starting with character */
	c.write(key, new IntWritable(characterCount));
    }
}
