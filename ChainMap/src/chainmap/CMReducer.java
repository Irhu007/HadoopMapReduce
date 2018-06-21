/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chainmap;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class CMReducer extends	Reducer<Text, IntWritable, Text, IntWritable> 
{
    @Override
    public void reduce(Text name, Iterable<IntWritable> value, Context con)throws IOException, InterruptedException
	{
            int sum=0;
            for(IntWritable x:value)
            {
                sum+=1;
            }
            con.write(name, new IntWritable(sum));
        }
}
