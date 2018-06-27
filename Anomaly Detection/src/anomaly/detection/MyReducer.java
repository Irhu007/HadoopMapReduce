/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package anomaly.detection;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends	Reducer<Text, IntWritable, Text, IntWritable> 
	{
        @Override
	public void reduce(Text IP , Iterable<IntWritable> value, Context con)throws IOException, InterruptedException
	{
            int sum=0;
            for(IntWritable add:value)
            {
                sum+=1;
            }
            con.write(IP,new IntWritable(sum));
        }
}
