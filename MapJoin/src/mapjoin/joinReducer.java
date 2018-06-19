/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapjoin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class joinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    @Override
    public void reduce(Text name, Iterable<IntWritable> value, Context con)throws IOException, InterruptedException
	{
            int revenue=0;
            
            for(IntWritable store:value)
            {
                revenue+=store.get();
            }
            con.write(name, new IntWritable(revenue));
        }
    
}
