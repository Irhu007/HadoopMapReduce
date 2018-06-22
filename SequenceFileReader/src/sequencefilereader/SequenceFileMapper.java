/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sequencefilereader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SequenceFileMapper extends Mapper<LongWritable, BytesWritable, Text, IntWritable> 
{

	
    public void map(LongWritable key, BytesWritable value, Context context)throws java.io.IOException, InterruptedException
    {
        String value_str = new String(value.getBytes(), "UTF-8");
	String data[]=value_str.toString().split(",");    //data =  [85 131 993 392 689....]
		
	
	for(String num:data){
	    int number=Integer.parseInt(num.trim());
	    if((number%2)==1){
		context.write(new Text("ODD"), new IntWritable(number));   //  ODD 85  131  993 
	    }
	    else{
		context.write(new Text("EVEN"), new IntWritable(number));   //  EVEN  392
	    }
			
	}
    }
    
}
