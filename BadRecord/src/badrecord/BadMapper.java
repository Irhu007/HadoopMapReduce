/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package badrecord;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

//Making Enum for counters(Static)
enum READER
{ 
	BADRECORDS
}


public class BadMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
//    private Text storeLocation = new Text();
//    private Text data = new Text();
    
    @Override
     protected void map(LongWritable key, Text value,  Context context)throws IOException, java.lang.InterruptedException
     {
         String[] record = value.toString().trim().split(",");
         
         for(String check:record)
         {
                boolean numeric = true;
           try 
           {
                Double num = Double.parseDouble(check);
           } 
           catch (NumberFormatException e) {
               numeric = false;
            }
           if(!numeric)
           {
               context.getCounter(READER.BADRECORDS).increment(1);
           }
           context.write(new Text(check), new IntWritable(1));
         }
         
     }
    
    
}
