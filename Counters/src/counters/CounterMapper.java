/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package counters;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

//Making Enum for counters(Static)
enum LOCATION
{ 
	TOTAL, BANGALORE,CHENNAI, HYDERABAD
}

public class CounterMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
//    private Text storeLocation = new Text();
//    private Text data = new Text();
    
    @Override
     protected void map(LongWritable key, Text value,  Context context)throws IOException, java.lang.InterruptedException
     {
         //Incrementing counter values using context
         context.getCounter(LOCATION.TOTAL).increment(1);
         
         String[] fields = value.toString().trim().split(",");
         
         if(fields[4].equalsIgnoreCase("bangalore"))
         {
             context.getCounter(LOCATION.BANGALORE).increment(1);
         }
         else if(fields[4].equalsIgnoreCase("hyderabad"))
         {
             context.getCounter(LOCATION.HYDERABAD).increment(1);
         }
         else if(fields[4].equalsIgnoreCase("chennai"))
         {
             context.getCounter(LOCATION.CHENNAI).increment(1);
         }
         else
             throw new RuntimeException("No such City");
         
         int sale=Integer.parseInt(fields[3]);
         
         if(sale<10)
//      Dynamic Counters
             context.getCounter("SALES", "LOW_SALES").increment(1);
         int price = Integer.parseInt(fields[2]);
         int revenue = sale*price;
         if(revenue>500)
//      Dynamic Counters
             context.getCounter("SALES", "HIGH_SALES").increment(1);
         
         context.write(new Text(fields[4]), new IntWritable(revenue));
     }
}
