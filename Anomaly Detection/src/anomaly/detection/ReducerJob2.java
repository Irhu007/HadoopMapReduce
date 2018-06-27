/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package anomaly.detection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJob2 extends Reducer<Text, Text, Text, Text> 
{
    ArrayList<String> topK = new ArrayList<String>();
        @Override
	public void reduce(Text IP , Iterable<Text> value, Context con)throws IOException, InterruptedException
	{
            for(Text access:value)
            {
                topK.add(IP.toString()+","+access.toString());
               // con.write(IP, access);
            }
        }
        @Override
        protected void cleanup(Context c) throws IOException, java.lang.InterruptedException
        {
	/* sort customers based on fraudpoints */
            Collections.sort(topK, new Comparator<String>()
            {
                @Override
		public int compare(String s1, String s2)
		{
		    int fp1 = Integer.parseInt(s1.split(",")[1]);
		    int fp2 = Integer.parseInt(s2.split(",")[1]);
		    
		    return -(fp1-fp2);     /*For desscending order*/
		}});
            int count = 3;
        for(String x:topK)
            {
                if(count>0)
                {
                    String[] fields = x.split(",");
                    c.write(new Text(fields[0]),new Text(fields[1]));
                    count--;
                }
                else
                    break;
            }
        }
    
}