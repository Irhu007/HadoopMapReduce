/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package innerreducejoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> 
{
    public void reduce(Text key, Iterable<Text> value_list, Context con)throws IOException, InterruptedException
    {
        List<String> Name = new ArrayList<String>();
        List<String> Hero = new ArrayList<String>();
        
        for(Text data:value_list)
        {
            String[] record = data.toString().trim().split(",");
            
            if(record[0].equalsIgnoreCase("Name"))
                Name.add(record[1]);
            else
                Hero.add(record[1]);
        }
        if(!Name.isEmpty() && !Hero.isEmpty())
        {
            for(String set1:Name)
            {
                for(String set2:Hero)
                {
                    con.write(key, new Text(set1+","+set2));
                }
            }
        }
    }
}
