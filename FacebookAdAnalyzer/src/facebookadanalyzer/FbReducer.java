/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package facebookadanalyzer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author logan
 */
public class FbReducer extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text word, Iterable<Text> value_list, Context con)throws IOException, InterruptedException
	{
            HashMap<String,String> hash= new HashMap<>();
            Iterator<Text> x= value_list.iterator();
            
            while(x.hasNext())
            {
                String[] content = x.next().toString().split(",");
                String location = content[0].trim();
                int ads= Integer.parseInt(content[1]);
                int clicked = Integer.parseInt(content[2]);
                
                double succRate=(clicked/(ads*1.0))*100;
                
                if(hash.containsKey(location))
                {
                    String s1 = hash.get(location);
                    String[] cityValue =  s1.split(",");
                    succRate = Double.parseDouble(cityValue[0]) + succRate;
                    int count = Integer.parseInt(cityValue[1])+1;
                    
                    hash.put(location,succRate+","+count);
                }
                else
                {
                    hash.put(location, succRate+",1");
                }
            }
            for(Map.Entry<String,String> e: hash.entrySet())
            {
                String[] V1= e.getValue().split(",");
                Double avgSuccess = Double.parseDouble(V1[0])/Integer.parseInt(V1[1]);
                con.write(word, new Text(e.getKey()+"\t"+avgSuccess));
            }
            
        }
    
}
