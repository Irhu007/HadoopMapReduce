/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package outerreducejoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class joinReduce extends Reducer<Text, Text, Text, Text> 
{
    @Override
    public void reduce(Text key, Iterable<Text> value_list, Context con)throws IOException, InterruptedException
    {
        List<String> Emp = new ArrayList<>();
        List<String> Dept = new ArrayList<>();
        
        for(Text data:value_list)
        {
            String[] record = data.toString().trim().split(",");
            
            if(record[0].equalsIgnoreCase("Emp"))
                Emp.add(record[1]+","+record[2]+","+record[3]+","+record[4]+","+record[5]);
            else
                Dept.add(record[1]+","+record[2]);
        }
        if(!Emp.isEmpty() && !Dept.isEmpty())       //Inner Join
        {
            for(String set1:Emp)
            {
                for(String set2:Dept)
                {
                    con.write(key, new Text(set1+","+set2));
                }
            }
        }
        if(!Emp.isEmpty() && Dept.isEmpty())        //Left Join
        {
            for(String set1:Emp)
            {
                con.write(key, new Text(set1+",NULL,NULL"));
            }
        }
        if(Emp.isEmpty() && !Dept.isEmpty())        //Right Join
        {
            for(String set2:Dept)
            {
                con.write(key, new Text("NULL,NULL,NULL,NULL,NULL"+set2));
            }
        }
    }
    
}
