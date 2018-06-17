/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package salaryhike;

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
public class SalaryReducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text name, Iterable<Text> value, Context con)throws IOException, InterruptedException
	{
            for(Text increase: value)
            {
                String[] data = increase.toString().trim().split(",");
            
               // double percentHike = Double.parseDouble(data);
              //  int currSalary = Integer.parseInt(data[0]);
               // con.write(name,new Text("\t"));
                double newSalar = Double.parseDouble(data[0]);
                int newSalary = (int)newSalar;
                con.write(name,new Text("\t"+newSalary));

            }
        }
}
