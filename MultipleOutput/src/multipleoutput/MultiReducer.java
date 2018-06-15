/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package multipleoutput;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultiReducer extends Reducer<Text, Text, Text, Text> 
{
    private MultipleOutputs<Text,Text> out;
    
    @Override
     protected void setup(Context c)	throws IOException, java.lang.InterruptedException
    {
	out = new MultipleOutputs(c);
    }
    @Override
    public void reduce(Text key, Iterable<Text> value_list, Context con)throws IOException, InterruptedException
    {
        String name="";
        int Salary=0;
        String dept="";
        
        for(Text detail:value_list)
        {
            String[] data = detail.toString().split(",");
            name = data[0];
            dept = data[1];
            Salary+=Integer.parseInt(data[2]);
        }
        if(dept.equalsIgnoreCase("hr"))
        {
            out.write("HR", key, new Text(name +","+Salary));
        }
        else
        {
            out.write("Accounts", key, new Text(name +","+Salary));
        } 
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        out.close();
    }
    
}
