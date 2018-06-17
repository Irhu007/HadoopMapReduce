/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package outerreducejoin;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class joinMapperB extends Mapper<LongWritable, Text, Text, Text>
{

    @Override
    public void map(LongWritable key, Text value, Context con)throws IOException, InterruptedException
    {
        String[] fields = value.toString().trim().split(",");
        con.write(new Text(fields[0]), new Text("Dept,"+fields[1]+","+fields[2]));
    }
}
