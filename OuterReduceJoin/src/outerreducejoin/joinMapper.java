package outerreducejoin;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class joinMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    public void map(LongWritable key, Text value, Context con)throws IOException, InterruptedException
    {
        String[] fields = value.toString().trim().split(",");
        con.write(new Text(fields[5]), new Text("Emp,"+fields[0]+","+fields[1]+","+fields[2]+","+fields[3]+","+fields[4]));
    }
}
