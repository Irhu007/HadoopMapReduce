/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapjoin;

import java.util.HashMap;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class joinMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private HashMap<String, String> store = new HashMap<>();
    private HashMap<String, Integer> product = new HashMap<>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] dataFile = DistributedCache.getLocalCacheFiles(conf);
        
        String record = ""; 
        
        for (Path path : dataFile)
	{
            if (path.getName().trim().equals("store.txt")) 
	    {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		    record = br.readLine();          // MGR,2  
		    while (record != null) 
		    {
			String data[] = record.split(",");                 //   [ {MGR} {2}]
			
			store.put(data[0].trim(), data[1].trim());
			record = br.readLine();
		    }
            }
            
            if (path.getName().trim().equals("product.txt")) 
	    {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		    record = br.readLine();          // MGR,2  
		    while (record != null) 
		    {
			String data[] = record.split(",");                 //   [ {MGR} {2}]
                        
                        product.put(data[0].trim(), Integer.parseInt(data[3].trim()));
			record = br.readLine();
		    }
            }
        }
    }
    
    @Override
    public void map(LongWritable key, Text value, Context con)throws IOException, InterruptedException
    {
        String[] line = value.toString().trim().split(",");
        
        int units = product.get(line[1]);
        String location = store.get(line[0]);
        
        int revenue = Integer.parseInt(line[3])*units;
        con.write(new Text(line[0]+","+location), new IntWritable(revenue));
    }
}
