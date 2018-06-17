/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package salaryhike;

import java.util.HashMap;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
/**
 *
 * @author logan
 */
public class SalaryMapper extends Mapper<LongWritable, Text, Text, Text>
{
   private HashMap<String, Double> hash = new HashMap<>();      // [ [ {MGR: 2} {DLP:5} {HR:6} ] ]
                                                                                                                                 
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
	/* read data from distributed cache */
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] dataFile = DistributedCache.getLocalCacheFiles(conf);
        
	String record = "";         // MGR,2  

		    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(dataFile[0])));
		    record = br.readLine();          // MGR,2  
		    while (record != null) 
		    {
			String data[] = record.split(",");                 //   [ {MGR} {2}]
			/* designation_code, increment_multiplier */
			hash.put(data[0].trim(), Double.parseDouble(data[1].trim()));
			record = br.readLine();
		    }		
 	
    }
    
    @Override
    public void map(LongWritable key, Text value, Context con)throws IOException, InterruptedException
    {
        String[] fields= value.toString().split(",");
        Double n;
        if(fields[2].equalsIgnoreCase("manager"))
        {
                 n= hash.get("MNG");      
        }
        else if(fields[2].equalsIgnoreCase("developer")){
            n = hash.get("DLP");
        }
        else
            n = hash.get("HR");
        
        double salary = (Integer.parseInt(fields[3])*(n/100.0))+Integer.parseInt(fields[3]);
        
        con.write(new Text(fields[1]),new Text(""+salary));
    }
}   
