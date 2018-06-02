/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package attributeaveragejava;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AttributeAverageJava {

   public static class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, Text>
   {    
          @Override
        public void map(Text key, Text value, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
              String[] field = value.toString().split(",");
              String country = field[4];
              String claims = field[8];
              
              if(claims.length()>0 && !claims.startsWith("\""))
              {
                  oc.collect(new Text(country), new Text(claims+",1"));
              }
        }
   }
   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable>
   { 
        double sum=0;
        int count =0;
        String val="m";
        @Override
        public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, DoubleWritable> oc, Reporter rprtr) throws IOException {
            while(value.hasNext())
            {
                String[] field = value.next().toString().split(",");
                sum+=Double.parseDouble(field[0]);
                count+=Integer.parseInt(field[1]);
            }
            oc.collect(key, new DoubleWritable(sum/count));
        }
       
   }
   public static class Combine implements Reducer<Text, Text, Text, Text>
   { 
        double sum=0;
        int count =0;
        @Override
        public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
            while(value.hasNext())
            {
                String[] field = value.next().toString().split(",");
                sum+=Double.parseDouble(field[0]);
                count+=Integer.parseInt(field[1]);
            }
            oc.collect(key, new Text(sum+","+count));
        }

        @Override
        public void configure(JobConf jc) {
        }

        @Override
        public void close() throws IOException {
        }
       
   }
   
   public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        JobConf job = new JobConf(conf, AttributeAverageJava.class);
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        job.setJobName("AttributeAverageJava");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Combine.class);
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.set("key.value.separator.in.input.line", ",");
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        JobClient.runJob(job);
    }
}
