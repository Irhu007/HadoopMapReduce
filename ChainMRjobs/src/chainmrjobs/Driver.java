/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chainmrjobs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // creating object of configuration class 
	Configuration conf = new Configuration();
	String[] In_Out_files = new GenericOptionsParser(conf, args).getRemainingArgs();
	
	Path input_path = new Path(In_Out_files[0]);
	Path output_directory = new Path(In_Out_files[1]);
				
	Job job_obj = new Job(conf, "Chain Mapper");
	// setting name of main class
	job_obj.setJarByClass(Driver.class);
	
	ChainMapper.addMapper(job_obj,
                              MapperA.class, //Mapper Class
                              LongWritable.class, //Input Key class
                              Text.class, //Input Value class
                              Text.class, //Output key Class
                              IntWritable.class, //Output Value Class
                              conf);      //Job configuration to use
        
        ChainMapper.addMapper(job_obj,
                              MapperB.class, //Mapper Class
                              Text.class, //Input Key class
                              IntWritable.class, //Input Value class
                              Text.class, //Output key Class
                              IntWritable.class, //Output Value Class
                              conf);      //Job configuration to use
        
        ChainReducer.setReducer(job_obj, 
                                CMReducer.class,
                                Text.class,
                                IntWritable.class,
                                Text.class,
                                IntWritable.class,
                                conf);
		
	FileInputFormat.addInputPath(job_obj, input_path);
		
	FileOutputFormat.setOutputPath(job_obj, new Path(output_directory+"job1"));
		
	output_directory.getFileSystem(job_obj.getConfiguration()).delete(new Path(output_directory+"job1"),true);
	
//      Run first MR  
        if(!job_obj.waitForCompletion(true))
        {
            System.out.println("ERROR completing first job");
	    System.exit(1);
        }
        
//        MR-2
        Configuration conf2 = new Configuration();
	Job job2 = new Job(conf2, "Chain Job2");
        
        job2.setJarByClass(Driver.class);
        
        job2.setMapperClass(MapperJob2.class);
	job2.setReducerClass(ReducerJob2.class);
        
        job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(IntWritable.class);
        
        /* Set input path: Read from putput of first MR */
	FileInputFormat.addInputPath(job2, new Path(output_directory + "job1/part-r-00000"));
	FileOutputFormat.setOutputPath(job2, output_directory);
	output_directory.getFileSystem(job2.getConfiguration()).delete(output_directory,true);

	/* run second MR */
	job2.waitForCompletion(true);

    }
    
}
