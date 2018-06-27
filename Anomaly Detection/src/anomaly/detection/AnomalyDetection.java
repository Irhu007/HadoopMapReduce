/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package anomaly.detection;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AnomalyDetection {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // creating object of configuration class 
	Configuration conf = new Configuration();
	String[] In_Out_files = new GenericOptionsParser(conf, args).getRemainingArgs();
	
	Path input_path = new Path(In_Out_files[0]);
	Path output_directory = new Path(In_Out_files[1]);
				
	Job job_obj = new Job(conf, "Anomaly Detection");
	// setting name of main class
	job_obj.setJarByClass(AnomalyDetection.class);
	//name of mapper class
	job_obj.setMapperClass(MyMapper.class);
		// name of reducer class
	job_obj.setReducerClass(MyReducer.class);
		
	job_obj.setOutputKeyClass(Text.class);
	job_obj.setOutputValueClass(IntWritable.class);
		
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
	Job job2 = new Job(conf2, "Anamoly Detection2");
        
        job2.setJarByClass(AnomalyDetection.class);
        
        job2.setMapperClass(MapperJob2.class);
	job2.setReducerClass(ReducerJob2.class);
        
        job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
        
        /* Set input path: Read from putput of first MR */
	FileInputFormat.addInputPath(job2, new Path(output_directory + "job1/part-r-00000"));
	FileOutputFormat.setOutputPath(job2, output_directory);
	output_directory.getFileSystem(job2.getConfiguration()).delete(output_directory,true);

	/* run second MR */
	job2.waitForCompletion(true);
    }
}