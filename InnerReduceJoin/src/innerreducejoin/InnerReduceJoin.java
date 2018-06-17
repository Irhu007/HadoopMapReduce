/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package innerreducejoin;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InnerReduceJoin {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
       Configuration conf = new Configuration();
	String[] In_Out_files = new GenericOptionsParser(conf, args).getRemainingArgs();
	
	Path input_path = new Path(In_Out_files[0]);
        Path input_path1 = new Path(In_Out_files[1]);
	Path output_directory = new Path(In_Out_files[2]);
	
        //conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        
	Job job_obj = new Job(conf, "InnerReduceJoin");
	// setting name of main class
	job_obj.setJarByClass(InnerReduceJoin.class);
	//name of mapper class
	job_obj.setMapperClass(MapperA.class);
        job_obj.setMapperClass(MapperB.class);
		// name of reducer class
	job_obj.setReducerClass(Reduce.class);
		// name of combiner class
	job_obj.setOutputKeyClass(Text.class);
	job_obj.setOutputValueClass(Text.class);
		
	MultipleInputs.addInputPath(job_obj, input_path, TextInputFormat.class, MapperA.class);
	MultipleInputs.addInputPath(job_obj, input_path1, TextInputFormat.class, MapperB.class);
        
	FileOutputFormat.setOutputPath(job_obj, output_directory);
		
	output_directory.getFileSystem(job_obj.getConfiguration()).delete(output_directory,true);
	System.exit(job_obj.waitForCompletion(true) ? 0 : 1);
    }
    
}
