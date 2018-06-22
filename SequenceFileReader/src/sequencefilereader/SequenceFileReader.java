/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sequencefilereader;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SequenceFileReader {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // creating object of configuration class 
	Configuration conf = new Configuration();
	String[] In_Out_files = new GenericOptionsParser(conf, args).getRemainingArgs();
	
	Path input_path = new Path(In_Out_files[0]);
	Path output_directory = new Path(In_Out_files[1]);
        
        Job job = new Job(conf, "");
        job.setJarByClass(SequenceFileReader.class);
        
        job.setMapperClass(SequenceFileMapper.class);
	job.setReducerClass(SequenceFileReducer.class);
        
        job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
        
        /* set job to read in a sequence file instead of a text file */
	job.setInputFormatClass(SequenceFileInputFormat.class);
	/* OPTIMIZATIONS */
	/* Optimization-1: Work with binary sequence files for efficiency */
	
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        /* Optimization-2: Compress sequence file */
	
	FileOutputFormat.setCompressOutput(job, true);
	FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
	SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
        
        FileInputFormat.addInputPath(job, input_path);
        
        FileOutputFormat.setOutputPath(job, output_directory);
	output_directory.getFileSystem(job.getConfiguration()).delete(output_directory,true);
        
        job.waitForCompletion(true);
    }
    
}
