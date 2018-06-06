/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package innerjoinreduceside;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapOutputCollector.Context;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InnerJoinReduceSide implements Tool{
    
    public static class MapClass extends DataJoinMapperBase{

        @Override
        protected Text generateInputTag(String inputFile) {
            String datasource = inputFile;
            return new Text(datasource);
        }
        
        public void map(Text key, Text value,
                        OutputCollector output,Reporter reporter) throws IOException {
            TaggedMapOutput aRecord = generateTaggedMapOutput(value);
            Text GroupKey = generateGroupKey(aRecord);
            output.collect(GroupKey,aRecord);
        }
        
        @Override
        protected TaggedMapOutput generateTaggedMapOutput(Object value) {
            TaggedWritable ret = new TaggedWritable((Text)value);
            ret.setTag(this.inputTag);
            return ret;
        }

        @Override
        protected Text generateGroupKey(TaggedMapOutput aRecord) {
            String line=((Text)aRecord.getData()).toString();
            String[] token=line.split(",");
            Text groupkey= new Text(token[0]);
            return groupkey;
        }
        
    }
    public static class TaggedWritable extends TaggedMapOutput{
        private Writable data;
        
        public TaggedWritable(Writable data){
            this.tag=new Text("");
            this.data=data;
        }
        
        @Override
        public Writable getData() {
            return data;
        }

        @Override
        public void write(DataOutput d) throws IOException {
            this.tag.write(d);
            this.data.write(d);
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            this.tag.readFields(di);
            this.data.readFields(di);
        }
        
    }
    public static class Reduce extends DataJoinReducerBase{

        @Override
        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            if(tags.length<2) 
                return null;
            String joinedStr = "";
            for(int i=0; i<values.length;i++){
                if(i>0) 
                    joinedStr += ",";
                TaggedWritable tw = (TaggedWritable) values[i];
                String line = ((Text) tw.getData()).toString();
                String[] tokens = line.split(",",2);
                joinedStr += tokens[1];
            }
            TaggedWritable retv = new TaggedWritable(new Text(joinedStr));
            retv.setTag((Text) tags[0]);
            return retv;
        }
        
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        JobConf job = new JobConf(conf, InnerJoinReduceSide.class);
      //  job.setJarByClass(ReduceSideJoin.class);
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        job.setJobName("InnerJoinReduceSide");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.set("key.value.separator.in.input.line", ",");
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        JobClient.runJob(job);
        return 0;
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new InnerJoinReduceSide(), args);
        System.exit(res);   
    }

    @Override
    public void setConf(Configuration c) {
    }

    @Override
    public Configuration getConf() {
        return null;
    }
    
}
