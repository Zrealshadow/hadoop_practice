/**
 * @program: LAB3_MapReduceExample
 * @description:
 * @author: E1ixir_zzZ
 * @create: 2019-05-20 14:30
 **/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.Content;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.FileOutputStream;
import java.io.IOException;

public class MergyAndDeduplication {
    private static Configuration conf=new Configuration();
    private static String locpath="hdfs://localhost:9000";
    private static void init() throws Exception{
        conf.set("fs.defaultFS","hdfs://localhost:9000");
        conf.set("dfs.replication","1");
    }

    public static class Map extends Mapper<Object,Text,Text,Text>{
        private  Text text=new Text();
        public void map(Object key,Text value, Context context)throws IOException,InterruptedException{
            text=value;
            context.write(text,new Text(""));
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException{
            context.write(key,new Text(""));
        }
    }

    public static void main(String [] args) throws Exception{
        init();
        String InputDirPath=locpath+"/test/Input";
        String OutputDirPath=locpath+"/test/Output";
        Job job=Job.getInstance(conf,"MergeAndDepulicated");
        job.setJarByClass(MergyAndDeduplication.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(InputDirPath));
        FileOutputFormat.setOutputPath(job,new Path(OutputDirPath));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
