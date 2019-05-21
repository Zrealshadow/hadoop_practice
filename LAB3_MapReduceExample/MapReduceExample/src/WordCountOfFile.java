import com.jcraft.jsch.IO;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @program: LAB3_MapReduceExample
 * @description: 统计每个文件下的词频，并逆序输出
 * @author: E1ixir_zzZ
 * @create: 2019-05-20 18:53
 **/
public class WordCountOfFile {

    private static Configuration conf = new Configuration();
    private static String locpath = "hdfs://localhost:9000";

    private static void init() throws Exception {
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("dfs.replication", "1");
    }

    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text valueInfo = new Text("1");
        private Text keyInfo = new Text();
        private FileSplit split;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            split = (FileSplit) context.getInputSplit();
            StringTokenizer stk = new StringTokenizer(value.toString());
            while (stk.hasMoreElements()) {
                String filename = split.getPath().getName();
                String word = stk.nextToken()+":"+filename;
                keyInfo.set(word);
                context.write(keyInfo, valueInfo);
            }
        }
    }
    public static class Combiner extends Reducer<Text,Text,Text,Text>{
        private Text outKey=new Text();
        private Text outValue=new Text();

        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException{
            int sum=0;
            for(Text value:values){
                sum+=Integer.parseInt(value.toString());
            }
            int SplitIndex=key.toString().indexOf(':');
            outKey.set(key.toString().substring(0,SplitIndex));
            String v=key.toString().substring(SplitIndex+1)+":"+Integer.toString(sum);
            System.out.println(v);
            System.out.println("***************");
            outValue.set(v);
            context.write(outKey,outValue);
        }
    }

    public static class Reduce extends Reducer<Text,Text, Text, Text> {
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
            StringBuffer string =new StringBuffer();
            for(Text val:values){
                string.append(val+";");
            }
//            System.out.println("kkk");
            context.write(key,new Text(string.toString()));
        }

    }

    public static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return -super.compare(o1, o2);
        }


    }

    public static void main(String[] args) throws Exception {
        init();
        String InputDir = locpath + "/test/WordCountInput";
        String OutputDir = locpath + "/test/WordCountOutput";
        Job job = Job.getInstance(conf, "Wordcount");
        job.setJarByClass(WordCountOfFile.class);

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(Combiner.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        job.setSortComparatorClass(IntWritableDecreasingComparator.class);

        FileInputFormat.addInputPath(job, new Path(InputDir));
        FileOutputFormat.setOutputPath(job, new Path(OutputDir));
        System.exit(job.waitForCompletion(true)?0:1);

    }
}
