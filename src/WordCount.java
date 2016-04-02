import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      
  	FileSplit split = (FileSplit) context.getInputSplit();
  	String[] filenameArray = split.getPath().toString().split("/");
  	String filename=filenameArray[filenameArray.length-1];
  	
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken()+"@"+filename);
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    if(args.length!=2){
    	System.out.println("run: hadoop jar VectorSpaceModel.jar "+WordCount.class.getSimpleName()+" <ip dir> <op dir>");
    	return;
    }
    Path IPDIR = new Path(args[0]);
    Path OPDIR = new Path(args[1]);
    FileInputFormat.addInputPath(job,IPDIR);
    FileOutputFormat.setOutputPath(job, OPDIR);// Delete output if exists
	FileSystem hdfs = FileSystem.get(conf);
	if (hdfs.exists(IPDIR)){
		hdfs.delete(OPDIR, true);
	}

	// Execute the line counting job
	int code = job.waitForCompletion(true) ? 0 : 1;
	System.exit(code);
  }
}