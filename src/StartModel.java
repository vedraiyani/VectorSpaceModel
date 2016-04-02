import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.nitrkl.bd.tf.TFMapper;
import com.nitrkl.bd.tf.TFReducer;
import com.nitrkl.bd.tf.TermDocumentWritable;

public class StartModel {
	
	static String TAG = StartModel.class.getSimpleName();

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);

		if (args.length != 2) {
		 // TODO: add package with class name TAG by ved
			System.out.println("run: hadoop jar VectorSpaceModel.jar " + TAG + " <ip dir> <op dir>");
			return;
		}
		
		Job tfJob = Job.getInstance(conf, "JobName");
		tfJob.setJarByClass(TFMapper.class);
		// specify mapper
		tfJob.setMapperClass(TFMapper.class);
		// specify output types
		tfJob.setMapOutputKeyClass(TermDocumentWritable.class);
		tfJob.setMapOutputValueClass(IntWritable.class);
		// specify reducer
		tfJob.setReducerClass(TFReducer.class);
		// specify output types
		tfJob.setOutputKeyClass(Text.class);
		tfJob.setOutputValueClass(IntWritable.class);

		// specify input and output DIRECTORIES (not files)

	    Path IPDIR = new Path(args[0]);
	    Path OPDIR = new Path(args[1]);
	    FileInputFormat.addInputPath(tfJob,IPDIR);
	    FileOutputFormat.setOutputPath(tfJob, OPDIR);// Delete output if exists
	    FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(IPDIR)){
			hdfs.delete(OPDIR, true);
		}

		// Execute TF job
		int code = tfJob.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
	}

}
