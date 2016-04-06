import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.nitrkl.bd.tf.TFMapper;
import com.nitrkl.bd.tf.TFReducer;
import com.nitrkl.bd.tf.TermDocumentKey;
import com.nitrkl.bd.tfidf.DocumentFrequencyValue;
import com.nitrkl.bd.tfidf.TFIDFMapper;
import com.nitrkl.bd.tfidf.TFIDFReducer;

public class StartModel {

	static String TAG = StartModel.class.getSimpleName();

	// public static int TOTAL_IP_DOCS=0;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);

		if (args.length != 3) {
			// TODO: add package with class name, set proper argument TAG by ved
			System.out.println("run: hadoop jar VectorSpaceModel.jar " + TAG + " <ip dir> <op dir>");
			return;
		}

		Job tfJob = Job.getInstance(conf, "TF counting");
		tfJob.setJarByClass(TFMapper.class);
		// specify mapper
		tfJob.setMapperClass(TFMapper.class);
		// specify output types
		tfJob.setMapOutputKeyClass(TermDocumentKey.class);
		tfJob.setMapOutputValueClass(IntWritable.class);
		// specify reducer
		tfJob.setReducerClass(TFReducer.class);
		// specify output types
		tfJob.setOutputKeyClass(Text.class);
		tfJob.setOutputValueClass(IntWritable.class);

		// specify input and output DIRECTORIES (not files)

		Path IPDIR = new Path(args[0]);
		Path OPDIR = new Path(args[1]);
		FileInputFormat.addInputPath(tfJob, IPDIR);
		FileOutputFormat.setOutputPath(tfJob, OPDIR);// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(OPDIR)) {
			hdfs.delete(OPDIR, true);
		}

		// Execute TF job
		int code = tfJob.waitForCompletion(true) ? 0 : 1;

		// job 2
		Job tfidfJob = Job.getInstance(conf, "TF*IDF counting");
		tfidfJob.setJarByClass(TFIDFMapper.class);
		// specify mapper
		tfidfJob.setMapperClass(TFIDFMapper.class);
		// specify output types
		tfidfJob.setMapOutputKeyClass(Text.class);
		tfidfJob.setMapOutputValueClass(DocumentFrequencyValue.class);
		// specify reducer
		tfidfJob.setReducerClass(TFIDFReducer.class);
		// specify output types
		tfidfJob.setOutputKeyClass(Text.class);
		tfidfJob.setOutputValueClass(Text.class);

		// specify input and output DIRECTORIES (not files)

		IPDIR = OPDIR;
		OPDIR = new Path(args[2]);
		FileInputFormat.addInputPath(tfidfJob, IPDIR);
		FileOutputFormat.setOutputPath(tfidfJob, OPDIR);// Delete output if
														// exists
		if (hdfs.exists(OPDIR)) {
			hdfs.delete(OPDIR, true);
		}

		// count no of input documents
		RemoteIterator<LocatedFileStatus> IPFILES = hdfs.listFiles(new Path(args[0]), false);
		while (IPFILES.hasNext()) {
			String f = IPFILES.next().getPath().getName();
			if (!f.endsWith("~")) {
				TFIDFReducer.TOTAL_IP_DOCS++;
			}
		}
		System.out.println("Total Documents : "+TFIDFReducer.TOTAL_IP_DOCS);
		// Execute TF job
		code = tfidfJob.waitForCompletion(true) ? 0 : 1;

		System.exit(code);
	}

}
