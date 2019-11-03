package hw4.pr_mr;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class PageRankMR extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(PageRankMR.class);

	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		public void map(final Object vertex, final Text adjacencyList, final Context context) throws IOException, InterruptedException {
			
			String [] structure = adjacencyList.toString().split(","); //split the record by a comma
			
			IntWritable n = new IntWritable(Integer.parseInt(structure[0])); //create a writable as the key that sends the graph structure
			
			context.write(n, new Text(structure[1])); //pass along the graph structure
			
			String [] outlinks = structure[1].split(","); //split the adjacency list with comma
			
			for(int i = 0 ; i < outlinks.length ; i++) {
				context.write(new IntWritable(i + 1), new Text(outlinks[i]));
				//send the contributions with the outgoing links
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		

		@Override
		public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			
		}
	}

	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "PageRank");
		job.setJarByClass(PageRankMR.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new PageRankMR(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}