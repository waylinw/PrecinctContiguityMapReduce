
                  // Data containers for Map() and Reduce() functions

                  // You would import the data types needed for your keys and values
import org.apache.hadoop.io.IntWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class


                 // For Map and Reduce jobs

import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function

                 // To start the MapReduce process

import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver


                // For File "I/O"

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename


// Exception handling

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.lang.String;


public class PrecinctContiguity { 
	// Mapper  Class Template

	public static class segmentMapper
     extends Mapper< LongWritable, Text, Text,  Text> {

		public void map(LongWritable key, Text value, Context context)
     		throws IOException, InterruptedException {
		
			String values[] = value.split("(");
			String precID = values[0];
			String keyString;

			if (values.length > 2) {
				String prev = "(" + values[1];
				prev = prev.strip(" ");

				for (int i=2; i < values.length; i++) {
					String cur = "(" + values[i];
					cur = cur.strip(" ");

					if (cur.compareTo(prev) < 0) {
						keyString = cur+" "+prev;
					}
					else {
						keyString = prev+" "+cur;
					}
				
					context.write(new Text(keyString), new Text(precID));
				}
			}
 		} // map
	} //segmentMapper


	public static class segmentReducer   // needs to replace the four type labels with actual Java class names
     		extends  Reducer<Text, Text, Text, Text> {

		@Override  // we are overriding the Reducer's reduce() method
		public void reduce( Text key, Iterable<Text> values, Context context)
     			throws IOException, InterruptedException {
			// Find distance of segments
			String[] points = key.split(" ");


			String[] precIDs = new String[2];
    		idx = 0;
			for(Text line : values) {
        		precIDs[idx++] = line.toString();
    		}

			if (idx == 2) {
				context.write(new Text(precIDs[0]), new Text(precIDs[1]));
				context.write(new Text(precIDs[1]), new Text(precIDs[0]));
			}
		} // reduce
	} // reducer

	public static class contiguityMapper     // Need to replace the four type labels there with actual Java class names
     		extends Mapper< LongWritable, Text, IntWritable, Text > {

		@Override
		public void map(Text key, Text value, Context context)
      		throws IOException, InterruptedException {
    		context.write(key, value);
		} // map
	} // totalCountMapper

	public static class contiguityReducer   // needs to replace the four type labels with actual Java class names
      	extends  Reducer< IntWritable, Text, IntWritable, Text> {

		@Override  // we are overriding the Reducer's reduce() method
		public void reduce( IntWritable key, Iterable<Text> values, Context context)
     			throws IOException, InterruptedException {

       	context.write(new IntWritable(index), new Text(outputString));
    	}
    } // reduce


} // reducer



//  MapReduce Driver

	public static void main(String[] args) throws Exception {
		// First MapReduce job
     	Job firstJob = Job.getInstance();
     	firstJob.setJarByClass(JavaContiguity.class);  

		FileInputFormat.addInputPath(firstJob, new Path("redistricting/prec_points")); // put what you need as input file
      FileOutputFormat.setOutputPath(firstJob, new Path("redistricting/prec_segments")); // put what you need as output file

      firstJob.setMapperClass(segmentMapper.class);
      firstJob.setReducerClass(segmentReducer.class);
  
      firstJob.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
      firstJob.setOutputValueClass(Text.class); 

      firstJob.setJobName("Finding contiguous line segments");
      firstJob.waitForCompletion(true);

  		// Second MapReduce job
   	Job secondJob = Job.getInstance();
   	secondJob.setJarByClass(JavaContiguity.class);
   	
		FileInputFormat.addInputPath(secondJob, new Path("prec_segments")); // put what you need as input file
   	FileOutputFormat.setOutputPath(secondJob, new Path("prec_contiguity")); // put what you need as output file
   	seedsJob.setMapperClass(seedsMapper.class);
   	seedsJob.setReducerClass(seedsReducer.class);
   	seedsJob.setOutputKeyClass(IntWritable.class);
   	seedsJob.setOutputValueClass(Text.class);
   	seedsJob.setJobName("finish up the job");
   
   System.exit(seedsJob.waitForCompletion(true) ? 0: 1);
   

  } // main()


} // MyMapReduceDriver

