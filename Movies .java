/* Use movies dataset. Write a map and reduce methods to determine the average rating of movies. The input consists of series of lines, each containing movie number, user number, rating and timestamp. The map should emit movie number and list of ratings and reduce should return the average rating for each movie number.
Lab: Laboratory Practice - I
Program by: Tushar B. Kute
tushar@tusharkute.com
http://tusharkute.com */

import java.io.IOException;
import java.util.*;        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Movies {
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       		String line = value.toString();  
			StringTokenizer s = new StringTokenizer(line,"\t");
			while(s.hasMoreTokens()) {
				int mnum = Integer.parseInt(s.nextToken());
				int unum = Integer.parseInt(s.nextToken());
				int rating = Integer.parseInt(s.nextToken());
				long times = Long.parseLong(s.nextToken());
				context.write(new IntWritable(mnum), new IntWritable(rating));
			}
    	}
 } 
        
 public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {	
			int sum = 0, total = 0;
			for (IntWritable rating : values) {
					int num = rating.get();
					total += 1;
					sum += num;
			}
			int avgRating = sum / total;
			context.write(key, new IntWritable(avgRating));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "Movie Ratings");
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
