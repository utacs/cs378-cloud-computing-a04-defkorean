package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		try {
				String pickupTime = fields[2].trim();
				String dropoffTime = fields[3].trim();
				Float pickupLat = fields[6].trim().isEmpty() ? 0f : Float.parseFloat(fields[6].trim());
				Float pickupLong = fields[7].trim().isEmpty() ? 0f : Float.parseFloat(fields[7].trim());
				Float dropOffLat = fields[8].trim().isEmpty() ? 0f : Float.parseFloat(fields[8].trim());
				Float dropOffLong = fields[9].trim().isEmpty() ? 0f : Float.parseFloat(fields[9].trim());
				
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				LocalDateTime pickupDateTime = LocalDateTime.parse(pickupTime, formatter);

				int pickupHour = pickupDateTime.getHour();
				int dropOffHour = LocalDateTime.parse(dropoffTime, formatter).getHour();

				int pickUpErrors = 0;
				int dropOffErrors = 0;

				if (pickupLat == 0) {
					pickUpErrors++;
				}
				if (pickupLong == 0) {
					pickUpErrors++;
				}
				if (dropOffLat == 0) {
					dropOffErrors++;
				}
				if (dropOffLong == 0) {
					dropOffErrors++;
				}
				if (pickUpErrors > 0) {
					word.set(String.valueOf(pickupHour));
					counter.set(pickUpErrors);
					context.write(word, counter);
				}
				if (dropOffErrors > 0) {
					word.set(String.valueOf(dropOffHour));
					counter.set(dropOffErrors);
					context.write(word, counter);
				}


		} catch (Exception e) {
			// Catch-all for any other exceptions
			return;
		}
		// while (itr.hasMoreTokens()) {
		// 	word.set(itr.nextToken());
		// 	context.write(word, counter);
		// }
	}
}