package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, FloatWritable> {

	// Create a counter and initialize with 1
	private final FloatWritable counter = new FloatWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		try {
				Float pickupLat = fields[6].trim().isEmpty() ? 0f : Float.parseFloat(fields[6].trim());
				Float pickupLong = fields[7].trim().isEmpty() ? 0f : Float.parseFloat(fields[7].trim());
				Float dropOffLat = fields[8].trim().isEmpty() ? 0f : Float.parseFloat(fields[8].trim());
				Float dropOffLong = fields[9].trim().isEmpty() ? 0f : Float.parseFloat(fields[9].trim());
				String taxiID = fields[0].trim();
				float errors = 0;

				if (Math.abs(pickupLat) < 0.001 || Math.abs(pickupLong) < 0.001 || Math.abs(dropOffLat) < 0.001 || Math.abs(dropOffLong) < 0.001) {
					errors = 1;
				}

				// if (errors > 0) {
				word.set(String.valueOf(taxiID));
				counter.set(errors);
				context.write(word, counter);
				// }


		} catch (Exception e) {
			return;
		}
	}
}