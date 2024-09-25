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
				
				String driverID = fields[1].trim();
				Float timeSecs = Float.parseFloat(fields[4].trim());
				Float timeMins = timeSecs / 60;
				Float amount = Float.parseFloat(fields[16].trim());

				word.set(String.valueOf(driverID));
				counter.set(timeMins);
				context.write(word, counter);


		} catch (Exception e) {
			return;
		}
	}
}