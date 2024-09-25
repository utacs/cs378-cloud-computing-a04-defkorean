package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, Wrapper> {

	// Create a counter and initialize with 1
	private final FloatWritable counter = new FloatWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		try {
				String driverID = fields[1].trim();
				float time = Float.parseFloat(fields[4]);
				float total = Float.parseFloat(fields[16]);



				word.set(String.valueOf(driverID));
				Wrapper wrapper = new Wrapper(time, total);
				context.write(word, wrapper);

		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
			return;
		}
	}
}