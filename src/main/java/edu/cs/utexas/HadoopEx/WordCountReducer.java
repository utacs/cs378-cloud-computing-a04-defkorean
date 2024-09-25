package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, FloatWritable, Text, FloatWritable> {

   public void reduce(Text text, Iterable<FloatWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       HashMap<String, float[]> taxiData = new HashMap<>(); // error, count
        PriorityQueue<String[]> errors = new PriorityQueue<>((a, b) -> Float.compare(Float.parseFloat(a[1]), Float.parseFloat(b[1])));

       
        for (FloatWritable val : values) {
            
            float value = val.get();
            String taxiID = text.toString();
            // System.out.println("Taxi ID: " + taxiID + " Errors: " + value);

            if (taxiData.containsKey(taxiID)) {
                float[] data = taxiData.get(taxiID);
                data[0] += value;
                data[1] += 1;
                taxiData.put(taxiID, data);
            } else {
                float[] data = new float[2];
                data[0] = value;
                data[1] = 1;
                taxiData.put(taxiID, data);

            }
        }
        for (String taxi : taxiData.keySet()) {
            float[] data = taxiData.get(taxi);
            float averageError = 0;
            if (data[0] != 0 && data[1] != 0) {
                averageError = data[0] / data[1];
            }
            errors.add(new String[]{taxi, String.valueOf(averageError)});
            if (errors.size() > 5) {
                errors.poll();
            }
            context.write(new Text(taxi), new FloatWritable(averageError));
        }
        // for (String[] error : errors) {
        //     System.out.println("Taxi ID: " + error[0] + " Average Error: " + error[1]);
        // }
       
   }
}