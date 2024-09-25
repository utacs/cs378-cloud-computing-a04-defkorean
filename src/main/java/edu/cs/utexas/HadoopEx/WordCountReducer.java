package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, Wrapper, Text, FloatWritable> {

   public void reduce(Text text, Iterable<Wrapper> values, Context context)
           throws IOException, InterruptedException {
	   
        HashMap<String, float[]> taxiData = new HashMap<>(); // error, count

        
        for (Wrapper val : values) {
            
            float time = val.gettime().get();
            float total = val.gettotal().get();
            String taxiID = text.toString();

            if (taxiData.containsKey(taxiID)) {
                float[] data = taxiData.get(taxiID);
                data[0] += time;
                data[1] += total;
                taxiData.put(taxiID, data);
            } else {
                float[] data = new float[2];
                data[0] = time;
                data[1] = total;
                taxiData.put(taxiID, data);

            }
        }
        for (String taxi : taxiData.keySet()) {
            float[] data = taxiData.get(taxi);
            float moneypmin = 0;
            if (data[0] != 0 && data[1] != 0) {
                moneypmin = data[1] / (data[0] * 60);
            }
            context.write(new Text(taxi), new FloatWritable(moneypmin));
        }
   }
}