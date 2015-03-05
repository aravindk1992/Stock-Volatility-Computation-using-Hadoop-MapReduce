
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.collect.Multiset.Entry;

public class StockVolatility1 {
	
	/**
	 * @author aravindk@buffalo.edu
	 * read file line by line
	 * Extract only the Date and Closing balance values
	 * Output key= FILENAME +"\t" + YEAR + "\t"+MONTH
	 * Output Value = DAY +"\t"+ADJUSTED CLOSING BALANCE
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text stockName = new Text(); // output key
		private Text vals = new Text(); // Output Value
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String line= value.toString();
		  	String element[] = null;
			element = line.split(","); //split the csv
			
			if(line.contains("Date")){ // ignore the first line that contains
					return;				// labels
				}
			
			//Find the filename and store is as filename
			// Reference http://stackoverflow.com/questions/19012482/how-to-get-the-input-file-name-in-the-mapper-in-a-hadoop-program
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			
			if(filename.contains(".csv")){	// remove any ".csv" from the filename
				filename= filename.split(".csv")[0];
			}
			
			String token[]= element[0].split("-");
			stockName.set(new Text(filename+"\t"+token[0]+"\t"+token[1])); //output key
			vals.set(token[2]+"\t"+element[6]);	// output value
					
			context.write(stockName, vals);		
		}
		
	}	
	/**
	 * @author aravindk@buffalo.edu
	 * Count the minimum and maximum dates of every month
	 * with the dates being computed extract their closing balance values and compute monthly return
	 * output: <key, value>, key = FILENAME + YEAR + MONTH , value = Monthly returns
	 * 
	 * */
	public static class Reduce extends Reducer<Text, Text, Text, Text>{

		
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int min=Integer.MAX_VALUE;
			int max=Integer.MIN_VALUE;
			String MaxclosingBalance=null;
			String MinclosingBalance=null;
			for (Text value : values){
				String token[]=(value.toString()).split("\t");	// split the date and closing balance
				
				if(Integer.parseInt(token[0])>max){	// compute maximum date of every month
					max=Integer.parseInt(token[0]);	// store max
					MaxclosingBalance= token[1];	// store corresponding closing balance	
				}
				
				if(Integer.parseInt(token[0])<min){	// compute minimum date of every month
					min=Integer.parseInt(token[0]);	// store min
					MinclosingBalance = token[1];	// store corresponding closing balance
				}
			
			}
			// formula to compute xi and store is at as a double
			double xi= (Double.parseDouble(MaxclosingBalance)-Double.parseDouble(MinclosingBalance))/(Double.parseDouble(MinclosingBalance));
			// now convert it to string
			String closebalance= ""+xi;
			// write the output as KEY= FILENAME+YEAR+MONTH, VALUE = Xi
			context.write(key,new Text(closebalance));
		}

	}
	
	/**
	 * @author aravindk@buffalo.edu
	 * Mapper 2
	 * output: <key, value>, key = FILENAME , value = Xi
	 * 
	 * */
	
public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text stockName = new Text(); // output key
		private Text vals = new Text(); // Output Value
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String line= value.toString();
		  	String token[]= line.split("\t");	// split using tab space
			stockName.set(token[0]);	// set key as Filename and throw everything else
			vals.set(token[3]);			// value as Xi
								
			context.write(stockName, vals);		
		}
		
	}	

	/**
	 * @author aravindk@buffalo.edu
	 * Reducer 2
	 * output: <key, value>, key = FILENAME , value = Volatility
	 * */
	public static class Reduce1 extends Reducer<Text, Text, Text, Text>{
				
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
		
			int count=0;
			ArrayList<Double> lst1 = new ArrayList<Double>();   // create an ArrayList 
					for(Text value:values){
						count++;	// will give us the number of entries for each stock
						lst1.add(Double.parseDouble(value.toString())); // store the xi value
					}
					double tempx = 0;
					double xbar=0;
					double timepass=0;
					double timepass1=0;
					for(int i=0;i<count;i++){
						tempx+=lst1.get(i);	 //Computes the sum of Xi											
					}
					xbar=tempx/count; // Compute Xbar
					for(int i=0;i<count;i++){
						timepass+= Math.pow(lst1.get(i)-xbar,2); //sum(xi-xbar)
					}
					
					timepass1=Math.pow((timepass/(count-1)),0.5); // volatility formula
					context.write(key,new Text(" "+timepass1));  // write key and value
		}
	}
	
	/**
	 * @author aravindk@buffalo.edu
	 * Mapper 3
	 * output: <key, value>, key = null , value = FILENAME+ Volatility
	 * */

public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text stockName = new Text(); // output key
		
		private Text vals = new Text(); // Output Value
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String line= value.toString();
		  				
			vals.set(line);			
			stockName.set("null");		// set key as null
			context.write(stockName,vals);		// write it to output
		}
		
	}	

/**
 * @author aravindk@buffalo.edu
 * Reducer 3
 * output: <key, value>, key = FILENAME , value = Volatility
 *	Returns the Top 10 most and least volatile stock names along with the volatility
 * */

public static class Reduce2 extends Reducer<Text, Text, Text, Text>{

	
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		
		HashMap<String,Double> finalOrder = new HashMap<String,Double>();
		TreeMap<String,Double> sortedMap = new TreeMap<String,Double>(); // to store in sorted order
		for(Text value:values){
			
			String line=value.toString();
			String token[]= line.split("/t");
			String[] GodDamn= token[0].split("\t");
						
			double compute=(Double.parseDouble(GodDamn[1]));
			if(!(Math.abs(compute) < 2 * Double.MIN_VALUE)){	// check for 0 volatility, if so then ignore
				if(!Double.isNaN(Double.parseDouble(GodDamn[1]))){ // check for NaN. if found then ignore
				finalOrder.put(GodDamn[0],Double.parseDouble(GodDamn[1]));	// put it in the Hashmap
					}
				}
			
			}
		sortedMap = SortByValue(finalOrder); // sort the HashMap by value and store it in a TreeMap
		List<String> list = new ArrayList<String>(sortedMap.keySet());	// List to access top 10 and
		List<Double> yeah = new ArrayList<Double>(sortedMap.values());	// last 10 elements
		
		for(int i=0;i<10;i++){
			// Top 10 Elements
			
			context.write(new Text(list.get(i)), new Text(yeah.get(i).toString()));			
		}
		
		for(int i=(list.size()-1);i>(list.size()-11);i--){
			// Bottom 10 Elements
			context.write(new Text(list.get(i)),new Text(yeah.get(i).toString()));
		}
		
	}
}

// Function to sort the HashMap based on the values
// Store the result in treeMap
	public static TreeMap<String, Double> SortByValue 
		(HashMap<String, Double> map) {
		ValueComparator vc =  new ValueComparator(map);
		TreeMap<String,Double> sortedMap = new TreeMap<String,Double>(vc);
		sortedMap.putAll(map);
		return sortedMap;
	}

public static void main(String[] args) {
	
		try {
			// Create a new Job
			
			Job job = Job.getInstance();
			Job job2 = Job.getInstance();
		    Job job3= Job.getInstance();
		     
		    job.setJarByClass(StockVolatility1.class);
		    job2.setJarByClass(StockVolatility1.class);
		    job3.setJarByClass(StockVolatility1.class);
			
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			job2.setMapperClass(Map1.class);
			job2.setReducerClass(Reduce1.class);
			
			job3.setMapperClass(Map2.class);
			job3.setReducerClass(Reduce2.class);
			job.setNumReduceTasks(12);
			job2.setNumReduceTasks(12);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path("Inter_"+args[1]));
			FileInputFormat.addInputPath(job2, new Path("Inter_"+args[1]));
			FileOutputFormat.setOutputPath(job2, new Path("Output_"+args[1]));
			FileInputFormat.addInputPath(job3, new Path("Output_"+args[1]));
			FileOutputFormat.setOutputPath(job3, new Path(args[1]));
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(Text.class);
			
			
			job.waitForCompletion(true);
			job2.waitForCompletion(true);
			job3.waitForCompletion(true);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
