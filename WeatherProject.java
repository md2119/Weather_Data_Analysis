package org.myorg;

import java.io.IOException;
import java.util.*;
import java.io.*;
import java.lang.*;
import java.text.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.mapred.join.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class WeatherProject {

	private static final NullWritable nullKey = NullWritable.get();


	public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private static String station2, month;
		private static Double temp = 0.0, prcp = 0.0;
		private Text word = new Text();
		private Text output_word = new Text();
		int prcp_len = 0;
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			if(!(line.contains("STN"))) {
				String[] splitline = line.split("\\s+");

				station2 = splitline[0].trim();
				month = splitline[2].trim().substring(4, 6);
				word.set(station2); 
				prcp_len =  splitline[19].trim().length() ;
				if( splitline[19].trim().equals( "99.99" ) ) {
					prcp = 0.0 ;
				}
				else {
					switch( splitline[19].trim().charAt( prcp_len - 1 ) ) {
						case 'A': 
							prcp = Double.parseDouble( splitline[19].trim().substring( 0, prcp_len - 1 ) ) / 6.0 ; 
							break;
						case 'B': case 'E': 
							prcp = Double.parseDouble( splitline[19].trim().substring( 0, prcp_len - 1 ) ) / 12.0 ; 
							break;
						case 'C':
							prcp = Double.parseDouble( splitline[19].trim().substring( 0, prcp_len - 1 ) ) / 18.0 ; 
							break;
						case 'D': case 'F': case 'G':
							prcp = Double.parseDouble( splitline[19].trim().substring( 0, prcp_len - 1 ) ) / 24.0 ; 
							break; 
						case 'H': case 'I':
							prcp = 0.0; 
							break;
						default:
							prcp = 0.0; 
							break;

					}
				}
				output_word.set( month + " " + splitline[3].trim() + " " + prcp );

				output.collect( word, output_word );
			}
		}	
	}

	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		OutputCollector op = null;
		HashMap<String, String> pair = new HashMap<String, String>();
		HashMap<String, String> orig_states = new HashMap<String, String>();
		HashMap<String, Double> unclassified = new HashMap<String, Double>();

		
		private Text final_key = new Text();
		private Text final_value = new Text();
		private Double north = 48.9101 , south = 26.4144, east = -67.3882, west = -124.2813;
		private static String station1, state;
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			Double latitude = 0.0, longitude = 0.0, dist = 0.0;
			int cnt = 1;
			op = output;
			String line = value.toString();
			if(!(line.contains("USAF"))) {
				if( line.contains("\"US\"") ) {//	&& !(line.contains("\"US\",\"\"")) ) {
					line = line.replace("\"\"","\" \"");
					String[] splitline = line.split("[\"]+[,]+[\"]+|^\"|\"$");
					station1 = splitline[1].trim();
					state = splitline[5].trim();
					if( !splitline[6].trim().isEmpty() )
						latitude = Double.parseDouble(splitline[6].trim());
					if( !splitline[7].trim().isEmpty() )
						longitude = Double.parseDouble(splitline[7].trim());
					if (latitude != 0.0 || longitude != 0.0) {
						if ( state.isEmpty() ) {
						if( !( latitude > north || latitude < south )  ) {
							if( longitude > east ) { 
								state = "AT";
								pair.put(station1, state);
							}
							else if( longitude < west ) { 
								state = "PC";
								pair.put(station1, state);
							}
							else {
								unclassified.put(station1, Math.sqrt(Math.pow(latitude,2) + Math.pow(longitude,2)) );
							}
						}
					}
					else {
						dist = Math.sqrt(Math.pow(latitude,2) + Math.pow(longitude,2));
						if( !orig_states.containsKey(state) ) {
							orig_states.put(state, dist + " " + cnt );
						}
						else {
							String val = orig_states.get(state);
							String[] arr = val.split(" ");
							dist = Double.parseDouble(arr[0]) + dist;
							cnt = Integer.parseInt(arr[1]) + 1;
							orig_states.put( state, dist + " " + cnt );
						}
						pair.put(station1, state);
					}
					}
										
				}
				
			}
		}

		public void close() throws IOException {

			String unknown_station;
			Double dist = 0.0, min_dist ;
	
			for( Map.Entry<String, Double> entry : unclassified.entrySet() ) {
                                unknown_station = entry.getKey() ;
                                dist = entry.getValue() ;
				min_dist = 100000.00 ;
				String min_state = "";
				for( Map.Entry<String, String> state_entry : orig_states.entrySet() ) {
					String[] arr = state_entry.getValue().split(" ");
					Double diff = Math.abs( (Double.parseDouble(arr[0])/Double.parseDouble(arr[1])) - dist);
					if (min_dist > diff) {
						min_dist = diff;
						min_state = state_entry.getKey() ;
					}
				}
                                op.collect( new Text( unknown_station ), new Text(min_state) );
                        }

			for( Map.Entry<String, String> st : pair.entrySet() ) {
				final_key.set( st.getKey() ); 
				final_value.set( st.getValue() );

				op.collect( final_key, final_value);
			}
		}

	}

        public static class Reduce extends MapReduceBase implements Reducer <Text, Text, Text, Text > {
                private Text output_value = new Text();
                public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
                        while (values.hasNext()) {
                                output_value.set( values.next() );
                                output.collect( key, output_value );

                        }
                }
        }

	public static class Reduce2 extends MapReduceBase implements Reducer <Text, Text, Text, Text > {
		private Text output_value = new Text();
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				output_value.set( values.next() );
				output.collect( key, output_value );

			}
		}
	}

	public static class CombineValuesMapper extends MapReduceBase implements Mapper<Text, TupleWritable, Text, Text>  { 

		private Text outValue = new Text();
		private Text outKey = new Text();
		private StringBuilder valueBuilder = new StringBuilder();
		private String separator = " ", state, month, temp;
	

		public void map(Text key, TupleWritable value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException  {
			valueBuilder.append(key).append(separator);
			for (Writable writable : value) {
				valueBuilder.append(writable.toString()).append(separator);
			}
			valueBuilder.setLength(valueBuilder.length() - 1);

                        String[] splitline = valueBuilder.toString().split(" ");
                        state = splitline[2].trim();
                        outValue.set( splitline[2] + " " + splitline[3] ); 
                        outKey.set( splitline[4] + "_" + splitline[1] );

			output.collect(outKey, outValue);
			valueBuilder.setLength(0);
		}
	}


	public static class AvgCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text > {
		private Text output_key = new Text();
		private Text output_value = new Text();
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			double sum_temp = 0, cnt = 0, avg_temp = 0, sum_prcp = 0, avg_prcp = 0;

			while (values.hasNext()) {
				String[] splitline = values.next().toString().split(" ");

				sum_temp += Double.parseDouble( splitline[0] ) ;
				sum_prcp += Double.parseDouble( splitline[1] ) ;
				cnt ++ ;
			}
			avg_temp = sum_temp / cnt ;
			avg_prcp = sum_prcp /cnt ;
			String[] word =  key.toString().split("_") ;
			output_key.set(word[0]) ;
			output_value.set(  word[1] + " " + avg_temp + " " + avg_prcp ) ;

			output.collect( output_key, output_value ) ;
		}

	}

        public static class ReadAvg extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
                public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String[] splitline = value.toString().split("\\s+");
			output.collect(new Text(splitline[0]), new Text(splitline[1] + " " + splitline[2] + " " + splitline[3] ));
                }
        }


	public static class Min_Max extends MapReduceBase implements Reducer<Text, Text, Text, Text > {
		double min_diff = Double.MAX_VALUE;
		OutputCollector op = null;
		String final_min_col = "", final_max_col = "";
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			NumberFormat temp_format = new DecimalFormat("###.##");
			NumberFormat prcp_format = new DecimalFormat("###.####");
			op = output;
			double temp, diff, min = 10000.00, max = -100000.00, max_month_prcp = 0.0, min_month_prcp = 0.0; // max = Double.MIN_VALUE
			String max_month = "", min_month = "";
			//String[] keysplit = key.toString().split(" ");
			while (values.hasNext()) {
				String[] split1 = values.next().toString().split(" ");
				temp = Double.parseDouble(split1[1]);
				if(temp > max) {
					max = temp;
					max_month = split1[0];
					max_month_prcp = Double.parseDouble( split1[2] ) ;
				}
				if(temp < min) {
					min = temp;
					min_month = split1[0];
					min_month_prcp = Double.parseDouble( split1[2] ) ;
				}		

			}
			diff = max - min;
			final_min_col = String.valueOf(temp_format.format(min)) + ", " + getMonth(min_month);
			final_max_col = String.valueOf(temp_format.format(max)) + ", " + getMonth(max_month);

			output.collect( key , new Text( final_min_col + "_" + final_max_col + "_" + prcp_format.format(diff) + "_" + prcp_format.format(min_month_prcp) + "_" + prcp_format.format(max_month_prcp) ) );

		}

		private String getMonth(String num_mon) {
			switch(Integer.parseInt(num_mon)) {
				case 1: return "January"; 
				case 2: return "February";
				case 3: return "March";
				case 4: return "April";
				case 5: return "May";
				case 6: return "June";
				case 7: return"July";
				case 8: return "August";
				case 9: return "September";
				case 10: return "October";
				case 11: return "November";
				case 12: return "December";
				default: return null;

			}

		}
	}

        public static class SortMapper extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {
                private Text final_key = new Text();
                private Text final_value = new Text();

                public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
                        String[] splitline = value.toString().split("_");
                        final_key.set( splitline[2] );
                        final_value.set( key + "_" + splitline[0] + "_" + splitline[1] + "_" + splitline[3] + "_" + splitline[4] );
                        output.collect(new DoubleWritable( Double.parseDouble( splitline[2] ) ), final_value );
                }

        }

	public static class SortReducer extends MapReduceBase implements Reducer <DoubleWritable, Text, NullWritable, Text > {
		private Text output_value = new Text();
		private Text final_key = new Text();
		private Text final_value = new Text();
		private int header = 0;

		public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
			while(values.hasNext()) {
				String[] splitline = values.next().toString().split("_");
				if( header == 0 ) {
					output.collect(nullKey, new Text("STATE\tMINIMUM\t\t\tMAXIMUM\t\t\tDIFFERENCE\tMIN_MONTH_PRCP\tMAX_MONTH_PRCP"));
					header = 1;
				}
				final_key.set( " " ); //splitline[0] );
				final_value.set(  splitline[1] + "\t\t" + splitline[2] + "\t\t" + key + "\t\t" + splitline[3] + "\t\t" + splitline[4] );

				output.collect( nullKey, final_value );
			}
		}
	}



	public static void main(String[] args) throws Exception {

		JobConf jobconf1 = new JobConf(WeatherProject.class);
		jobconf1.setJobName("stationwise_aggregate");
		jobconf1.setMapOutputKeyClass(Text.class);
		jobconf1.setMapOutputValueClass(Text.class);
		jobconf1.setOutputKeyClass(Text.class);
		jobconf1.setOutputValueClass(Text.class); 
		jobconf1.setMapperClass(Map1.class);
		jobconf1.setReducerClass(Reduce.class);
		jobconf1.setInputFormat(TextInputFormat.class);
		jobconf1.setOutputFormat(TextOutputFormat.class);
		jobconf1.setNumMapTasks(1);
		FileInputFormat.setInputPaths(jobconf1, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobconf1, new Path(args[1]));
		JobClient.runJob(jobconf1);



		JobConf jobconf2 = new JobConf(WeatherProject.class);
		jobconf2.setJobName("extract_state_station");
		jobconf2.setMapOutputKeyClass(Text.class);
		jobconf2.setMapOutputValueClass(Text.class);
		jobconf2.setOutputKeyClass(Text.class);
		jobconf2.setOutputValueClass(Text.class);
		jobconf2.setMapperClass(Map2.class);
		jobconf2.setReducerClass(Reduce2.class);
		jobconf2.setInputFormat(TextInputFormat.class);
		jobconf2.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(jobconf2, new Path(args[2]));
		FileOutputFormat.setOutputPath(jobconf2, new Path(args[3]));
		JobClient.runJob(jobconf2);

		JobConf jobconf3 = new JobConf(WeatherProject.class); 
		jobconf3.setJobName("MAPSIDE_INNER_JOIN");
		jobconf3.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
		String joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, args[1], args[3]);
		jobconf3.set("mapred.join.expr", joinExpression);
		jobconf3.setMapOutputKeyClass(Text.class);

		jobconf3.setReducerClass(AvgCombiner.class);
                //jobconf3.setReducerClass(Min_Max.class);

		jobconf3.setOutputKeyClass(Text.class);
		jobconf3.setOutputValueClass(Text.class);
		jobconf3.setMapperClass(CombineValuesMapper.class);
		jobconf3.setInputFormat(CompositeInputFormat.class); 
		//FileInputFormat.setInputPaths(jobconf3, new Path(args[1]+","+args[3]));
		FileOutputFormat.setOutputPath(jobconf3, new Path(args[4]));
		JobClient.runJob(jobconf3);

		JobConf jobconf4 = new JobConf(WeatherProject.class);
		jobconf4.setJobName("GENERATE_MINMAX");
		jobconf4.setMapOutputKeyClass(Text.class);
		jobconf4.setMapOutputValueClass(Text.class);
		jobconf4.setOutputKeyClass(Text.class);
		jobconf4.setOutputValueClass(Text.class);
		jobconf4.setMapperClass(ReadAvg.class);
		jobconf4.setReducerClass(Min_Max.class);
		jobconf4.setInputFormat(TextInputFormat.class);
		jobconf4.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(jobconf4, new Path(args[4]));
		FileOutputFormat.setOutputPath(jobconf4, new Path(args[5]));
		JobClient.runJob(jobconf4);

		JobConf jobconf5 = new JobConf(WeatherProject.class);
		jobconf5.setJobName("GENERATE_FINAL_OUTPUT");
		jobconf5.setMapOutputKeyClass(DoubleWritable.class);
		jobconf5.setMapOutputValueClass(Text.class);
		jobconf5.setOutputKeyClass(NullWritable.class);
		jobconf5.setOutputValueClass(Text.class);
		jobconf5.setMapperClass(SortMapper.class);
		jobconf5.setReducerClass(SortReducer.class);
		jobconf5.setInputFormat(TextInputFormat.class);
		jobconf5.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(jobconf5, new Path(args[5]));
		FileOutputFormat.setOutputPath(jobconf5, new Path(args[6]));
		JobClient.runJob(jobconf5);
/*
		Job j1 = new Job(jobconf1);
		Job j2 = new Job(jobconf2);
		Job j3 = new Job(jobconf3);
		Job j4 = new Job(jobconf4);
		Job j5 = new Job(jobconf5);

		JobControl jbcntrl = new JobControl("jbcntrl");

		jbcntrl.addJob(j1);
		jbcntrl.addJob(j2);
		jbcntrl.addJob(j3);
		jbcntrl.addJob(j4);
		jbcntrl.addJob(j5);

		j3.addDependingJob(j1);
		j3.addDependingJob(j2);
		j4.addDependingJob(j3);
		j5.addDependingJob(j4);
  Thread thread = new Thread(jbcntrl);
        thread.start();

        while (!jbcntrl.allFinished()) {
            System.out.println("Still running...");
            Thread.sleep(1000);
        }
	System.exit(0);
//new Thread(myJobControlInstance).start();
*/
	}
}
