import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Integer;
import java.util.StringTokenizer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
public class YellowTaxi extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new YellowTaxi(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/user/lada27/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Title Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);

        jobA.setMapperClass(TaxiMap.class);
        jobA.setReducerClass(TaxiReduce.class);
	    jobA.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(YellowTaxi.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Titles Statistics");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(Text.class);

        jobB.setMapOutputKeyClass(IntWritable.class);
        jobB.setMapOutputValueClass(Text.class);

        jobB.setMapperClass(TaxiStatMap.class);
        jobB.setReducerClass(TaxiStatReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(YellowTaxi.class);
        return jobB.waitForCompletion(true) ? 0 : 1;

        
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }


    public static class TaxiMap extends Mapper<Object, Text, Text, Text> {
      
            String delimiters  = ",";
           public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line [] = value.toString().split(delimiters);
            DateTimeFormatter frmtIn =DateTimeFormatter.ofPattern("yyyy-M-d H:m:s");
            String date1=null;
            String val = line[13];

            if((!line[2].equals(""))&(line[2].contains("-"))&&(line[2].length()>10)){
                 String [] element = line[2].toString().split("-| |:");
                date1=line[2];  
                LocalDateTime toa  = LocalDateTime.parse(date1.toString().trim() , frmtIn);                
                context.write(new Text(toa.getDayOfWeek()+" "+toa.getHour()),new Text(val));
            }
        }
    }

    public static class TaxiReduce extends Reducer<Text, Text, Text, Text> {
        @Override
       public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
            float sum = 0.0f;
            float num = 0.0f;
            for (Text val: values){

                sum += Float.parseFloat(val.toString());
                num +=1.0;
            }
            float wynik = sum/num;
            context.write(key, new Text(Float.toString(wynik)));
        }
    }

    public static class TaxiStatMap extends Mapper<Text, Text, IntWritable, Text> {
           @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String [] elements = key.toString().split(" ");
            context.write(new IntWritable(Integer.parseInt(elements[1])), new Text(elements[0]+" "+value.toString()));

        }
    }

    public static class TaxiStatReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
         public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            float max = 0.0f;
            float tip = 0.0f;
            String maxDay = "";
            String [] line;
            for(Text val: values){
             line = val.toString().split(" ");  
             tip = Float.parseFloat(line[1]);
                if (tip > max){
                max = tip;
                maxDay = line[0];   
                }
        	}

            context.write(key, new Text(maxDay + " "+Float.toString(max)));
        }
    
    }
}

