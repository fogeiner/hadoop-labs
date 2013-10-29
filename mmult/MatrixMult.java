
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MatrixMult {

    private static Logger LOGGER = Logger.getLogger(MatrixMult.class.getName());

    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, IntWritable, MapWritable> {

        public void map(LongWritable key, Text value,
                OutputCollector<IntWritable, MapWritable> output,
                Reporter reporter) throws IOException {

            String tokens[] = value.toString().split("\\s+");

            IntWritable i = new IntWritable(Integer.parseInt(tokens[0]));
            IntWritable j = new IntWritable(Integer.parseInt(tokens[1]));
            FloatWritable Aij = new FloatWritable(Float.parseFloat(tokens[2]));

            MapWritable map = new MapWritable();
            map.put(j, Aij);

            LOGGER.info("i=" + i.get() + " j=" + j.get() + " Aij=" + Aij);

            output.collect(i, map);
        }
    }

    public static class Reduce extends MapReduceBase implements
            Reducer<IntWritable, MapWritable, IntWritable, FloatWritable> {

        public void reduce(IntWritable key, Iterator<MapWritable> values,
                OutputCollector<IntWritable, FloatWritable> output,
                Reporter reporter) throws IOException {
            
        	float product = 0;
            List<Float> vector = getVector();
            
            while (values.hasNext()) {
                MapWritable m = values.next();
                
                IntWritable jW = (IntWritable)m.keySet().iterator().next();
                FloatWritable valueW = (FloatWritable)m.get(jW);
                
                int i = key.get();
                int j = jW.get();
                LOGGER.info("i="+i+" j="+j + " value=+"
                		+vector.get(j - 1)+"*"+valueW.get());
                product += vector.get(j - 1) * valueW.get();                
            }

            LOGGER.info("i=" + key.get() + " product=" + product);

            output.collect(key, new FloatWritable(product));
        }
    }

    private static List<Float> vector;

    public static void setVector(List<Float> vector) {
        MatrixMult.vector = vector;
    }

    public static List<Float> getVector() {
        return vector;
    }

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(MatrixMult.class);

        if (args.length != 3 || args[2] == null) {
            System.err.println("The \"vector\" (3rd argument) is not defined");
            System.out.println("Usage: " + args[0] + " input_dir output_dir vector");
            return;
        }

        List<Float> vector = new ArrayList<Float>();
        for (String token : args[2].split(" ")) {
            vector.add(Float.parseFloat(token));
        }
        MatrixMult.setVector(vector);

        conf.setJobName("mmult");

        conf.setInputFormat(TextInputFormat.class);

        // Set the outputs for the Job
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(MapWritable.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(FloatWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        // conf.setCombinerClass(Reduce.class);

        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
