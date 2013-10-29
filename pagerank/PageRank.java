import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.*;
import java.util.logging.Logger;

import javax.swing.event.ListSelectionEvent;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.*;

public class PageRank {

	private static JobConf processGraph(Path in, Path out) {
		JobConf job = new JobConf(GraphToMatrix.class);
		job.setJobName("graphtomatrix");

		job.setMapperClass(GraphToMatrix.Map.class);
		job.setReducerClass(GraphToMatrix.Reduce.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		return job;
	}

	private static JobConf processMatrix(Path in, Path out) {

		JobConf job = new JobConf(MatrixMult.class);
		job.setJobName("mmult");

		job.setMapperClass(MatrixMult.Map.class);
		job.setReducerClass(MatrixMult.Reduce.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		return job;
	}

    private static double distance(List<Float> v1, List<Float> v2) {
        if (v1.size() != v2.size()) {
            throw new IllegalArgumentException("Vectors cannot have different lengths");
        }
        double diff = 0;
        for (int i = 0; i < v1.size(); ++i) {
            diff += Math.abs(v1.get(i) - v2.get(i));
        }
        return diff;
    }
    
    private static double eps = 1e-5;
    public static double getEps() {
    	return eps;
    }
	
	private static int matrixSize;

	private static int getMatrixSize() {
		return matrixSize;
	}

	private static void setMatrixSize(int matrixSize) {
		PageRank.matrixSize = matrixSize;
	}

	private static List<Float> generateVector(int size) {
		List<Float> vector = new ArrayList<Float>(size);
		for (int i = 0; i < size; ++i) {
			vector.add(1.0F / size);
		}
		return vector;
	}

	public static void main(String args[]) throws Exception {
		if (args.length != 3) {
			System.out.println("Parameters: input output matrixSize");
			return;
		}
		
		PageRank.setMatrixSize(Integer.parseInt(args[2]));
		GraphToMatrix.setMatrixSize(PageRank.getMatrixSize());

		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Path tempPath = new Path("temp");
				
        JobConf conf = new JobConf(PageRank.class);
        conf.setJobName("pagerank");
        
		JobConf matrixProcess = processGraph(inputPath, tempPath);
        JobClient.runJob(matrixProcess);
		
        List<Float> vector = PageRank.generateVector(getMatrixSize());
        List<Float> newVector = PageRank.generateVector(getMatrixSize());
        
        Configuration configuration = new Configuration();
    	FileSystem fs = outputPath.getFileSystem(configuration);
    	int steps = 0;
    	double distance;
        do {
        	MatrixMult.setVector(vector);
        	JobConf matrixMult = processMatrix(tempPath, outputPath);	
        	fs.delete(outputPath, true);	
        	JobClient.runJob(matrixMult);

        	
        	for (FileStatus status : fs.listStatus(outputPath)) {
        		Path path = status.getPath();
        		if(!path.getName().startsWith("part-")) {
        			continue;
        		}
            
        		BufferedReader br = new BufferedReader(
        				new InputStreamReader(fs.open(path)));
        		String line = br.readLine();
        		while (line != null) {
        				String tokens[] = line.split("\\s+");
        				newVector.set(Integer.parseInt(tokens[0]) - 1, Float.parseFloat(tokens[1]));
        				line=br.readLine();
        		}
        		br.close();
        	}

        
        	distance = distance(vector, newVector);
        	steps++;
        	
        	System.out.println(
        			  "=================================================="
        			+ "\nStep: " + steps 
        			+ "\nPageRank vector: " + vector.toString()
        			+ "\nNew PageRank vector: " + newVector.toString()
        			+ "\nDifference: " + distance
        			+ "\n==================================================");	
        	    	
        	Collections.copy(vector, newVector);
        } while(distance > eps);
	}
}