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

public class GraphToMatrix {

	private static Logger LOGGER = Logger.getLogger(GraphToMatrix.class
			.getName());

	private static class Edge {
		private int from;
		private int to;

		public Edge(int from, int to) {
			this.from = from;
			this.to = to;
		}

		public int getFrom() {
			return from;
		}

		public int getTo() {
			return to;
		}

		public void setFrom(int from) {
			this.from = from;
		}

		public void setTo(int to) {
			this.to = to;
		}

		@Override
		public String toString() {
			return "Edge{from=" + from + ",to=" + to + "}";
		}
	}

	private static class Triple {
		private Integer row;
		private Integer column;
		private Double value;

		public Triple(Integer row, Integer column, Double value) {
			this.row = row;
			this.column = column;
			this.value = value;
		}

		public Integer getRow() {
			return row;
		}

		public void setRow(Integer row) {
			this.row = row;
		}

		public Integer getColumn() {
			return column;
		}

		public void setColumn(Integer column) {
			this.column = column;
		}

		public Double getValue() {
			return value;
		}

		public void setValue(Double value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return "Triple{" + "row=" + row + ", column=" + column + ", value="
					+ value + '}';
		}
	}

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {

			String tokens[] = value.toString().split("\\s+");

			IntWritable from = new IntWritable(Integer.parseInt(tokens[0]));
			IntWritable to = new IntWritable(Integer.parseInt(tokens[1]));

			LOGGER.info("from=" + from.get() + " to=" + to.get());

			output.collect(from, to);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<IntWritable, IntWritable, NullWritable, Text> {

		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			int count = 0;
			List<Edge> edges = new ArrayList<Edge>();
			while (values.hasNext()) {
				IntWritable to = values.next();
				edges.add(new Edge(key.get(), to.get()));
				count++;
			}

			List<Triple> matrix = new ArrayList<Triple>();
			int row = key.get();
			for (int colomn = 1; colomn <= getMatrixSize(); ++colomn) {
				double value;
				if (count != 0) {
					value = (1 - getBeta()) * 1 / getMatrixSize();
					for (Edge edge : edges) {
						if (edge.getFrom() == row && edge.getTo() == colomn) {
							value += 1.0 / count * getBeta();
							break;
						}
					}
				} else {
					value = 1.0 / getMatrixSize();
				}
				matrix.add(new Triple(row, colomn, value));
			}

			for (Triple t : matrix) {
				output.collect(NullWritable.get(), new Text(t.getColumn() + " "
						+ t.getRow() + " " + t.getValue()));
			}

		}
	}

	private static int matrixSize;
	private static double beta = 0.8;

	public static double getBeta() {
		return beta;
	}

	public static int getMatrixSize() {
		return GraphToMatrix.matrixSize;
	}

	public static void setMatrixSize(int matrixSize) {
		GraphToMatrix.matrixSize = matrixSize;
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(GraphToMatrix.class);

		if (args.length != 3 || args[2] == null) {
			System.err
					.println("The \"matrix_size\" (3rd argument) is not defined");
			System.out.println("Usage: " + args[0]
					+ " input_dir output_dir matrix_size");
			return;
		}

		setMatrixSize(Integer.parseInt(args[2]));

		conf.setJobName("graphtomatrix");
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}