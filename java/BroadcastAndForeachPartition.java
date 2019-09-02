package sparkexamples;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import java.io.*;

import java.util.*;
import org.apache.spark.sql.Encoders;

//
// Demonstrates the application of Broadcast Variables and ForeachPartition in Spark
// At first, two sample ".txt" files are broadcasted to Spark executors and then
// both the files are read line by line from each executor 
//
public class BroadcastAndForeachPartition implements Serializable {

	// Static Variable Declarations
	public static SparkSession spark = null;
	public static String file1Path = "/Users/temp_user/Desktop/EMP_DETAILS_20190824.txt";
	public static String file2Path = "/Users/temp_user/Desktop/EMP_DETAILS_20190825.txt";
	public static ArrayList<String> fileList = new ArrayList<String>();

	// Main Method
	public static void main(String args[]) throws Exception {
		System.out.println("This is Main Method");

		// Add the files to fileList ArrayList<String>
		fileList.add(file1Path);
		fileList.add(file2Path);

		// 1st Step creates Spark Session, broadcasts the files to executor nodes and
		// prints the contents of file
		prepareAndExecuteFiles(fileList);
	}

	// Method to broadcast files from Spark Driver and print each File line by line
	// on executors
	public static void prepareAndExecuteFiles(ArrayList<String> fileList) throws Exception {
		// Running on Driver Node
		System.out.println("Preparing Files for Distributing to Executors");

		spark = SparkSession.builder().appName("File Content Printing").master("local[*]").getOrCreate();

		SparkContext sc = spark.sparkContext();

		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

		Map<String, ArrayList<String>> fileMap = new HashMap<String, ArrayList<String>>();
		if (fileList.size() > 0) {
			for (String file : fileList) {
				fileMap.put(file, fileList);
			}
		} else {
			System.out.println("No Eligible Files, terminating the program");
			System.exit(0);
		}

		Broadcast<Map<String, ArrayList<String>>> broadCastedFiles = jsc.broadcast(fileMap);

		System.out.println("Total Number of Files in current run " + fileList.size());

		// Running on Executor Nodes
		JavaRDD<String> filesRdd = jsc.parallelize(fileList, fileList.size());

		Dataset<String> pipelineDs = spark.sqlContext().createDataset(filesRdd.rdd(), Encoders.STRING());

		System.out.println("Printing pipelineDs");

		pipelineDs.show();

		pipelineDs.foreachPartition(new ForeachPartitionFunction<String>() {

			@Override
			public void call(Iterator<String> iterator) throws Exception {

				Map<String, ArrayList<String>> procToMap = broadCastedFiles.value();

				System.out.println("My BroadCasted Map Size is : " + procToMap.size());

				while (iterator.hasNext()) {
					String FileName = iterator.next();
					System.out.println("This is each row of dataset and to be executed once per table");
					System.out.println("starting file reading for - " + FileName);
					BufferedReader reader;
					try {
						File folder = new File(SparkFiles.getRootDirectory());
						File[] files = folder.listFiles();

						for (File file : files) {
							System.out.println("File Name is " + file.getName());

							try {
								reader = new BufferedReader(new FileReader(file));
								String line = reader.readLine();
								while (line != null) {
									System.out.println(line);
									// read next line
									line = reader.readLine();
								}
								reader.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}

					} catch (Exception e) {
						e.printStackTrace();
					}

				}

			}

		});

	}

}
