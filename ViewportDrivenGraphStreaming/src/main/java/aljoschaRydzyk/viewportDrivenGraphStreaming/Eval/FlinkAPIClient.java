package aljoschaRydzyk.viewportDrivenGraphStreaming.Eval;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

public class FlinkAPIClient {
	private static String clusterEntryPointAddress;
//	private static String jobID;
	private static OkHttpClient client;
	
	public static void main(final String[] args) throws Exception {

		PrintStream fileOut = null;
		try {
			fileOut = new PrintStream("/home/aljoscha/eval_out.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		System.setOut(fileOut);
		
		//parse command line
		try {
			parseInput(args);
		} catch (ParseException e) {
			e.printStackTrace();
		}	
		
		client = new OkHttpClient();
		
		List<Request> requests = new ArrayList<Request>();
		
		Request jobmanagerHeapRequest = new Request.Builder()
                .url("http://" + clusterEntryPointAddress + ":8081" 
		          		+ "/jobmanager"
                		+ "/metrics"
                		+ "?get=Status.JVM.Memory.Heap.Used"
               		 )
                .build();
		requests.add(jobmanagerHeapRequest);
		
		Request taskmanagerHeapRequest = new Request.Builder()
                .url("http://" + clusterEntryPointAddress + ":8081" 
		          		+ "/taskmanagers"
                		+ "/metrics"
                		+ "?get=Status.JVM.Memory.Heap.Used"
               		 )
                .build();
		requests.add(taskmanagerHeapRequest);
		
		Request jobOverviewRequest = new Request.Builder()
                .url("http://" + clusterEntryPointAddress + ":8081" 
		          		+ "/jobs"
                		+ "/overview"
               		 )
                .build();
		requests.add(jobOverviewRequest);

		while(true) {
			for (Request request: requests) {
				Response response = doRequest(request);
				String jsonResponse = response.body().string();
				System.out.println("Request: " + request);
				System.out.println("Response: " + jsonResponse);
			}	
			System.out.println(System.currentTimeMillis());
			Thread.sleep(60000);
		}
	}
		
//		Request request = new Request.Builder()
//		                     .url("http://" + clusterEntryPointAddress + ":8081" + 
//		                    		 "/taskmanagers/f62a42bf2284a9bd00fc4380059e18d5/metrics")
//		                     .build();
		
		
//		Request request = new Request.Builder()
//                .url("http://" + clusterEntryPointAddress + ":8081" + 
////               		 "/taskmanagers"
////               		 + "/5a5b1b8449539a1837792e9bcde0363f"
////               		 + "/thread-dump"
//						"/jobmanager"
//               		 + "/metrics"
////               		 + "?get=Status.Flink.Memory.Managed.Used"
//               		 )
//                .build();
		
//		Request request = new Request.Builder()
//                .url("http://" + clusterEntryPointAddress + ":8081" + 
//                		"/jobs/overview"
////                		+ "/49d09bfb08cea1c9535d11d4fed32110"
////                		+ "/metrics"
////                		+ "vertices/5820e929907c7e7cf4555451bf2a6cc3"
//               		 )
//                .build();
//		
		
//		while (true) {
//			
//			Response memoryResponse = client.newCall(
//					new Request.Builder().url("http://" + clusterEntryPointAddress + ":8081"
//							+ "/taskmanagers/9c3715361da688b7bddb942beeeb4e45"
//							+ "/metrics?get=Status.JVM.Memory.Heap.Used").build()).execute();
//			System.out.println(memoryResponse.body().string());

			
//			Response response = doRequest(request);
//			String jsonResponse = response.body().string();
////			System.out.println(jsonResponse);
//			
//			List<String> jobIDs = new ArrayList<String>();
//			JSONObject json = new JSONObject(jsonResponse);
//			Iterator<Object> iter = json.getJSONArray("jobs").iterator();
//			while(iter.hasNext()) {
//				JSONObject job = (JSONObject) iter.next();
//				String jobName = (String) job.get("name");
//				String jobId = (String) job.get("jid");
//				Integer duration = (Integer) job.get("duration");
//				System.out.println("Jobname: " + jobName + ", jobID: " + jobId + ", duration: " + duration);
//				Response durationResponse = client.newCall(
//						new Request.Builder().url("http://" + clusterEntryPointAddress + ":8081"
//								+ "/jobs/" + 
//								+ "/metrics?get=Status.JVM.Memory.Heap.Used").build()).execute();
//				if (jobName.equals("buildTopView")){
//					Response memoryResponse = client.newCall(
//							new Request.Builder().url("http://" + clusterEntryPointAddress + ":8081"
//									+ "/taskmanagers/12b8768952c2dd68ea252cd354e11db8"
//									+ "/metrics?get=Status.JVM.Memory.Heap.Used").build()).execute();
////					String memory = ((JSONObject) new JSONArray(memoryResponse.body().string()).get(0)).getString("value");
////					System.out.println(jobName + ": " + job.get("state") + ": " + memory);
//					System.out.println(jobName + ": " + job.get("state") + ": " + memoryResponse.body().string());
//				
//				System.out.println(jobName + ": " + job.get("state"));
//				jobIDs.add((String) job.get("jid"));
//			}
//			
//			Thread.sleep(500);
//		}
		
		//retrieve job metrics
//		try {
//			ApiClient client = api.getApiClient();
//			Call jobManagerMetricsCall = client.buildCall("/jobmanager/metrics", "GET", 
//					new ArrayList<Object>(), , , , , 
//					, );
//			new Pair(clusterEntryPointAddress, clusterEntryPointAddress);
//			client.bui
//			ApiResponse<Object> response = client.execute(jobManagerMetricsCall);
//			response.getData().toString();
//			JobDetailsInfo details = api.getJobDetails(jobID);
//			Long duration = details.getDuration();
//		} catch (ApiException e) {
//			e.printStackTrace();
//		}
//	}
	
	private static void parseInput(String[] args) throws Exception {
		Options options = new Options();
		options.addOption("c", true, "Address of flink cluster entrypoint");
//		options.addOption("j", false, "JobID");
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		clusterEntryPointAddress = cmd.getOptionValue("c");
		if (clusterEntryPointAddress == null) throw new NullPointerException("Please provide cluster entry point at command line with -c!");
		System.out.println("Cluster is located on: " + clusterEntryPointAddress);
//		jobID = cmd.getOptionValue("j");
	}
	
	private static Response doRequest(Request request) throws IOException {
		return client.newCall(request).execute();
	}
}
