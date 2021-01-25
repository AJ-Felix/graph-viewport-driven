package aljoschaRydzyk.viewportDrivenGraphStreaming.Eval;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.nextbreakpoint.flinkclient.api.ApiClient;
import com.nextbreakpoint.flinkclient.api.ApiException;
import com.nextbreakpoint.flinkclient.api.ApiResponse;
import com.nextbreakpoint.flinkclient.api.FlinkApi;
import com.nextbreakpoint.flinkclient.api.Pair;
import com.nextbreakpoint.flinkclient.model.JobDetailsInfo;
import com.squareup.okhttp.Call;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.*;
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.webmonitor.HttpRequestHandler;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;

public class FlinkAPIClient {
	private static String clusterEntryPointAddress;
	private static String jobID;
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

//		Request request = new Request.Builder()
//		                     .url("http://" + clusterEntryPointAddress + ":8081" + 
//		                    		 "/taskmanagers/f62a42bf2284a9bd00fc4380059e18d5/metrics")
//		                     .build();
		
		
		Request request = new Request.Builder()
                .url("http://" + clusterEntryPointAddress + ":8081" + 
               		 "/taskmanagers"
               		 + "/f62a42bf2284a9bd00fc4380059e18d5/"
//						"/jobmanager/"
               		 + "metrics"
               		 + "?get=Status.JVM.CPU.Load"
               		 )
                .build();
		
		while (true) {
			Response response = doRequest(request);
			Thread.sleep(50);
			System.out.println(response.body().string());
		}
		
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
	}
	
	private static void parseInput(String[] args) throws ParseException {
		Options options = new Options();
		options.addOption("c", true, "Address of flink cluster entrypoint");
		options.addOption("j", true, "JobID");
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		clusterEntryPointAddress = cmd.getOptionValue("c");
		jobID = cmd.getOptionValue("j");
	}
	
	private static Response doRequest(Request request) throws IOException {
		return client.newCall(request).execute();
	}
}
