package Temporary;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;

@Path("")
public class RequestHandler {
	  private static ExecutionEnvironment env;
	  private static GradoopFlinkConfig graflink_cfg;
	  private static GradoopHBaseConfig gra_hbase_cfg;
	  private static Configuration hbase_cfg;
	  private static EnvironmentSettings fsSettings;
	  private static StreamExecutionEnvironment fsEnv;
	  private static StreamTableEnvironment fsTableEnv;

	  public static void configureFlink() {
	    env = ExecutionEnvironment.getExecutionEnvironment();
	    graflink_cfg = GradoopFlinkConfig.createConfig(env);
		gra_hbase_cfg = GradoopHBaseConfig.getDefaultConfig();
		hbase_cfg = HBaseConfiguration.create();
		fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
	    System.out.println("RequestHandler is finished configuring!");
	  }
	  
	  @GET
	  @Path("testFunction")
	  @Produces(MediaType.TEXT_PLAIN + ";charset=UTF-8")
	  public Response getIt() {
		  String responseString = "this is a string from inside RequestHandler.java";
		  return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
	  }

}
