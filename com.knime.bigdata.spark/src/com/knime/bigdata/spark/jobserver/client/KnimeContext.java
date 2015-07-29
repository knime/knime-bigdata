package com.knime.bigdata.spark.jobserver.client;

import java.util.Collections;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.json.JsonArray;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.knime.core.node.CanceledExecutionException;

import com.knime.bigdata.spark.jobserver.jobs.NamedRDDUtilsJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.util.SparkUtil;

/**
 * handles the client side of the job-server in all requests related to contexts
 *
 * We currently use a single context for all requests, it has a configurable prefix and a user name (TODO - we have to
 * support multiple context on multiple servers)
 *
 * @author dwk
 *
 */
public class KnimeContext {

    /**
     * path prefix for contexts
     */
    public static final String CONTEXTS_PATH = "/contexts";

    private final static Logger LOGGER = Logger.getLogger(KnimeContext.class.getName());

    /**
     * get the current spark context (name prefix can be specified in the application.conf file), the postfix is number
     * between 0 and 10000
     *
     * if possible, then an existing context is re-used to the extend that the server is queried for contexts and if
     * there is one already running, then it is re-used
     *
     * @return context name
     * @throws GenericKnimeSparkException
     */
    public static KNIMESparkContext getSparkContext() throws GenericKnimeSparkException {

        final KNIMESparkContext defaultContext = new KNIMESparkContext();
        //query server for existing context and re-use if there is one
        //and it is (one of) the current user's context(s)
        final JsonArray contexts = RestClient.toJSONArray(defaultContext, CONTEXTS_PATH);
        if (contexts.size() > 0) {
            for (int i = 0; i < contexts.size(); i++) {
                if (contexts.getString(i).equals(KNIMEConfigContainer.CONTEXT_NAME)) {
                    return defaultContext;
                }
            }
        }
        return createSparkContext();

    }

    /**
     * @param context the unique name of the context to check
     * @return <code>true</code> if the context exists
     * @throws GenericKnimeSparkException
     */
    public static boolean sparkContextExists(final KNIMESparkContext context) throws GenericKnimeSparkException {
        if (context == null) {
            throw new IllegalArgumentException("context must not be empty");
        }
        //query server for existing context so that we can re-use it if there is one
        JsonArray contexts = RestClient.toJSONArray(context, CONTEXTS_PATH);
        if (contexts.size() > 0) {
            for (int i = 0; i < contexts.size(); i++) {
                if (context.getContextName().equals(contexts.getString(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * create a new spark context (name prefix can be specified in the application.conf file), the postfix is number
     * between 0 and 10000
     *
     * @return context container
     * @throws GenericKnimeSparkException
     */
    private static KNIMESparkContext createSparkContext() throws GenericKnimeSparkException {

        KNIMESparkContext contextContainer = new KNIMESparkContext();

        //upload jar with our extensions
        final String jobJarPath = SparkUtil.getJobJarPath();
        //TODO: Upload the static jobs jar only if not exists
        JobControler.uploadJobJar(contextContainer, jobJarPath);

        // curl command would be:
        // curl -d ""
        // 'xxx.xxx.xxx.xxx:8090/contexts/knime?num-cpu-cores=4&memory-per-node=512m'
        //use this to add specific extensions:
        //"dependent-jar-uris", "file:///path-on-server.jar"
        final Response response =
            RestClient.post(contextContainer, CONTEXTS_PATH + "/" + contextContainer.getContextName(),
                new String[]{"num-cpu-cores", "" + contextContainer.getNumCpuCores(), "memory-per-node",
                    contextContainer.getMemPerNode(),
                    //TODO - make this configurable
                    "spark.yarn.executor.memoryOverhead", "1000"}, Entity.text(""));

        // String response = builder.post(Entity.text(entity)entity("",
        // MediaType.APPLICATION_JSON),
        // String.class);
        // we don't care about the response as long as it is "OK"
        RestClient.checkStatus(response, "Error: failed to create context!", Status.OK);

        return contextContainer;
    }

    // "WARN yarn.YarnAllocationHandler: Container killed by YARN for exceeding memory limits. 2.1 GB of 2.1 GB virtual memory used. Consider boosting spark.yarn.executor.memoryOverhead.
    /**
     * query the job-server for the status of the given context
     *
     * @param aContextContainer context configuration container
     * @return status
     * @throws GenericKnimeSparkException
     */
    public static JobStatus getSparkContextStatus(final KNIMESparkContext aContextContainer)
        throws GenericKnimeSparkException {
        // curl xxx.xxx.xxx.xxx:8090/contexts
        JsonArray contexts = RestClient.toJSONArray(aContextContainer, CONTEXTS_PATH);
        // response texts looks like this: ["c1", "c2", ...]
        for (int i = 0; i < contexts.size(); i++) {
            String info = contexts.getString(i);
            if (aContextContainer.getContextName().equals(info.toString())) {
                return JobStatus.OK;
            }
        }
        return JobStatus.GONE;
    }

    /**
     * ask the job-server to destroy the given context
     *
     * @param aContextContainer context configuration container
     * @throws GenericKnimeSparkException
     */
    public static void destroySparkContext(final KNIMESparkContext aContextContainer) throws GenericKnimeSparkException {
        // curl -X DELETE xxx.xxx.xxx.xxx:8090/contexts/knime3268
        // we don't care about the response as long as it is "OK"
        // if it were not OK, then an exception would be thrown by the handler
        // client.delete("/contexts/" + contextName, Status.Ok).run;
        LOGGER.log(Level.INFO, "Shutting down context " + aContextContainer.getContextName());
        Response response =
            RestClient.delete(aContextContainer, CONTEXTS_PATH + "/" + aContextContainer.getContextName());
        // we don't care about the response as long as it is "OK"
        RestClient.checkStatus(response,
            "Error: failed to destroy context " + aContextContainer.getContextName() + "!", Status.OK);
    }

    /**
     * remove reference to named RDD from context
     *
     * @param aContextContainer context configuration container
     * @param aNamedRdd RDD name reference
     */
    public static void deleteNamedRDD(final KNIMESparkContext aContextContainer, final String aNamedRdd) {
        String jsonArgs =
            JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
                new String[]{ParameterConstants.PARAM_STRING, NamedRDDUtilsJob.OP_DELETE, ParameterConstants.PARAM_TABLE_1, aNamedRdd}});
        try {
            String jobId =
                JobControler.startJob(aContextContainer, NamedRDDUtilsJob.class.getCanonicalName(), jsonArgs);
            JobControler.waitForJobAndFetchResult(aContextContainer, jobId, null);
            //just for testing:
//            Set<String> names = listNamedRDDs(aContextContainer);
//            int ix = 1;
//            for (String name : names){
//                LOGGER.info("Active named RDD "+(ix++)+" of "+names.size()+": "+name);
//            }
        } catch (CanceledExecutionException e) {
            // impossible with null execution context
        } catch (GenericKnimeSparkException e) {
            LOGGER.warning("Failed to remove reference to named RDD on server.");
        }
    }

    /**
     * query given context for names of currently referenced named RDDs
     *
     * @param aContextContainer context configuration container
     * @return Set of named RDD names
     */
    public static Set<String> listNamedRDDs(final KNIMESparkContext aContextContainer) {
        String jsonArgs =
            JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, new String[]{ParameterConstants.PARAM_STRING, NamedRDDUtilsJob.OP_INFO}});
        try {
            String jobId =
                JobControler.startJob(aContextContainer, NamedRDDUtilsJob.class.getCanonicalName(), jsonArgs);
            JobResult res = JobControler.waitForJobAndFetchResult(aContextContainer, jobId, null);
            return res.getTableNames();
        } catch (CanceledExecutionException e) {
            // impossible with null execution context
            return Collections.emptySet();
        } catch (GenericKnimeSparkException e) {
            LOGGER.warning("Failed to query server for set of named RDDs.");
            return Collections.emptySet();
        }
    }

}