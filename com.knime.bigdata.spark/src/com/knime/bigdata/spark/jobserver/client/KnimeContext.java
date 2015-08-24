package com.knime.bigdata.spark.jobserver.client;

import java.io.File;
import java.net.SocketException;
import java.util.Collections;
import java.util.Set;

import javax.json.JsonArray;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.SparkPlugin;
import com.knime.bigdata.spark.jobserver.jobs.NamedRDDUtilsJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
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

    private final static NodeLogger LOGGER = NodeLogger.getLogger(KnimeContext.class.getName());

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
        return openSparkContext(new KNIMESparkContext());
    }

    /**
     * @param context the {@link KNIMESparkContext} to open
     * @return the opened {@link KNIMESparkContext}
     * @throws GenericKnimeSparkException if the context could not be created on the Spark Jobsever
     */
    public static KNIMESparkContext openSparkContext(final KNIMESparkContext context) throws GenericKnimeSparkException {
        if (context == null) {
            throw new NullPointerException("context must not be null");
        }
        //query server for existing context and re-use if there is one
        //and it is (one of) the current user's context(s)
        try {
            synchronized (LOGGER) {
                if (sparkContextExists(context)) {
                    return context;
                }
                return createSparkContext(context);
            }
        } catch (ProcessingException e) {
            final StringBuilder buf = new StringBuilder("Could not establish connection to Spark Jobserver.");
            if (e.getCause() != null && e.getCause() instanceof SocketException) {
                buf.append(" Error: '" + e.getCause().getMessage()
                    + "' possible reason jobserver down or incompatible connection settings. "
                    + "For details see logfile View->Open KNIME log.");
                LOGGER.error("Context information: " + context);
            } else {
                buf.append(" Error: " + e.getMessage());
            }
            final String msg = buf.toString();
            LOGGER.error(msg, e);
            throw new GenericKnimeSparkException(msg);
        }
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
     * @param aContextContainer the {@link KNIMESparkContext} to create
     *
     * @return context container
     * @throws GenericKnimeSparkException
     */
    private static KNIMESparkContext createSparkContext(final KNIMESparkContext aContextContainer)
            throws GenericKnimeSparkException {
        //upload jar with our extensions
        final String jobJarPath = getJobJarPath();
        //TODO: Upload the static jobs jar only if not exists
        JobControler.uploadJobJar(aContextContainer, jobJarPath);

        // curl command would be:
        // curl -d ""
        // 'xxx.xxx.xxx.xxx:8090/contexts/knime?num-cpu-cores=4&memory-per-node=512m'
        //use this to add specific extensions:
        //"dependent-jar-uris", "file:///path-on-server.jar"
        final Response response =
            RestClient.post(aContextContainer, CONTEXTS_PATH + "/" + aContextContainer.getContextName(),
                new String[]{"num-cpu-cores", "" + aContextContainer.getNumCpuCores(), "memory-per-node",
                    aContextContainer.getMemPerNode(),
                    //TODO - make this configurable
                    "spark.yarn.executor.memoryOverhead", "1000"}, Entity.text(""));

        // String response = builder.post(Entity.text(entity)entity("",
        // MediaType.APPLICATION_JSON),
        // String.class);
        // we don't care about the response as long as it is "OK"
        RestClient.checkStatus(response, "Failed to create context!", Status.OK);

        return aContextContainer;
    }

    private static String getJobJarPath() {
        if (KNIMEConfigContainer.m_config.hasPath("unitTestMode")) {
            return SparkPlugin.getDefault().getPluginRootPath() + File.separatorChar + ".." + File.separatorChar
                + "com.knime.bigdata.spark" + File.separator + "resources" + File.separatorChar + "knimeJobs.jar";
        }
        return SparkUtil.getJobJarPath();
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
        LOGGER.info("Shutting down context " + aContextContainer.getContextName());
        Response response =
            RestClient.delete(aContextContainer, CONTEXTS_PATH + "/" + aContextContainer.getContextName());
        // we don't care about the response as long as it is "OK"
        RestClient.checkStatus(response,
            "Failed to destroy context " + aContextContainer.getContextName() + "!", Status.OK);
    }

    /**
     * remove reference to named RDD from context
     *
     * @param aContextContainer context configuration container
     * @param aNamedRdd RDD name reference
     */
    public static void deleteNamedRDD(final KNIMESparkContext aContextContainer, final String aNamedRdd) {
        String jsonArgs =
            JsonUtils.asJson(new Object[]{
                ParameterConstants.PARAM_INPUT,
                new String[]{NamedRDDUtilsJob.PARAM_OP, NamedRDDUtilsJob.OP_DELETE,
                    KnimeSparkJob.PARAM_INPUT_TABLE, aNamedRdd}});
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
            LOGGER.warn("Failed to remove reference to named RDD on server.");
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
            JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
                new String[]{NamedRDDUtilsJob.PARAM_OP, NamedRDDUtilsJob.OP_INFO}});
        try {
            String jobId =
                JobControler.startJob(aContextContainer, NamedRDDUtilsJob.class.getCanonicalName(), jsonArgs);
            JobResult res = JobControler.waitForJobAndFetchResult(aContextContainer, jobId, null);
            return res.getTableNames();
        } catch (CanceledExecutionException e) {
            // impossible with null execution context
            return Collections.emptySet();
        } catch (GenericKnimeSparkException e) {
            LOGGER.warn("Failed to query server for set of named RDDs.");
            return Collections.emptySet();
        }
    }

}