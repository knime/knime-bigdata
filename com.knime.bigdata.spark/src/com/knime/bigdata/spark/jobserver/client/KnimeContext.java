package com.knime.bigdata.spark.jobserver.client;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.json.JsonArray;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.knime.bigdata.spark.SparkPlugin;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;

/**
 * handles the client side of the job-server in all requests related to contexts
 *
 * We currently use a single context for all requests, it has a configurable prefix and a user name
 * (TODO - we have to support multiple context on multiple servers)
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

    private final static String CONTEXT_NAME = KNIMEConfigContainer.m_config.getString("spark.contextName");
//            + "."+ KNIMEConfigContainer.m_config.getString("spark.userName");
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

        //query server for existing context and re-use if there is one
        //and it is (one of) the current user's context(s)
        final JsonArray contexts = RestClient.toJSONArray(CONTEXTS_PATH);
        if (contexts.size() > 0) {
            for (int i=0; i<contexts.size(); i++) {
                if (contexts.getString(i).equals(getContextName())) {
                    return new KNIMESparkContext(contexts.getString(0));
                }
            }
        }
        return createSparkContext();

    }

    /**
     * @return the context name to use
     */
    private static String getContextName() {
        return CONTEXT_NAME;
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
        JsonArray contexts = RestClient.toJSONArray(CONTEXTS_PATH);
        if (contexts.size() > 0) {
            for (int i=0; i<contexts.size(); i++) {
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
     * @return context name
     * @throws GenericKnimeSparkException
     */
    private static KNIMESparkContext createSparkContext() throws GenericKnimeSparkException {

        //upload jar with our extensions
        final String jobJarPath = SparkPlugin.getDefault().getPluginRootPath()
                + File.separatorChar + "resources" + File.separatorChar + "knimeJobs.jar";
        JobControler.uploadJobJar(jobJarPath);

//        final String contextName =  CONTEXT_PREFIX + (int)(10000 * Math.random());
        final String contextName =  getContextName();

        final int numCpuCores = KNIMEConfigContainer.m_config.getInt("spark.numCPUCores");
        final String memPerNode = KNIMEConfigContainer.m_config.getString("spark.memPerNode");

        // curl command would be:
        // curl -d ""
        // 'xxx.xxx.xxx.xxx:8090/contexts/knime?num-cpu-cores=4&memory-per-node=512m'
        //use this to add specific extensions:
        //"dependent-jar-uris", "file:///path-on-server.jar"
        final Response response = RestClient.post(CONTEXTS_PATH+"/" + contextName,
            new String[]{"num-cpu-cores", ""+numCpuCores, "memory-per-node", memPerNode},
            Entity.text(""));

        // String response = builder.post(Entity.text(entity)entity("",
        // MediaType.APPLICATION_JSON),
        // String.class);
        // we don't care about the response as long as it is "OK"
        RestClient.checkStatus(response, "Error: failed to create context!", Status.OK);

        return new KNIMESparkContext(contextName, numCpuCores, memPerNode);
    }

    /**
     * query the job-server for the status of the given context
     *
     * @param aContextName name of context as returned by createSparkContext
     * @return status
     * @throws GenericKnimeSparkException
     */
    public static JobStatus getSparkContextStatus(final String aContextName) throws GenericKnimeSparkException {
        // curl xxx.xxx.xxx.xxx:8090/contexts
        JsonArray contexts = RestClient.toJSONArray(CONTEXTS_PATH);
        // response texts looks like this: ["c1", "c2", ...]
        for (int i = 0; i < contexts.size(); i++) {
            String info = contexts.getString(i);
            if (aContextName.equals(info.toString())) {
                return JobStatus.OK;
            }
        }
        return JobStatus.GONE;
    }

    /**
     * ask the job-server to destroy the given context
     *
     * @param aContextName name of context as returned by createSparkContext
     * @throws GenericKnimeSparkException
     */
    public static void destroySparkContext(final String aContextName) throws GenericKnimeSparkException {
        // curl -X DELETE xxx.xxx.xxx.xxx:8090/contexts/knime3268
        // we don't care about the response as long as it is "OK"
        // if it were not OK, then an exception would be thrown by the handler
        // client.delete("/contexts/" + contextName, Status.Ok).run;
        LOGGER.log(Level.INFO, "Shutting down context " + aContextName);
        Response response = RestClient.delete(CONTEXTS_PATH+"/" + aContextName);
        // we don't care about the response as long as it is "OK"
        RestClient.checkStatus(response, "Error: failed to destroy context " + aContextName + "!", Status.OK);
    }

    public static void deleteNamedRDD(final String name) {
        // TK_TODO we need to have a method that allows us to delete named rdds in the given context.
        //However the JobControler might not be the right class to do this

    }

}