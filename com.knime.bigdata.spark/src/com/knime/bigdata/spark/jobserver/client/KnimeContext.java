package com.knime.bigdata.spark.jobserver.client;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.json.JsonArray;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;

/**
 * handles the client side of the job-server in all requests related to contexts
 *
 * TODO - we currently use a single context for all requests, need to make it at least user specific (and, possibly,
 * terminate context when KNIME is closed)
 *
 * @author dwk
 *
 */
public class KnimeContext {

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
    public static String getSparkContext() throws GenericKnimeSparkException {

        //query server for existing context and re-use if there is one
        //TODO - make sure that this is the current user's context
        JsonArray contexts = RestClient.toJSONArray("/contexts");
        if (contexts.size() > 0) {
            return contexts.getString(0);
        }
        return createSparkContext();

    }

    /**
     * create a new spark context (name prefix can be specified in the application.conf file), the postfix is number
     * between 0 and 10000
     *
     * @return context name
     * @throws GenericKnimeSparkException
     */
    private static String createSparkContext() throws GenericKnimeSparkException {

        //upload jar with our extensions
        JobControler.uploadJobJar(KnimeConfigContainer.m_config.getString("spark.knimeJobJar"));

        //TODO - add user prefix
        final String contextName = KnimeConfigContainer.m_config.getString("spark.contextNamePrefix") + (int)(10000 * Math.random());

        // curl command would be:
        // curl -d ""
        // 'xxx.xxx.xxx.xxx:8090/contexts/knime?num-cpu-cores=4&memory-per-node=512m'
        //TODO - these should be params
        Invocation.Builder builder =
            RestClient.getInvocationBuilder("/contexts/" + contextName, new String[]{"num-cpu-cores", "2",
                "memory-per-node", "512m"});

        //use this to add specific extensions:
        //"dependent-jar-uris", "file:///path-on-server.jar"

        final Response response = builder.post(Entity.text(""));
        // String response = builder.post(Entity.text(entity)entity("",
        // MediaType.APPLICATION_JSON),
        // String.class);
        // we don't care about the response as long as it is "OK"
        RestClient.checkStatus(response, "Error: failed to create context!", Status.OK);

        return contextName;
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
        JsonArray contexts = RestClient.toJSONArray("/contexts");
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
        Invocation.Builder builder = RestClient.getInvocationBuilder("/contexts/" + aContextName, null);

        Response response = builder.buildDelete().invoke();
        // we don't care about the response as long as it is "OK"
        RestClient.checkStatus(response, "Error: failed to destroy context " + aContextName + "!", Status.OK);
    }

}