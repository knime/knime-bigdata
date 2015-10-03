package com.knime.bigdata.spark.jobserver.client;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.preferences.KNIMEConfigContainer;

/**
 * creates and handles REST requests
 *
 * @author dwk
 *
 */
public class RestClient {

    private final static NodeLogger LOGGER = NodeLogger.getLogger(RestClient.class.getName());

    private static class RestClientFactory {

        static IRestClient getClient(final String aJobServerURL) {
            if (aJobServerURL == null || aJobServerURL.equals("dummy") || aJobServerURL.length() < 2) {
                LOGGER.debug("Using dummy client");
                return new DummyRestClient();
            }
            return new WsRsRestClient();
        }
    }

    private final IRestClient client;

    /**
     * create a config-specific REST client instance
     * @param aJobServerURL
     */
    public RestClient(final String aJobServerURL) {
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Create RestClient");
            LOGGER.debug("Url: " + aJobServerURL);
        }
        client = RestClientFactory.getClient(aJobServerURL);
    }

    /**
     * check the status of the given response
     *
     * @param response response to check
     * @param jobClassName the name of the {@link KnimeSparkJob} that belongs to the response
     * @param aJsonParams the {@link JobConfig} of the job
     * @throws GenericKnimeSparkException if the status is not ok
     */
    public void checkJobStatus(final Response response, final String jobClassName, final String aJsonParams)
        throws GenericKnimeSparkException {
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Rest checkJobStatus");
            LOGGER.debug("Response: " + response);
            LOGGER.debug("JobClass: " + jobClassName);
            LOGGER.debug("JsonParam: " + aJsonParams);
        }
        client.checkJobStatus(response, jobClassName, aJsonParams);
    }

    /**
     * check the status of the given response
     *
     * @param response response to check
     * @param aErrorMsg error message prefix in case the response is not in one of the expected stati
     * @param aStatus array of expected stati that are OK, all other response stati will cause an exception
     * @throws GenericKnimeSparkException
     */
    public void checkStatus(final Response response, final String aErrorMsg, final Status... aStatus)
        throws GenericKnimeSparkException {
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Rest checkStatus");
            LOGGER.debug("Response: " + response);
            LOGGER.debug("ErrorMsg: " + aErrorMsg);
            LOGGER.debug("Expected stati: " + aStatus);
        }
        client.checkStatus(response, aErrorMsg, aStatus);
    }

    /**
     * post the given request
     *
     * @param aContextContainer context configuration container
     *
     * @param aPath
     * @param aArgs
     * @param aEntity
     * @return server response
     * @throws GenericKnimeSparkException
     */
    public <T> Response post(final KNIMESparkContext aContextContainer, final String aPath,
        final String[] aArgs, final Entity<T> aEntity) throws GenericKnimeSparkException {
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Rest post");
            LOGGER.debug("Context: " + aContextContainer);
            LOGGER.debug("Path: " + aPath);
            LOGGER.debug("Args: " + aArgs);
            LOGGER.debug("Entity: " + aEntity);
        }
        return client.post(aContextContainer, aPath, aArgs, aEntity);
    }

    /**
     * post the given delete request
     *
     * @param aContextContainer context configuration container
     * @param aPath
     * @return server response
     * @throws GenericKnimeSparkException
     */
    public Response delete(final KNIMESparkContext aContextContainer, final String aPath)
        throws GenericKnimeSparkException {
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Rest delete");
            LOGGER.debug("Context: " + aContextContainer);
            LOGGER.debug("Path: " + aPath);
        }
        return client.delete(aContextContainer, aPath);
    }

    /**
     * send the given type of command to the REST server and convert the result to a JSon array
     *
     * @param aContextContainer context configuration container
     * @param aType
     * @return JSonArray with result
     * @throws GenericKnimeSparkException
     */
    public JsonArray toJSONArray(final KNIMESparkContext aContextContainer, final String aType)
        throws GenericKnimeSparkException {
        return client.toJSONArray(aContextContainer, aType);
    }

    /**
     * send the given type of command to the REST server and convert the result to a JSon object
     *
     * @param aContextContainer context configuration container
     * @param aType
     * @return JsonObject with result
     * @throws GenericKnimeSparkException
     */
    public JsonObject toJSONObject(final KNIMESparkContext aContextContainer, final String aType)
        throws GenericKnimeSparkException {
        return client.toJSONObject(aContextContainer, aType);
    }

    /**
     * return the string value of the given field / sub-field combination from the given response
     *
     * @param response
     * @param aField
     * @param aSubField
     * @return String value
     * @throws GenericKnimeSparkException
     */
    public String getJSONFieldFromResponse(final Response response, final String aField, final String aSubField)
        throws GenericKnimeSparkException {
        return client.getJSONFieldFromResponse(response, aField, aSubField);
    }
}
