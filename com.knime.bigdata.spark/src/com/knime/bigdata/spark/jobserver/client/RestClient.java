package com.knime.bigdata.spark.jobserver.client;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;

/**
 * creates and handles REST requests
 *
 * @author dwk
 *
 */
public class RestClient {

    private static class RestClientFactory {

        static IRestClient getClient() {
            final String host;
            if (!KnimeConfigContainer.m_config.hasPath("spark.jobServer")) {
                host = null;
            } else {
                host = KnimeConfigContainer.m_config.getString("spark.jobServer");
            }
            if (host == null || host.equals("dummy") || host.length() < 2) {
                return new DummyRestClient();
            }
            return new WsRsRestClient();
        }
    }

    private static final IRestClient client = RestClientFactory.getClient();

    /**
     * check the status of the given response
     *
     * @param response response to check
     * @param aErrorMsg error message prefix in case the response is not in one of the expected stati
     * @param aStatus array of expected stati that are OK, all other response stati will cause an exception
     * @throws GenericKnimeSparkException
     */
    public static void checkStatus(final Response response, final String aErrorMsg, final Status... aStatus)
        throws GenericKnimeSparkException {
        client.checkStatus(response, aErrorMsg, aStatus);
    }

    /**
     * post the given request
     * @param aPath
     * @param aArgs
     * @param aEntity
     * @return server response
     * @throws GenericKnimeSparkException
     */
    public static <T>  Response post(final String aPath, final String[] aArgs, final Entity<T> aEntity) throws GenericKnimeSparkException {
        return client.post(aPath, aArgs, aEntity);
    }

    /**
     * post the given delete request
     * @param aPath
     * @return server response
     * @throws GenericKnimeSparkException
     */
    public static Response delete(final String aPath) throws GenericKnimeSparkException {
        return client.delete(aPath);
    }

    /**
     * send the given type of command to the REST server and convert the result to a JSon array
     *
     * @param aType
     * @return JSonArray with result
     * @throws GenericKnimeSparkException
     */
    public static JsonArray toJSONArray(final String aType) throws GenericKnimeSparkException {
        return client.toJSONArray(aType);
    }

    /**
     * send the given type of command to the REST server and convert the result to a JSon object
     *
     * @param aType
     * @return JsonObject with result
     * @throws GenericKnimeSparkException
     */
    public static JsonObject toJSONObject(final String aType) throws GenericKnimeSparkException {
        return client.toJSONObject(aType);
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
    public static String getJSONFieldFromResponse(final Response response, final String aField, final String aSubField)
        throws GenericKnimeSparkException {
        return client.getJSONFieldFromResponse(response, aField, aSubField);
    }
}
