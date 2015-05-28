package com.knime.bigdata.spark.jobserver.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.media.multipart.MultiPartFeature;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;

/**
 * creates and handles REST requests
 *
 * @author dwk
 *
 */
class WsRsRestClient implements IRestClient {
    private final static Logger LOGGER = Logger.getLogger(WsRsRestClient.class.getName());

    // Config: host, port, use https2?, credentials
    private static final String host = KnimeConfigContainer.m_config.getString("spark.jobServer");

    private static final int port = KnimeConfigContainer.m_config.getInt("spark.jobServerPort");

    private static final Client client = ClientBuilder.newClient();

    static {
        client.register(MultiPartFeature.class);
    }

    private static WebTarget getTarget(final String aPath, final String aQuery, final String aFragment)
        throws URISyntaxException {
        WebTarget target = client.target(new URI("http", null, host, port, aPath, aQuery, aFragment));
        return target;
    }

    /**
     * check the status of the given response
     *
     * @param response response to check
     * @param aErrorMsg error message prefix in case the response is not in one of the expected stati
     * @param aStatus array of expected stati that are OK, all other response stati will cause an exception
     * @throws GenericKnimeSparkException
     */
    @Override
    public void checkStatus(final Response response, final String aErrorMsg, final Status... aStatus)
        throws GenericKnimeSparkException {
        Status s = Status.fromStatusCode(response.getStatus());
        for (int i = 0; i < aStatus.length; i++) {
            if (s == aStatus[i]) {
                return;
            }
        }
        final StringBuilder msg = new StringBuilder(aErrorMsg);
        msg.append("\n").append("Status is: ").append(s).append("\nIndication: ").append(response.toString());
        LOGGER.severe(aErrorMsg);
        LOGGER.severe("Status is: " + s);
        LOGGER.severe("Indication: " + response.toString());
        throw new GenericKnimeSparkException(msg.toString());
    }

    /**
     * create the invocation builder for this REST client
     * @param aPath invocation path
     * @param aParams optional parameters
     * @return builder
     * @throws GenericKnimeSparkException
     */
    private Invocation.Builder getInvocationBuilder(final String aPath, @Nullable final String[] aParams)
        throws GenericKnimeSparkException {
        WebTarget target;
        try {
            target = getTarget(aPath, null, null);
        } catch (URISyntaxException e) {
            LOGGER.severe(e.getMessage());
            throw new GenericKnimeSparkException(e);
        }
        if (aParams != null) {
            for (int p = 0; p < aParams.length; p = p + 2) {
                target = target.queryParam(aParams[p], aParams[p + 1]);
            }
        }
        Invocation.Builder builder = target.request();
        return builder;
    }

    @Override
    public <T> Response post(final String aPath, final String[] aArgs, final Entity<T> aEntity) throws GenericKnimeSparkException {
        Invocation.Builder builder = getInvocationBuilder(aPath, aArgs);
        return builder.post(aEntity);
    }

    @Override
    public Response delete(final String aPath) throws GenericKnimeSparkException {
        Invocation.Builder builder = getInvocationBuilder(aPath, null);
        return builder.buildDelete().invoke();
    }

    /**
     * send the given type of command to the REST server and convert the result to a JSon array
     * @param aType
     * @return JSonArray with result
     * @throws GenericKnimeSparkException
     */
    @Override
    public JsonArray toJSONArray(final String aType) throws GenericKnimeSparkException {
        Invocation.Builder builder = getInvocationBuilder(aType, null);
        Response response = builder.get();
        return JsonUtils.toJsonArray(responseToString(response));
    }


    /**
     * send the given type of command to the REST server and convert the result to a JSon object
     * @param aType
     * @return JsonObject with result
     * @throws GenericKnimeSparkException
     */
    @Override
    public JsonObject toJSONObject(final String aType) throws GenericKnimeSparkException {
        Invocation.Builder builder = getInvocationBuilder(aType, null);
        Response response = builder.get();
        return Json.createReader(new StringReader(responseToString(response))).readObject();
    }

    private static String responseToString(final Response response) throws GenericKnimeSparkException {
        InputStream data = (InputStream)response.getEntity();
        try {
            BufferedReader streamReader = new BufferedReader(new InputStreamReader(data, "UTF-8"));

            StringBuilder responseStrBuilder = new StringBuilder();
            String inputStr;
            while ((inputStr = streamReader.readLine()) != null) {
                responseStrBuilder.append(inputStr);
            }
            return responseStrBuilder.toString();
        } catch (IOException e) {
            LOGGER.severe(e.getMessage());
            throw new GenericKnimeSparkException(e);
        }
    }

    /**
     * return the string value of the given field / sub-field combination from the given response
     * @param response
     * @param aField
     * @param aSubField
     * @return String value
     * @throws GenericKnimeSparkException
     */
    @Override
    public String getJSONFieldFromResponse(final Response response, final String aField, final String aSubField)
        throws GenericKnimeSparkException {

        JsonObject jsonObject = Json.createReader(new StringReader(responseToString(response))).readObject();
        JsonObject myResponse = jsonObject.getJsonObject(aField);

        return myResponse.getString(aSubField);
    }
}
