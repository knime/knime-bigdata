package com.knime.bigdata.spark.jobserver.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.typesafe.config.ConfigValueFactory;

/**
 * creates and handles REST requests
 *
 * @author dwk
 *
 */
class DummyRestClient implements IRestClient {
    private final static Logger LOGGER = Logger.getLogger(DummyRestClient.class.getName());

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

    @Override
    public <T> Response post(final String aPath, final String[] aArgs, final Entity<T> aEntity)
        throws GenericKnimeSparkException {

        if (aPath.startsWith(KnimeContext.CONTEXTS_PATH)) {
            KnimeConfigContainer.m_config =
                KnimeConfigContainer.m_config.withValue(
                    KnimeContext.CONTEXTS_PATH,
                    ConfigValueFactory.fromAnyRef("[\"" + aPath.substring(KnimeContext.CONTEXTS_PATH.length() + 1)
                        + "\"]"));
        }
        return Response.ok().build();
    }

    @Override
    public Response delete(final String aPath) throws GenericKnimeSparkException {
        if (aPath.startsWith(KnimeContext.CONTEXTS_PATH + "/")) {
            KnimeConfigContainer.m_config = KnimeConfigContainer.m_config.withoutPath(KnimeContext.CONTEXTS_PATH);
        }
        return Response.ok().build();
    }

    @Override
    public JsonArray toJSONArray(final String aType) throws GenericKnimeSparkException {
        String val = "[]";
        if (KnimeConfigContainer.m_config.hasPath(aType)) {
            val = KnimeConfigContainer.m_config.getString(aType);
        }
        return Json.createReader(new StringReader(val)).readArray();
    }

    @Override
    public JsonObject toJSONObject(final String aType) throws GenericKnimeSparkException {
        String val = "";
        if (KnimeConfigContainer.m_config.hasPath(aType)) {
            val = KnimeConfigContainer.m_config.getString(aType);
        }
        return Json.createReader(new StringReader(val)).readObject();
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
     *
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
