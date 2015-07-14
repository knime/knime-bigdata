package com.knime.bigdata.spark.jobserver.client;

import java.io.StringReader;
import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
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
    public <T> Response post(final KNIMESparkContext aContextContainer, final String aPath, final String[] aArgs, final Entity<T> aEntity)
        throws GenericKnimeSparkException {

        if (aPath.startsWith(KnimeContext.CONTEXTS_PATH)) {
            KNIMEConfigContainer.m_config =
                KNIMEConfigContainer.m_config.withValue(
                    KnimeContext.CONTEXTS_PATH,
                    ConfigValueFactory.fromAnyRef("[\"" + aPath.substring(KnimeContext.CONTEXTS_PATH.length() + 1)
                        + "\"]"));
        }
        if (aPath.startsWith(JobControler.JOBS_PATH)) {
            KNIMEConfigContainer.m_config =
                KNIMEConfigContainer.m_config.withValue(
                    JobControler.JOBS_PATH,
                    ConfigValueFactory.fromAnyRef("{\"result\" : {\"jobId\":\"sldkkjksjEURXBflskf"
                        + System.currentTimeMillis() + "\"}}"));
        }

        return Response.ok().build();
    }

    @Override
    public Response delete(final KNIMESparkContext aContextContainer, final String aPath) throws GenericKnimeSparkException {
        if (aPath.startsWith(KnimeContext.CONTEXTS_PATH + "/")) {
            KNIMEConfigContainer.m_config = KNIMEConfigContainer.m_config.withoutPath(KnimeContext.CONTEXTS_PATH);
        }
        return Response.ok().build();
    }

    @Override
    public JsonArray toJSONArray(final KNIMESparkContext aContextContainer, final String aType) throws GenericKnimeSparkException {
        String val = "[]";
        if (KNIMEConfigContainer.m_config.hasPath(aType)) {
            val = KNIMEConfigContainer.m_config.getString(aType);
        }
        return Json.createReader(new StringReader(val)).readArray();
    }

    @Override
    public JsonObject toJSONObject(final KNIMESparkContext aContextContainer, final String aType) throws GenericKnimeSparkException {
        String val = "{\"result\":\"OK\"}";
        if (KNIMEConfigContainer.m_config.hasPath(aType)) {
            val = KNIMEConfigContainer.m_config.getString(aType);
        }
        return Json.createReader(new StringReader(val)).readObject();
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
        String val = "";
        if (aSubField.equals("jobId")) {
            val = KNIMEConfigContainer.m_config.getString(JobControler.JOBS_PATH);
        }

        JsonObject jsonObject = Json.createReader(new StringReader(val)).readObject();
        JsonObject myResponse = jsonObject.getJsonObject(aField);

        return myResponse.getString(aSubField);
    }
}
