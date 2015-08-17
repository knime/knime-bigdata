package com.knime.bigdata.spark.jobserver.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;

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
import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;

/**
 * creates and handles REST requests
 *
 * @author dwk
 *
 */
class WsRsRestClient implements IRestClient {
    private final static NodeLogger LOGGER = NodeLogger.getLogger(WsRsRestClient.class.getName());

    private static final Client client = ClientBuilder.newClient();

    static {
        client.register(MultiPartFeature.class);
    }

    private static WebTarget getTarget(final KNIMESparkContext aContextContainer, final String aPath, final String aQuery, final String aFragment)
        throws URISyntaxException {
        WebTarget target = client.target(new URI("http", null, aContextContainer.getHost(), aContextContainer.getPort(), aPath, aQuery, aFragment));
        return target;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkJobStatus(final Response response, final String jobClassName, final String aJsonParams)
            throws GenericKnimeSparkException {
        final Status s = Status.fromStatusCode(response.getStatus());
        if (s == Status.ACCEPTED || s == Status.OK) {
            return;
        }
        final StringBuilder logmsg = new StringBuilder();
        logmsg.append("Spark job execution failed.");
        logmsg.append("\tJob class: ").append(jobClassName);
        logmsg.append("\tJob config: ").append(aJsonParams);
        logmsg.append("\tStatus code: ").append(s.getStatusCode());
        logmsg.append("\tStatus family: ").append(s.getFamily().toString());
        logmsg.append("\tReason: ").append(s.getReasonPhrase());
        logmsg.append("\t Response:").append(response.toString());
        LOGGER.error(logmsg.toString());
        final String className;
        if (jobClassName != null) {
            final int lastIndexOf = jobClassName.lastIndexOf('.');
            if (lastIndexOf >= 0) {
                className = jobClassName.substring(lastIndexOf + 1);
            } else {
                className = jobClassName;
            }
        } else {
            className = "<unknown>";
        }
        switch (s.getStatusCode()) {
            case 400:
                throw new GenericKnimeSparkException(
                    "Executing Spark job '" + className + "' failed. Reason: " + s.getReasonPhrase()
                    + ". This might be caused by missing or incorrect job parameters."
                    + " For more details see the KNIME log file.");
            case 500:
                throw new GenericKnimeSparkException(
                    "Executing Spark job '" + className + "' failed. Reason: " + s.getReasonPhrase()
                    + ". Possibly because the class file with job info was not uploaded to the server."
                    + " For more details see the KNIME log file.");
            case 503:
                LOGGER.info("Possibly to many parallel jobs. To allow more parallel jobs increase the "
                        + "max-jobs-per-context parameter in the environment.conf file of your Spark jobserver.");
                throw new GenericKnimeSparkException(
                    "Execution Spark job '" + className + "' failed. Reason: " + s.getReasonPhrase()
                    + ". Possibly because of too many parallel Spark jobs. For more details see the KNIME log file.");
            default:
                throw new GenericKnimeSparkException("Executing Spark job '" + className
                    + "' failed. Reason: " + s.getReasonPhrase() + ". Error code: " + s.getStatusCode()
                    + ". For more details see the KNIME log file.");
        }
    }

    /**
     * check the status of the given response
     *
     * @param response response to check
     * @param aErrorMsg error message postfix in case the response is not in one of the expected stati
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
        final StringBuilder logmsg = new StringBuilder();
        logmsg.append("Spark request failed.").append("\t").append(aErrorMsg);
        logmsg.append("\tStatus code: ").append(s.getStatusCode());
        logmsg.append("\tStatus family: ").append(s.getFamily().toString());
        logmsg.append("\tReason: ").append(s.getReasonPhrase());
        logmsg.append("\t Response:").append(response.toString());
        LOGGER.error(logmsg.toString());
        switch (s.getStatusCode()) {
            case 400:
                throw new GenericKnimeSparkException(
                    "Executing Spark request failed. Error: " + aErrorMsg + ". Reason: " + s.getReasonPhrase()
                    + ". This might be caused by missing or incorrect job parameters."
                    + ". For more details see the KNIME log file.");
            case 500:
                throw new GenericKnimeSparkException(
                    "Executing Spark request failed. Error: " + aErrorMsg + ". Reason: " + s.getReasonPhrase()
                    + ". Possibly because the class file with job info was not uploaded to the server."
                    + " For more details see the KNIME log file.");
            case 503:
                LOGGER.info("Possibly to many parallel jobs. To allow more parallel jobs increase the "
                        + "max-jobs-per-context parameter in the environment.conf file of your Spark jobserver.");
                throw new GenericKnimeSparkException(
                    "Execution Spark request failed. Error: " + aErrorMsg + ". Reason: " + s.getReasonPhrase()
                    + ". Possibly because of too many parallel Spark jobs. For more details see the KNIME log file.");
            default:
                throw new GenericKnimeSparkException("Executing Spark request failed. Error: " + aErrorMsg
                    + ". Reason: " + s.getReasonPhrase() + ". Error code: " + s.getStatusCode()
                    + ". For more details see the KNIME log file.");
        }
    }

    /**
     * create the invocation builder for this REST client
     * @param aPath invocation path
     * @param aParams optional parameters
     * @return builder
     * @throws GenericKnimeSparkException
     */
    private Invocation.Builder getInvocationBuilder(final KNIMESparkContext aContextContainer, final String aPath, @Nullable final String[] aParams)
        throws GenericKnimeSparkException {
        WebTarget target;
        try {
            target = getTarget(aContextContainer, aPath, null, null);
        } catch (URISyntaxException e) {
            LOGGER.error(e.getMessage());
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
    public <T> Response post(final KNIMESparkContext aContextContainer, final String aPath, final String[] aArgs, final Entity<T> aEntity) throws GenericKnimeSparkException {
        Invocation.Builder builder = getInvocationBuilder(aContextContainer, aPath, aArgs);
        return builder.post(aEntity);
    }

    @Override
    public Response delete(final KNIMESparkContext aContextContainer, final String aPath) throws GenericKnimeSparkException {
        Invocation.Builder builder = getInvocationBuilder(aContextContainer, aPath, null);
        return builder.buildDelete().invoke();
    }

    /**
     * send the given type of command to the REST server and convert the result to a JSon array
     * @param aType
     * @return JSonArray with result
     * @throws GenericKnimeSparkException
     */
    @Override
    public JsonArray toJSONArray(final KNIMESparkContext aContextContainer, final String aType) throws GenericKnimeSparkException {
        Invocation.Builder builder = getInvocationBuilder(aContextContainer, aType, null);
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
    public JsonObject toJSONObject(final KNIMESparkContext aContextContainer, final String aType) throws GenericKnimeSparkException {
        Invocation.Builder builder = getInvocationBuilder(aContextContainer, aType, null);
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
            LOGGER.error(e.getMessage());
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
