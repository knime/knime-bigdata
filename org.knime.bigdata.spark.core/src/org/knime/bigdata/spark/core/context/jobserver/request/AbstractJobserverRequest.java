/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Mar 7, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.context.jobserver.request;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.Response;

import org.apache.log4j.Level;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.jobserver.rest.RestClient;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.exception.SparkContextNotFoundException;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.core.node.NodeLogger;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public abstract class AbstractJobserverRequest<T> {

    protected final SparkContextID m_contextId;

    protected final RestClient m_client;

    protected final SparkContextConfig m_config;

    protected final int m_maxAttempts;

    public AbstractJobserverRequest(final SparkContextID contextId, final SparkContextConfig contextConfig, final RestClient restClient) {
        this(contextId, contextConfig, restClient, 1);
    }

    public AbstractJobserverRequest(final SparkContextID contextId, final SparkContextConfig contextConfig, final RestClient restClient, final int maxAttempts) {
        m_contextId = contextId;
        m_client = restClient;
        m_config = contextConfig;
        m_maxAttempts = maxAttempts;
    }


    public T send() throws KNIMESparkException {
        int currentAttempt = 0;

        while(true) {
            currentAttempt++;

            try {
                return sendInternal();
            } catch (ProcessingException e) {
                // Thrown by Java REST API when something went wrong while sending the request
                // e.g. connection refused
                if (e.getCause() != null) {
                    throw new KNIMESparkException(e.getCause().getMessage(), e);
                } else {
                    throw new KNIMESparkException("Request to Spark Jobserver failed " + KNIMESparkException.SEE_LOG_SNIPPET, e);
                }
            } catch (RetryableKNIMESparkException e) {
                // Thrown by the request class to indicate that an error has been returned by the Jobserver
                // but that it might make sense to repeat the request (up to maxAttempts times).
                if (currentAttempt >= m_maxAttempts) {
                    throw e;
                } else {
                    // wait a bit before retrying the request
                    sleepSafely();
                }
            }
            // not handled here: KNIMESparkException => leads to error being immediately reported

        }

    }

    private void sleepSafely() {
        try {
            // sleep for a random offset when
            Thread.sleep((long) (1000 * Math.random()));
        } catch (InterruptedException e1) {
        }
    }

    protected abstract T sendInternal() throws KNIMESparkException;


    protected static String readResponseAsString(final Response response) throws KNIMESparkException {
        try {
            InputStream responseEntityStream = (InputStream) response.getEntity();
            BufferedReader streamReader = new BufferedReader(new InputStreamReader(responseEntityStream, "UTF-8"));
            StringBuilder responseStrBuilder = new StringBuilder();
            String inputStr;
            while ((inputStr = streamReader.readLine()) != null) {
                responseStrBuilder.append(inputStr);
            }
            return responseStrBuilder.toString();
        } catch (IOException e) {
            throw new KNIMESparkException("I/O error while reading Spark Jobserver response. Possible reason: Network problems " + KNIMESparkException.SEE_LOG_SNIPPET, e);
        }
    }

    protected void handleGeneralFailures(final ParsedResponse parsedResponse) throws KNIMESparkException {
        switch (parsedResponse.getFailureReason()) {
            case UNPARSEABLE_RESPONSE:
            case REDIRECT:
                throw createUnexpectedResponseException(parsedResponse);
            case REQUEST_TIMEOUT:
                throw new KNIMESparkException("Request to Spark Jobserver timed out.");
            case AUTHENTICATION_REQUIRED:
                throw new KNIMESparkException(
                    "Spark Jobserver requires authentication. Please configure credentials in the respective 'Create Spark Context' node.");
            case AUTHENTICATION_FAILED:
                throw new KNIMESparkException(
                    "Spark Jobserver authentication failed. Please correct the credentials in the respective 'Create Spark Context' node.");
            case PROXY_AUTHENTICATION_REQUIRED:
                throw new KNIMESparkException(
                    "You are connecting to Spark Jobserver via a proxy that requires authentication. Please configure proxy credentials in File > "
                        + "Preferences > General > Network Connections.");
            case ENTITY_TOO_LARGE:
                throw new KNIMESparkException(
                    "Request to Spark Jobserver failed, because the uploaded data exceeded the amount allowed by the Spark Jobserver. "
                    + "For instructions to change this please see the installation guide (https://www.knime.org/knime-spark-executor#install).");
            case UNKNOWN:
                logResponseAsError(parsedResponse);
                throw new KNIMESparkException("Error on Spark Jobserver: " + parsedResponse.getCustomErrorMessage());
            case THROWABLE:
                throw new KNIMESparkException(parsedResponse.getThrowable());
            case CONTEXT_NOT_FOUND:
                throw new SparkContextNotFoundException(m_contextId);
            default:
                break;
        }
    }

    protected KNIMESparkException createUnexpectedResponseException(final ParsedResponse parsedResponse) {
        logResponseAsError(parsedResponse);
        return new KNIMESparkException(String.format(
            "Spark Jobserver gave unexpected response %s. Possible reason: Incompatible Jobserver version, malconfigured Spark Jobserver",
            KNIMESparkException.SEE_LOG_SNIPPET));
    }

    protected String formatMessage(final String msg) {
        if (msg == null || msg.length() == 0) {
            return "n/a";
        } else {
            return msg;
        }
    }

    protected void logResponseAsError(final ParsedResponse parsedResponse) {
        final StringBuilder logmsg = new StringBuilder();

        logmsg.append(String.format("HTTP Status code: %d | ", parsedResponse.getHttpResponseStatus()));
        logmsg.append(String.format("Response Body: %s", parsedResponse.getResponseEntity()));
        getLogger().error(logmsg.toString());
    }

    protected abstract NodeLogger getLogger();

    protected int getJobLog4jLevel() {
        return Level.toLevel(m_config.getSparkJobLogLevel()).toInt();
    }
}
