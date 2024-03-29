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
 *   Created on Mar 14, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.sparkjobserver.request;

import java.util.List;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.bigdata.spark.core.sparkjobserver.context.JobserverConstants;
import org.knime.bigdata.spark.core.sparkjobserver.jobapi.JobserverJobInput;
import org.knime.bigdata.spark.core.sparkjobserver.jobapi.TypesafeConfigSerializationUtils;
import org.knime.bigdata.spark.core.sparkjobserver.rest.RestClient;
import org.knime.core.node.NodeLogger;

import com.typesafe.config.ConfigRenderOptions;

import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;

/**
 *
 * @author Bjoern Lohrmann, KNIME.COM
 */
public class StartJobRequest extends AbstractJobserverRequest<JsonObject> {

    private final static NodeLogger LOGGER = NodeLogger.getLogger(StartJobRequest.class);

    private final String m_jobserverAppName;

    private final String m_jobserverJobClass;

    private final JobserverJobInput m_jsInput;

    private final String m_sparkJobClass;

    private final boolean m_prependUserToContextName;

    /**
     * @param contextId
     * @param contextConfig
     * @param jobserverAppName
     * @param prependUserToContextName
     * @param restClient
     * @param jobserverJobClass
     * @param sparkJobClass
     * @param jsInput Job configuration rendered as HOCON
     * @param inputFilesOnServer
     */
    public StartJobRequest(final SparkContextID contextId,
        final JobServerSparkContextConfig contextConfig,
        final String jobserverAppName,
        final boolean prependUserToContextName,
        final RestClient restClient,
        final String jobserverJobClass,
        final String sparkJobClass,
        final JobserverJobInput jsInput) {

        this(contextId, contextConfig, jobserverAppName, prependUserToContextName, restClient, jobserverJobClass, sparkJobClass, jsInput, null);
    }

    /**
     * @param contextId
     * @param contextConfig
     * @param jobserverAppName
     * @param prependUserToContextName
     * @param restClient
     * @param jobserverJobClass
     * @param sparkJobClass
     * @param jsInput Job configuration rendered as HOCON
     * @param inputFilesOnServer
     */
    public StartJobRequest(final SparkContextID contextId,
        final JobServerSparkContextConfig contextConfig,
        final String jobserverAppName,
        final boolean prependUserToContextName,
        final RestClient restClient,
        final String jobserverJobClass,
        final String sparkJobClass,
        final JobserverJobInput jsInput,
        final List<String> inputFilesOnServer) {

        super(contextId, contextConfig, restClient, JobserverConstants.MAX_REQUEST_ATTEMTPS);
        m_jobserverAppName = jobserverAppName;
        m_prependUserToContextName = prependUserToContextName;
        m_jobserverJobClass = jobserverJobClass;
        m_sparkJobClass = sparkJobClass;
        m_jsInput = jsInput;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JsonObject sendInternal() throws KNIMESparkException {

        final String serializedjsInput = TypesafeConfigSerializationUtils
            .serializeToTypesafeConfig(m_jsInput).root().render(ConfigRenderOptions.concise());

        String contextName = m_config.getContextName();
        if (m_prependUserToContextName) {
            contextName = m_config.getUser() + "~" + contextName;
        }

        final Response response =
            m_client.post(JobserverConstants.JOBS_PATH, new String[]{"appName", m_jobserverAppName, "context",
                contextName, "classPath", m_jobserverJobClass}, Entity.text(serializedjsInput));

        final ParsedResponse parsedResponse =
            JobserverResponseParser.parseResponse(response.getStatus(), readResponseAsString(response));

        if (parsedResponse.isFailure()) {
            // throws exception if it handles the error
            handleGeneralFailures(parsedResponse);

            // throws exception if it handles the error
            handleRequestSpecificFailures(parsedResponse);

            return null; // never reached
        } else {
            return (JsonObject)parsedResponse.getJsonBody();
        }
    }

    /**
     * @param response
     * @param stringResponse
     * @param parsedResponse
     * @throws KNIMESparkException
     */
    private void handleRequestSpecificFailures(final ParsedResponse parsedResponse) throws KNIMESparkException {
        switch (parsedResponse.getFailureReason()) {
            case JOB_NO_FREE_SLOTS_AVAILABLE:
                throw new RetryableKNIMESparkException("Spark job execution failed because no free job slots were available on Spark jobserver.");
            case JOB_VALIDATION_FAILED:
                throw new KNIMESparkException(parsedResponse.getThrowable());
            case JOB_CLASSPATH_NOT_FOUND:
                throw new KNIMESparkException(
                    "Spark job could not be loaded on Spark jobserver. Possible reason: Spark context restarted by someone else. Please reset all preceding nodes and try again.");
            case JOB_APPID_NOT_FOUND:
                throw new KNIMESparkException(
                    "Spark job jar not present on jobserver. Possible reason: Spark jobserver has been restarted by someone else. Please reset all preceding nodes and try again.");
            case JOB_CANNOT_PARSE_CONFIG:
            case JOB_LOADING_FAILED:
            case JOB_TYPE_INVALID:
                if (parsedResponse.getThrowable() != null) {
                    throw new KNIMESparkException("Error when trying to execute Spark job. Please restart the Spark context, reset all preceding nodes and try again.", parsedResponse.getThrowable());
                } else {
                    throw new KNIMESparkException("Error when trying to execute Spark job. Please restart the Spark context, reset all preceding nodes and try again.");                }
            default:
                throw createUnexpectedResponseException(parsedResponse);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NodeLogger getLogger() {
        return LOGGER;
    }
}
