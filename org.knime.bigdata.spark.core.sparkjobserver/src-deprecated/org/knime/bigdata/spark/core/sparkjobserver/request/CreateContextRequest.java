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
package org.knime.bigdata.spark.core.sparkjobserver.request;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.bigdata.spark.core.sparkjobserver.context.JobserverConstants;
import org.knime.bigdata.spark.core.sparkjobserver.rest.RestClient;
import org.knime.core.node.NodeLogger;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;

/**
 * Request to create a new Spark context. The return value of {@link #send()} is true if the context was created
 * successfully, false if the context existed already. The method throws an exception if something else went wrong.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class CreateContextRequest extends AbstractJobserverRequest<Boolean> {

    private final static NodeLogger LOGGER = NodeLogger.getLogger(CreateContextRequest.class);

    /**
     * @param contextId The ID of the Spark context.
     * @param contextConfig
     * @param restClient // TODO Auto-generated method stub return null;
     *
     */
    public CreateContextRequest(final SparkContextID contextId, final JobServerSparkContextConfig contextConfig,
        final RestClient restClient) {
        super(contextId, contextConfig, restClient);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Boolean sendInternal() throws KNIMESparkException {

        try {
            final Response response = m_client.post(JobserverConstants.buildContextPath(m_config.getContextName()),
                getCustomSettings(), Entity.text(""));

            final ParsedResponse parsedResponse =
                    JobserverResponseParser.parseResponse(response.getStatus(), readResponseAsString(response));

                boolean contextWasCreatedSuccessfully = true;

                if (parsedResponse.isFailure()) {
                    contextWasCreatedSuccessfully = false;

                    // throws exception if it handles the error
                    handleGeneralFailures(parsedResponse);

                    // throws exception if it handles the error
                    handleRequestSpecificFailures(parsedResponse);
                }

                return contextWasCreatedSuccessfully;

        } catch (ProcessingException e) {
            if (e.getCause() != null && e.getCause() instanceof SocketTimeoutException) {
                throw new KNIMESparkException(
                    "Timeout while creating Spark context. Please increase the \"Jobserver response timeout\"\n"
                        + "in the \"Connection Settings\" of the \"Create Spark Context\" node.");
            } else {
                throw e;
            }
        }
    }

    private String[] getCustomSettings() {
        if (m_config.useCustomSparkSettings() && !m_config.getCustomSparkSettings().isEmpty()) {

            ArrayList<String> elements = new ArrayList<>();
            for (Entry<String, String> entry : m_config.getCustomSparkSettings().entrySet()) {
                elements.add(entry.getKey());
                elements.add(entry.getValue());
            }

            return elements.toArray(new String[]{});

        } else {
            return new String[]{};
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
            case CONTEXT_NAME_MUST_START_WITH_LETTERS:
                throw new KNIMESparkException("Name of Spark context must start with a letter.");
            case CONTEXT_ALREADY_EXISTS:
                // since the context already exists, only warn about it
                LOGGER.warn(String.format("Not creating Spark context with name %s, because it already exists.",
                    m_config.getContextName()));
                break;
            case CONTEXT_INIT_FAILED:
                String msg = "Failed to initialize Spark context " + KNIMESparkException.SEE_LOG_SNIPPET;
                if (parsedResponse.getThrowable() != null) {
                    msg = String.format("%s %s",
                        parsedResponse.getThrowable().getOriginalMessage(),
                        KNIMESparkException.SEE_LOG_SNIPPET);
                }
                throw new KNIMESparkException(msg, parsedResponse.getThrowable());
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
