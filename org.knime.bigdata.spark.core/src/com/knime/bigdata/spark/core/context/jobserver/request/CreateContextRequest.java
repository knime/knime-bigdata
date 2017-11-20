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
package com.knime.bigdata.spark.core.context.jobserver.request;

import java.util.ArrayList;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.jobserver.JobserverConstants;
import com.knime.bigdata.spark.core.context.jobserver.rest.RestClient;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;

/**
 * Request to create a new Spark context. The return value of {@link #send()} is true if the context was created
 * successfully, false if the context existed already. The method throws an exception if something else went wrong.
 *
 * @author Bjoern Lohrmann, KNIME.COM
 */
public class CreateContextRequest extends AbstractJobserverRequest<Boolean> {

    private final static NodeLogger LOGGER = NodeLogger.getLogger(CreateContextRequest.class);

    /**
     * @param contextConfig
     * @param restClient // TODO Auto-generated method stub return null;
     *
     */
    public CreateContextRequest(final SparkContextID contextId, final SparkContextConfig contextConfig,
        final RestClient restClient) {
        super(contextId, contextConfig, restClient);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Boolean sendInternal() throws KNIMESparkException {

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
    }

    private String[] getCustomSettings() {
        if (m_config.overrideSparkSettings() && m_config.getCustomSparkSettings() != null
            && !m_config.getCustomSparkSettings().isEmpty()) {
            ArrayList<String> elements = new ArrayList<String>();
            for (String line : m_config.getCustomSparkSettings().split("\n")) {
                line = line.trim();
                for (String e : line.split(": *", 2)) {
                    elements.add(e);
                }
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
