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
 *   Created on Mar 12, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.sparkjobserver.request;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.bigdata.spark.core.sparkjobserver.context.JobserverConstants;
import org.knime.bigdata.spark.core.sparkjobserver.request.ParsedResponse.FailureReason;
import org.knime.bigdata.spark.core.sparkjobserver.rest.RestClient;
import org.knime.core.node.NodeLogger;

import jakarta.ws.rs.core.Response;

/**
 *
 * @author Bjoern Lohrmann, KNIME.COM
 */
public class DestroyContextRequest extends AbstractJobserverRequest<Void> {

    private final static NodeLogger LOGGER = NodeLogger.getLogger(DestroyContextRequest.class);

    /**
     * @param contextConfig
     * @param restClient
     */
    public DestroyContextRequest(final SparkContextID contextId, final JobServerSparkContextConfig contextConfig, final RestClient restClient) {
        super(contextId, contextConfig, restClient);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected Void sendInternal() throws KNIMESparkException {
        Response response = m_client.delete(JobserverConstants.buildContextPath(m_config.getContextName()));

        final ParsedResponse parsedResponse =
                JobserverResponseParser.parseResponse(response.getStatus(), readResponseAsString(response));

        if (parsedResponse.isFailure()) {
            // throws exception if it handles the error
            handleGeneralFailures(parsedResponse);

            if (parsedResponse.getFailureReason() == FailureReason.CONTEXT_NOT_FOUND) {
                LOGGER.warn("Tried to destroy nonexistent Spark context %s.");
            } else {
                // no request specific failures to handle
                throw createUnexpectedResponseException(parsedResponse);
            }
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NodeLogger getLogger() {
        return LOGGER;
    }

}
