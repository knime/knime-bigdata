/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
package com.knime.bigdata.spark.core.context.jobserver.request;

import javax.ws.rs.core.Response;

import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.jobserver.JobserverConstants;
import com.knime.bigdata.spark.core.context.jobserver.request.ParsedResponse.FailureReason;
import com.knime.bigdata.spark.core.context.jobserver.rest.RestClient;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;

/**
 *
 * @author Bjoern Lohrmann, KNIME.COM
 */
public class DeleteDataFileRequest extends AbstractJobserverRequest<Void> {

    private final static NodeLogger LOGGER = NodeLogger.getLogger(CreateContextRequest.class);

    private final String m_serverFilePath;

    /**
     * @param contextConfig
     * @param restClient
     */
    public DeleteDataFileRequest(final SparkContextID contextId, final SparkContextConfig contextConfig, final RestClient restClient, final String serverFilePath) {
        super(contextId, contextConfig, restClient);
        this.m_serverFilePath = serverFilePath;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Void sendInternal() throws KNIMESparkException {
        Response response = m_client.delete(JobserverConstants.buildDataPath(m_serverFilePath));

        final ParsedResponse parsedResponse =
                JobserverResponseParser.parseResponse(response.getStatus(), readResponseAsString(response));

        if (parsedResponse.isFailure()) {
            // throws exception if it handles the error
            handleGeneralFailures(parsedResponse);

            if (parsedResponse.getFailureReason() == FailureReason.DATAFILE_DELETION_FAILED) {
                throw new KNIMESparkException("Failed to delete data file on Spark jobserver.");
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
