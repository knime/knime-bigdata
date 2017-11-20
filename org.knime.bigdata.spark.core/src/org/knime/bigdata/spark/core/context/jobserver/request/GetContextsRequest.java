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

import javax.json.JsonArray;
import javax.ws.rs.core.Response;

import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.jobserver.JobserverConstants;
import com.knime.bigdata.spark.core.context.jobserver.rest.RestClient;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;

/**
 * Query server for existing context so that we can re-use it if there is one.
 *
 * @author Bjoern Lohrmann, KNIME.COM
 */
public class GetContextsRequest extends AbstractJobserverRequest<JsonArray> {

    private final static NodeLogger LOGGER = NodeLogger.getLogger(GetContextsRequest.class);

    /**
     * @param contextConfig
     * @param restClient
     */
    public GetContextsRequest(final SparkContextID contextId, final SparkContextConfig contextConfig, final RestClient restClient) {
        super(contextId, contextConfig, restClient);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JsonArray sendInternal() throws KNIMESparkException {
        final Response response = m_client.get(JobserverConstants.CONTEXTS_PATH);

        final ParsedResponse parsedResponse =
                JobserverResponseParser.parseResponse(response.getStatus(), readResponseAsString(response));

        if (parsedResponse.isFailure()) {
            // throws exception if it handles the error
            handleGeneralFailures(parsedResponse);

            // no request specific failures to handle
            throw createUnexpectedResponseException(parsedResponse);
        } else {
            return (JsonArray) parsedResponse.getJsonBody();
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
