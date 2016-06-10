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
 *   Created on Mar 23, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.context.jobserver.request;

import javax.json.JsonObject;
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
 * @author Bjoern Lohrmann, KNIME.com
 */
public class GetJobStatusRequest extends AbstractJobserverRequest<JsonObject> {

    private final static NodeLogger LOGGER = NodeLogger.getLogger(GetJarsRequest.class);

    private final String jobID;

    /**
     * @param contextConfig
     * @param restClient
     * @param jobID1
     */
    public GetJobStatusRequest(final SparkContextID contextId, final SparkContextConfig contextConfig, final RestClient restClient, final String jobID1) {
        super(contextId, contextConfig, restClient, JobserverConstants.MAX_REQUEST_ATTEMTPS);
        this.jobID = jobID1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JsonObject sendInternal() throws KNIMESparkException {
        final Response response = m_client.get(JobserverConstants.buildJobPath(jobID));

        final ParsedResponse parsedResponse =
                JobserverResponseParser.parseResponse(response.getStatus(), readResponseAsString(response));

        if (parsedResponse.isFailure()) {
            // throws exception if it handles the error
            handleGeneralFailures(parsedResponse);

            if (parsedResponse.getFailureReason() == FailureReason.JOB_NOT_FOUND) {
                throw new RetryableKNIMESparkException(
                    "Spark jobserver reported an error (it failed to find a previously started job). "
                    + "Please reset and rerun the workflow.");
            }

            throw createUnexpectedResponseException(parsedResponse);
        } else {
            return (JsonObject) parsedResponse.getJsonBody();
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
