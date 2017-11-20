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
 *   Created on Mar 24, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.context.jobserver.request;

import javax.ws.rs.core.Response;

import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.jobserver.JobserverConstants;
import com.knime.bigdata.spark.core.context.jobserver.rest.RestClient;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;

/**
 *
 * @author bjoern
 */
public class KillJobRequest extends AbstractJobserverRequest<Void>{

    private final static NodeLogger LOGGER = NodeLogger.getLogger(KillJobRequest.class);

    private final String m_jobID;

    /**
     * @param contextConfig
     * @param restClient
     */
    public KillJobRequest(final SparkContextID contextId, final SparkContextConfig contextConfig, final RestClient restClient, final String jobID) {
        super(contextId, contextConfig, restClient, JobserverConstants.MAX_REQUEST_ATTEMTPS);
        this.m_jobID = jobID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Void sendInternal() throws KNIMESparkException {
        Response response = m_client.delete(JobserverConstants.buildJobPath(m_jobID));

        final ParsedResponse parsedResponse =
                JobserverResponseParser.parseResponse(response.getStatus(), readResponseAsString(response));

        if (parsedResponse.isFailure()) {
            // throws exception if it handles the error
            handleGeneralFailures(parsedResponse);

            // throws exception if it handles the error
            handleRequestSpecificFailures(parsedResponse);

            // no request specific failures to handle
            throw createUnexpectedResponseException(parsedResponse);
        }

        return null;
    }

    /**
     * @param parsedResponse
     * @throws RetryableKNIMESparkException
     */
    private void handleRequestSpecificFailures(final ParsedResponse parsedResponse) throws KNIMESparkException {
        switch(parsedResponse.getFailureReason()) {
            case JOB_NOT_FOUND:
                throw new RetryableKNIMESparkException(
                    "Failed to cancel job, because Spark jobserver failed to find the job.");
            case JOB_NOT_RUNNING_ANYMORE:
                // this is not actually an error because the job has finished!
                throw new JobAlreadyFinishedException();
            default:
                break;
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
