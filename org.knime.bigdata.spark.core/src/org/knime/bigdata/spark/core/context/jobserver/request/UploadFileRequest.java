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
 *   Created on Mar 11, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.context.jobserver.request;

import java.io.File;

import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.jobserver.rest.RestClient;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.core.node.NodeLogger;

/**
 *
 * @author Bjoern Lohrmann, KNIME.COM
 */
public class UploadFileRequest extends AbstractJobserverRequest<String> {

    private final static NodeLogger LOGGER = NodeLogger.getLogger(GetJarsRequest.class);

    private final File m_localFile;

    private final String m_remotePath;

    /**
     * @param contextConfig
     * @param restClient
     * @param localFile
     * @param remotePath
     */
    public UploadFileRequest(final SparkContextID contextId, final SparkContextConfig contextConfig, final RestClient restClient, final File localFile,
        final String remotePath) {
        super(contextId, contextConfig, restClient);

        this.m_localFile = localFile;
        this.m_remotePath = remotePath;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String sendInternal() throws KNIMESparkException {
        Response response = m_client.post(m_remotePath, null, Entity.entity(m_localFile, MediaType.APPLICATION_OCTET_STREAM));

        final ParsedResponse parsedResponse =
                JobserverResponseParser.parseResponse(response.getStatus(), readResponseAsString(response));

        if (parsedResponse.isFailure()) {
            // throws exception if it handles the error
            handleGeneralFailures(parsedResponse);

            // throws exception if it handles the error
            throw createRequestSpecificFailures(parsedResponse);
        } else {
            return handleSuccess(parsedResponse);
        }
    }

    private String handleSuccess(final ParsedResponse parsedResponse) {
        // when uploading to /data we get the server-local filename
        if (parsedResponse.hasJsonObjectBody()) {
            final JsonValue result = ((JsonObject) parsedResponse.getJsonBody()).get("result");

            if (result instanceof JsonObject && ((JsonObject) result).containsKey("filename")) {
                return ((JsonObject) result).getString("filename");
            } else {
                // uploading to /jar only returns ,,result: jar'' in SJS >= 0.7.0
                return null;
            }
        } else {
            // uploading to /jar only returns "OK" in SJS <= 0.6.2
            return null;
        }
    }

    /**
     * @param parsedResponse
     * @throws KNIMESparkException
     */
    private KNIMESparkException createRequestSpecificFailures(final ParsedResponse parsedResponse) {

        switch (parsedResponse.getFailureReason()) {
            case JAR_INVALID:
                return new KNIMESparkException("Spark jobserver rejected jar file to be uploaded: " + m_localFile.getAbsolutePath());
            case DATAFILE_STORING_FAILED:
                return new KNIMESparkException("Spark jobserver refused to store an uploaded file. Details can be found in the server-side Spark jobserver log.");
            default:
                return createUnexpectedResponseException(parsedResponse);
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
