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
 *   Created on 27.08.2015 by dwk
 */
package org.knime.bigdata.spark.core.sparkjobserver.context;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.bigdata.spark.core.sparkjobserver.request.DeleteDataFileRequest;
import org.knime.bigdata.spark.core.sparkjobserver.request.UploadFileRequest;
import org.knime.bigdata.spark.core.sparkjobserver.rest.RestClient;

/**
 * Simple utility that uploads some file to the jobserver and can clean up afterwards
 *
 *
 * @author dwk
 * @author Bjoern Lohrmann, KNIME.com
 */
class UploadUtil {

    private static final Logger LOGGER = Logger.getLogger(UploadUtil.class);

    private final SparkContextID m_contextId;

    private final JobServerSparkContextConfig m_context;

    private final RestClient m_restClient;

    private final boolean m_deleteRemoteFilesDuringCleanup;

    private final List<Path> m_localFiles;

    private final List<Path> m_serverFileNames;

    public UploadUtil(final SparkContextID contextId, final JobServerSparkContextConfig contextConfig, final RestClient restClient, final List<Path> file) {
        this(contextId, contextConfig, restClient, file, false);
    }

    public UploadUtil(final SparkContextID contextId, final JobServerSparkContextConfig contextConfig, final RestClient restClient, final List<Path> file,
        final boolean deleteRemoteFilesDuringCleanup) {

        m_contextId = contextId;
        m_context = contextConfig;
        m_restClient = restClient;
        m_localFiles = file;
        m_deleteRemoteFilesDuringCleanup = deleteRemoteFilesDuringCleanup;
        m_serverFileNames = new LinkedList<>();
    }

    /**
     * Upload local files to jobserver.
     *
     * @throws KNIMESparkException
     */
    public void upload() throws KNIMESparkException {
        try {
            for (Path localFile : m_localFiles) {
                final String serverFileName = new UploadFileRequest(m_contextId, m_context, m_restClient, localFile,
                    JobserverConstants.buildDataPath(localFile.getFileName().toString())).send();
                m_serverFileNames.add(Paths.get(serverFileName));
            }
        } catch (KNIMESparkException e) {
            cleanup();
            throw e;
        }
    }

    /**
     * @return filenames on jobserver
     */
    public List<Path> getServerFileNames() {
        return m_serverFileNames;
    }

    /**
     * @return local files
     */
    public List<Path> getLocalFiles() {
        return m_localFiles;
    }

    /**
     * Delete uploaded files.
     *
     */
    public void cleanup() {
        if (m_deleteRemoteFilesDuringCleanup) {
            for (Path serverFileName : m_serverFileNames) {
                try {
                    new DeleteDataFileRequest(m_contextId, m_context, m_restClient, serverFileName.toString()).send();
                } catch (KNIMESparkException e) {
                    LOGGER.error("Failed to delete previously uploaded file on jobserver: " + serverFileName, e);
                }
            }
        }
    }
}
