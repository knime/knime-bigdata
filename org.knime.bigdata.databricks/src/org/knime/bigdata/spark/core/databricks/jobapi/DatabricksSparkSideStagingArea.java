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
 *   Created on Jul 2, 2018 by bjoern
 */
package org.knime.bigdata.spark.core.databricks.jobapi;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksJobSerializationUtils.StagingAreaAccess;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.util.SparkDistributedTempProvider;

/**
 * Spark-side utility class that delegates staging area access to {@link DatabricksSparkSideNIOStagingArea} or
 * {@link DatabricksSparkSideHadoopStagingArea} depending on the configuration.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SparkClass
public class DatabricksSparkSideStagingArea implements StagingAreaAccess, SparkDistributedTempProvider {

    public static final DatabricksSparkSideStagingArea SINGLETON_INSTANCE = new DatabricksSparkSideStagingArea();

    private volatile DatabricksSparkSideStagingAreaProvider m_stagingArea;

    public static synchronized void ensureInitialized(final String stagingArea, final boolean stagingAreaUseHadoopFS,
        final boolean stagingAreaIsPath, final File localTmpDir, final Configuration hadoopConf)
        throws URISyntaxException {

        if (SINGLETON_INSTANCE.m_stagingArea == null) {
            if (stagingAreaUseHadoopFS) {
                SINGLETON_INSTANCE.m_stagingArea =
                    new DatabricksSparkSideHadoopStagingArea(stagingArea, stagingAreaIsPath, localTmpDir, hadoopConf);
            } else {
                SINGLETON_INSTANCE.m_stagingArea = new DatabricksSparkSideNIOStagingArea(stagingArea, localTmpDir);
            }
        }
    }

    @Override
    public Entry<String, OutputStream> newUploadStream() throws IOException {
        return m_stagingArea.newUploadStream();
    }

    @Override
    public Entry<String, OutputStream> newUploadStream(final String stagingFilename) throws IOException {
        return m_stagingArea.newUploadStream(stagingFilename);
    }

    @Override
    public String uploadAdditionalFile(final File fileToUpload, final String stagingFilename) throws IOException {
        return m_stagingArea.uploadAdditionalFile(fileToUpload, stagingFilename);
    }

    @Override
    public InputStream newDownloadStream(final String stagingFilename) throws IOException {
        return m_stagingArea.newDownloadStream(stagingFilename);
    }

    @Override
    public Path downloadToFile(final InputStream in) throws IOException {
        return m_stagingArea.downloadToFile(in);
    }

    @Override
    public void deleteSafely(final String stagingFilename) {
        m_stagingArea.deleteSafely(stagingFilename);
    }

    public void cleanUp() {
        m_stagingArea.cleanUp();
    }

    @Override
    public URI getDistributedTempDirURI() {
        return m_stagingArea.getDistributedTempDirURI();
    }

}
