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
 *   Created on Oct 22, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark3_5.base;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.apache.spark.SparkContext;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksSparkSideStagingArea;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Context wrapper that initializes the staging area once and keeps spark context and staging area URI for job
 * execution in a execution context / interactive REPL.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class DatabricksSparkREPLContext {
    private final SparkContext m_sparkContext;
    private final DatabricksSparkSideStagingArea m_stagingArea;

    /**
     * Default constructor. See {@link DatabricksSparkJob#createContext(SparkContext, String)}.
     *
     * @param sc spark context to use
     * @param stagingArea URI or path of staging area
     * @param stagingAreaIsPath <code>true</code> if staging area is a path on default Hadoop FS
     * @throws Exception on failures
     */
    protected DatabricksSparkREPLContext(final SparkContext sc, final String stagingArea, final boolean stagingAreaIsPath) throws Exception {
        m_sparkContext = sc;
        final File tmp = determineSparkLocalDir(sc);
        DatabricksSparkSideStagingArea.ensureInitialized(stagingArea, stagingAreaIsPath, tmp, sc.hadoopConfiguration());
        m_stagingArea = DatabricksSparkSideStagingArea.SINGLETON_INSTANCE;
    }

    /**
     * Run a spark job in this context.
     *
     * @param jobId unique job identifier
     * @throws Exception
     */
    public void run(final String jobId) throws Exception {
        new DatabricksSparkJob(jobId).call(m_sparkContext, m_stagingArea);
    }

    private static File determineSparkLocalDir(final SparkContext sparkContext) throws KNIMESparkException {

        final String knimeTmpDirName = "knime-spark-tmp-" + UUID.randomUUID().toString();
        File knimeTmpDir = null;

        final File envVarCandidate = getFirstSuitableLocalDir(System.getenv("SPARK_LOCAL_DIRS"));
        if (envVarCandidate != null) {
            knimeTmpDir = new File(envVarCandidate, knimeTmpDirName);
        }

        final File sparkConfCandidate = getFirstSuitableLocalDir(sparkContext.conf().get("spark.local.dir", "/tmp"));
        if (sparkConfCandidate != null) {
            knimeTmpDir = new File(sparkConfCandidate, knimeTmpDirName);
        }

        if (knimeTmpDir != null) {
            if (!knimeTmpDir.mkdirs()) {
                throw new KNIMESparkException(
                    "Failed to create temp directory on Spark driver at " + knimeTmpDir.getAbsolutePath());
            }
            return knimeTmpDir;
        } else {
            throw new KNIMESparkException("Could not find suitable temp directory on Spark driver.");
        }
    }

    private static File getFirstSuitableLocalDir(final String localDirEnvVars) {
        if (localDirEnvVars != null && localDirEnvVars.length() > 0) {
            for (String localDirCandidate : localDirEnvVars.split(":")) {
                final Path p = Paths.get(localDirCandidate);
                if (Files.isDirectory(p) && Files.isWritable(p)) {
                    return p.toFile();
                }
            }
        }
        return null;
    }

}
