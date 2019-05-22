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
 *   Created on Apr 26, 2016 by bjoern
 */
package org.knime.bigdata.spark2_4.jobs.prepare.livy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.DataType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.jar.JobJarDescriptor;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.livy.jobapi.LivyPrepareContextJobInput;
import org.knime.bigdata.spark.core.livy.jobapi.LivyPrepareContextJobOutput;
import org.knime.bigdata.spark.core.livy.jobapi.SparkSideStagingArea;
import org.knime.bigdata.spark.core.livy.jobapi.StagingAreaTester;
import org.knime.bigdata.spark2_4.api.NamedObjects;
import org.knime.bigdata.spark2_4.api.SparkJob;
import org.knime.bigdata.spark2_4.api.TypeConverters;
import org.knime.bigdata.spark2_4.jobs.prepare.ValidationUtil;

import scala.Tuple2;

/**
 * Context preparation job used after a Spark context was started using Apache Livy.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class LivyPrepareContextJob implements SparkJob<LivyPrepareContextJobInput, LivyPrepareContextJobOutput> {

    private static final long serialVersionUID = 5767134504557370285L;

    /**
     * {@inheritDoc}
     */
    @Override
    public LivyPrepareContextJobOutput runJob(final SparkContext sparkContext, final LivyPrepareContextJobInput input,
        final NamedObjects namedObjects) throws Exception {

        SparkSideStagingArea.ensureInitialized(input.getStagingArea(), input.stagingAreaIsPath(),
            determineSparkLocalDir(sparkContext), sparkContext.hadoopConfiguration());

        sparkContext.addSparkListener(new KNIMELivySparkListener());

        try {
            JobJarDescriptor jobJarInfo =
                JobJarDescriptor.load(this.getClass().getClassLoader().getResourceAsStream(JobJarDescriptor.FILE_NAME));

            ValidationUtil.validateSparkVersion(sparkContext, input);
            ValidationUtil.validateKNIMEPluginVersion(input, jobJarInfo);
        } catch (IOException e) {
            throw new KNIMESparkException(
                "Spark context was probably not created with KNIME Extension for Apache Spark (or an old version of it). Please destroy and reopen this Spark context or use a different one.",
                e);
        }

        TypeConverters.ensureConvertersInitialized(input.<DataType> getTypeConverters());


        final String testfileName = validateStagingAreaAccess(input.getTestfileName());
        final String sparkWebUI = sparkContext.uiWebUrl().getOrElse(null);
        final Map<String, String> sparkConf =
            Arrays.stream(sparkContext.conf().getAll()).collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
        return new LivyPrepareContextJobOutput(sparkWebUI, sparkConf, testfileName);
    }

    private static String validateStagingAreaAccess(final String testfileName) throws KNIMESparkException {
        try {
            StagingAreaTester.validateTestfileContent(SparkSideStagingArea.SINGLETON_INSTANCE, testfileName);
            return StagingAreaTester.writeTestfileContent(SparkSideStagingArea.SINGLETON_INSTANCE);
        } catch (Exception e) {
            throw new KNIMESparkException("Staging area access from inside Spark failed: " + e.getMessage(), e);
        } finally {
            SparkSideStagingArea.SINGLETON_INSTANCE.deleteSafely(testfileName);
        }
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
