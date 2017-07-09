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
 *   Created on Apr 26, 2016 by bjoern
 */
package com.knime.bigdata.spark1_6.jobs.prepare;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.DataType;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.jar.JobJarDescriptor;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.util.PrepareContextJobInput;
import com.knime.bigdata.spark1_6.api.NamedObjects;
import com.knime.bigdata.spark1_6.api.SimpleSparkJob;
import com.knime.bigdata.spark1_6.api.TypeConverters;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class PrepareContextJob implements SimpleSparkJob<PrepareContextJobInput> {

    private static final long serialVersionUID = 5767134504557370285L;

    /**
     * A regex pattern to extract the major and minor Spark version from a full version string (the Spark version string
     * provided by KNIME).
     */
    private static final Pattern versionPrefixPattern = Pattern.compile("([0-9]+\\.[0-9]+).*");

    /**
     * {@inheritDoc}
     */
    @Override
    public void runJob(final SparkContext sparkContext, final PrepareContextJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        try {
            JobJarDescriptor jobJarInfo =
                JobJarDescriptor.load(this.getClass().getClassLoader().getResourceAsStream(JobJarDescriptor.FILE_NAME));

            // input.getSparkVersion() returns something like 1.6 or 1.6.0.cdh5_9
            // to do at least basic version checking we need to extract the major and minor version components,
            // which is what the regex pattern does
            final Matcher m = versionPrefixPattern.matcher(input.getSparkVersion());
            if (!m.matches()) {
                throw new RuntimeException("Invalid Spark version: " + input.getSparkVersion() + ". This is a bug.");
            }
            final String knimeSparkVersionPrefix = m.group(1);

            if (!sparkContext.version().startsWith(knimeSparkVersionPrefix)) {
                throw new KNIMESparkException(String.format(
                    "Spark version mismatch: KNIME Spark Executor is set to %s, but the cluster runs %s. Please correct the setting under Preferences > KNIME > Spark. Then destroy and reopen the Spark context.",
                    input.getSparkVersion(), sparkContext.version()));
            }

            if (!input.getKNIMEPluginVersion().equals(jobJarInfo.getPluginVersion())) {
                throw new KNIMESparkException(String.format(
                    "Spark context was created by version %s of the KNIME Spark Executor, but you are running %s. Please destroy and reopen this Spark context or use a different one.",
                    jobJarInfo.getPluginVersion(), input.getKNIMEPluginVersion()));
            }

            // FIXME Deactivated hash check, as this was causing trouble with win+lin on the same context.
            //            if (!input.getJobJarHash().equals(jobJarInfo.getHash())) {
            //                throw new KNIMESparkException(
            //                    "Spark context was created by a KNIME Spark Executor that has incompatible community extensions. Please destroy and reopen this Spark context or use a different one.");
            //            }

        } catch (IOException e) {
            throw new KNIMESparkException("Spark context was probably not created with KNIME Spark Executor (or an old version of it).  Please destroy and reopen this Spark context or use a different one.",
                e);
        }

        TypeConverters.ensureConvertersInitialized(input.<DataType>getTypeConverters());
    }
}
