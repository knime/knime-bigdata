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
package org.knime.bigdata.spark1_3.jobs.prepare;

import java.io.IOException;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.DataType;
import org.knime.bigdata.spark.core.context.util.PrepareContextJobInput;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.jar.JobJarDescriptor;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark1_3.api.NamedObjects;
import org.knime.bigdata.spark1_3.api.SimpleSparkJob;
import org.knime.bigdata.spark1_3.api.TypeConverters;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class PrepareContextJob implements SimpleSparkJob<PrepareContextJobInput> {

    private static final long serialVersionUID = 5767134504557370285L;

    /**
     * {@inheritDoc}
     */
    @Override
    public void runJob(final SparkContext sparkContext, final PrepareContextJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        try {
            JobJarDescriptor jobJarInfo =
                JobJarDescriptor.load(this.getClass().getClassLoader().getResourceAsStream(JobJarDescriptor.FILE_NAME));

            if (!sparkContext.version().startsWith(input.getSparkVersion())) {
                throw new KNIMESparkException(String.format(
                    "Spark version mismatch: KNIME Extension for Apache Spark is set to %s, but the cluster runs %s. Please correct the setting under Preferences > KNIME > Spark. Then destroy and reopen the Spark context.",
                    input.getSparkVersion(), sparkContext.version()));
            }

            if (!input.getKNIMEPluginVersion().equals(jobJarInfo.getPluginVersion())) {
                throw new KNIMESparkException(String.format(
                    "Spark context was created by version %s of the KNIME Extension for Apache Spark, but you are running %s. Please destroy and reopen this Spark context or use a different one.",
                    jobJarInfo.getPluginVersion(), input.getKNIMEPluginVersion()));
            }

            // FIXME Deactivated hash check, as this was causing trouble with win+lin on the same context.
            //            if (!input.getJobJarHash().equals(jobJarInfo.getHash())) {
            //                throw new KNIMESparkException(
            //                    "Spark context was created by a KNIME Extension for Apache Spark that has incompatible community extensions. Please destroy and reopen this Spark context or use a different one.");
            //            }

        } catch (IOException e) {
            throw new KNIMESparkException("Spark context was probably not created with KNIME Extension for Apache Spark (or an old version of it).  Please destroy and reopen this Spark context or use a different one.",
                e);
        }

        TypeConverters.ensureConvertersInitialized(input.<DataType>getTypeConverters());
    }
}
