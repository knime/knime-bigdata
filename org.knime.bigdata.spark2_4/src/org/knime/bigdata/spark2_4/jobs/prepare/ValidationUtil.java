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
package org.knime.bigdata.spark2_4.jobs.prepare;

import org.apache.spark.SparkContext;
import org.knime.bigdata.spark.core.context.util.PrepareContextJobInput;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.jar.JobJarDescriptor;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Utility methods to perform certain validations after having created a new Spark context.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class ValidationUtil {

    public static void validateKNIMEPluginVersion(final PrepareContextJobInput input, final JobJarDescriptor jobJarInfo)
        throws KNIMESparkException {
        if (!input.getKNIMEPluginVersion().equals(jobJarInfo.getPluginVersion())) {
            throw new KNIMESparkException(String.format(
                "Spark context was created with version %s of the KNIME Extension for Apache Spark, but you are running %s. Please create a new Spark context.",
                jobJarInfo.getPluginVersion(), input.getKNIMEPluginVersion()));
        }
    }

    public static void validateSparkVersion(final SparkContext sparkContext, final PrepareContextJobInput input)
        throws KNIMESparkException {
        if (!sparkContext.version().startsWith(input.getSparkVersion())) {
            throw new KNIMESparkException(
                String.format("Spark version mismatch: Version %s was expected, but the cluster runs %s.",
                    input.getSparkVersion(), sparkContext.version()));
        }
    }

}
