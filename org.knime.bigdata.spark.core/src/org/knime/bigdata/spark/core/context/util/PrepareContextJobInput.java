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
package org.knime.bigdata.spark.core.context.util;

import java.util.List;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverter;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class PrepareContextJobInput extends JobInput {

    private static final String KEY_TYPE_CONVERTERS = "converters";

    private static final String KEY_KNIME_PLUGIN_VERSION = "pluginVersion";

    private static final String KEY_SPARK_VERSION = "sparkVersion";

    private static final String KEY_JOB_JAR_HASH = "jobJarHash";

    /**
     *
     */
    public PrepareContextJobInput() {
    }

    /**
     * @return the job jar hash for comparison
     */
    public String getJobJarHash() {
        return get(KEY_JOB_JAR_HASH);
    }

    /**
     * @return the Spark version to check for compatibility
     */
    public String getSparkVersion() {
        return get(KEY_SPARK_VERSION);
    }

    /**
     * @return the KNIME Spark plugin version to check for compatibility
     */
    public String getKNIMEPluginVersion() {
        return get(KEY_KNIME_PLUGIN_VERSION);
    }

    /**
     * @return the {@link IntermediateToSparkConverter} to use
     */
    @SuppressWarnings("unchecked")
    public <T> List<IntermediateToSparkConverter<T>> getTypeConverters() {
        return (List<IntermediateToSparkConverter<T>>) get(KEY_TYPE_CONVERTERS);
    }

    /**
     * @param jobJarHash the job jar hash
     * @param sparkVersion the Spark version
     * @param pluginVersion the KNIME Spark plugin version
     * @param typeConverters the {@link IntermediateToSparkConverter} to use
     * @return the {@link PrepareContextJobInput}
     */
    public static PrepareContextJobInput create(final String jobJarHash, final String sparkVersion,
        final String pluginVersion, final List<IntermediateToSparkConverter<?>> typeConverters) {
        PrepareContextJobInput ret = new PrepareContextJobInput();
        ret.set(KEY_JOB_JAR_HASH, jobJarHash);
        ret.set(KEY_SPARK_VERSION, sparkVersion);
        ret.set(KEY_KNIME_PLUGIN_VERSION, pluginVersion);
        ret.set(KEY_TYPE_CONVERTERS, typeConverters);
        return ret;
    }
}
