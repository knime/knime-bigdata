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

import java.io.IOException;
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
     * Creates a new instance.
     *
     * @param jobJarHash the job jar hash
     * @param sparkVersion the Spark version
     * @param pluginVersion the KNIME Spark plugin version
     * @param typeConverters the {@link IntermediateToSparkConverter} to use
     */
    public PrepareContextJobInput(final String jobJarHash, final String sparkVersion, final String pluginVersion,
        final List<IntermediateToSparkConverter<?>> typeConverters) {
        set(KEY_JOB_JAR_HASH, jobJarHash);
        set(KEY_SPARK_VERSION, sparkVersion);
        set(KEY_KNIME_PLUGIN_VERSION, pluginVersion);
        // we have to base64 encode the converters because in the case of Livy,
        // some initialization is necessary (staging area), before complex objects
        // can be deserialized.
        set(KEY_TYPE_CONVERTERS, Base64SerializationUtils.serializeToBase64(typeConverters));
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
        final String base64 = get(KEY_TYPE_CONVERTERS);
        try {
            return (List<IntermediateToSparkConverter<T>>)Base64SerializationUtils.deserializeFromBase64(base64,
                getClass().getClassLoader());
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static PrepareContextJobInput create() {
        PrepareContextJobInput ret = new PrepareContextJobInput();
        return ret;
    }
}
