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
 *   Created on May 11, 2016 by oole
 */
package org.knime.bigdata.spark.node.preproc.normalize;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Ole Ostergaard
 */
@SparkClass
public class NormalizeJobOutput extends JobOutput {

    private static final String SCALES = "scales";
    private static final String TRANSLATIONS = "translations";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public NormalizeJobOutput() {}

    /**
     * @param scales
     * @param translations
     */
    public NormalizeJobOutput(final double[] scales, final double[] translations) {
        set(SCALES,scales);
        set(TRANSLATIONS, translations);
    }

    /**
     * @return the normalization scales
     */
    public double[] getScales() {
        return get(SCALES);
    }

    /**
     * @return the normalization transformations
     */
    public double[] getTranslations() {
        return get(TRANSLATIONS);
    }
}
