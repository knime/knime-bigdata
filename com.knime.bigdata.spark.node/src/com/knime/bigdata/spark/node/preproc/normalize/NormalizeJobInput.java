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
 *   Created on May 10, 2016 by oole
 */
package com.knime.bigdata.spark.node.preproc.normalize;

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Ole Ostergaard
 */
@SparkClass
public class NormalizeJobInput extends JobInput {

    private final static String INCLUDE_COL_IDXS = "includeColIdxs";
    private final static String NORMALIZATION_SETTINGS = "normalizationSettings";

    /**
     * Pramless constructor for automatic deserialization.
     */
    public NormalizeJobInput () {}

    NormalizeJobInput (final String namedInputObject, final String namedOutputObject, final Integer[] includeColIdxs, final NormalizationSettings normalizationSettings) {
        addNamedInputObject(namedInputObject);
        addNamedOutputObject(namedOutputObject);
        set(INCLUDE_COL_IDXS, includeColIdxs);
        set(NORMALIZATION_SETTINGS, normalizationSettings);
    }

    /**
     * @return the includes column indices
     */
    public Integer[] getIncludeColIdxs() {
        return get(INCLUDE_COL_IDXS);
    }

    /**
     * @return the normalization settings
     */
    public NormalizationSettings getNormalizationSettings() {
        return get(NORMALIZATION_SETTINGS);
    }


}
