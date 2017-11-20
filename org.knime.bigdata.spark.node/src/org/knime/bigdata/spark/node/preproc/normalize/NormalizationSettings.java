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
 *   Created on 16.07.2015 by dwk
 */
package com.knime.bigdata.spark.node.preproc.normalize;

import java.io.Serializable;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 * normalization settings, type plus respective parameters for each type
 *
 * @author dwk
 */
@SparkClass
public class NormalizationSettings implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The Normalization mode */
    public final NormalizationMode m_mode;

    /** The Normalization min */
    public final double m_min;

    /** The Normalization max */
    public final double m_max;

    /**
     *
     * @param aMode
     * @param aMin
     * @param aMax
     */
    NormalizationSettings(final NormalizationMode aMode, final double aMin, final double aMax) {
        m_mode = aMode;
        m_min = aMin;
        m_max = aMax;
    }

    /**
     * indicates mapping type
     *
     * @author dwk
     */
    public enum NormalizationMode {
        /**
         * No Normalization mode.
         */
        NONORM_MODE,
        /**
         * MINMAX mode.
         */
        MINMAX_MODE,
        /**
         * ZSCORE mode.
         */
        ZSCORE_MODE,

        /**
         * DECIMAL SCALING mode.
         */
        DECIMALSCALING_MODE;
    }

    /**
     * @return target range after normalization (only relevant for MINMAX_MODE)
     */
    public double getTargetRange() {
        if (m_mode == NormalizationMode.MINMAX_MODE) {
            final double range = m_max - m_min;
            if (range > 0) {
                return range;
            }
        }
        return 1;
    }
}