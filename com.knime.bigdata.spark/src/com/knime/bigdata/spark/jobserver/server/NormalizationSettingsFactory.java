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
 *   Created on 31.07.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.server;

import com.knime.bigdata.spark.jobserver.server.NormalizationSettings.NormalizationMode;

/**
 *
 * @author dwk
 */
public class NormalizationSettingsFactory {

    /**
     *
     * @param aMin
     * @param aMax
     * @return NormalizationSettings with MINMAX_MODE
     */
    public static NormalizationSettings createNormalizationSettingsForMinMaxScaling(final double aMin, final double aMax) {
        return new NormalizationSettings(NormalizationMode.MINMAX_MODE, aMin, aMax);
    }

    /**
     *
     * @return NormalizationSettings with ZSCORE_MODE
     */
    public static NormalizationSettings createNormalizationSettingsForZScoreNormalization() {
        return new NormalizationSettings(NormalizationMode.ZSCORE_MODE, Double.MIN_VALUE, Double.MAX_VALUE);
    }

    /**
     *
     * @return NormalizationSettings with DECIMALSCALING_MODE
     */
    public static NormalizationSettings createNormalizationSettingsForDecimalScaling() {
        return new NormalizationSettings(NormalizationMode.DECIMALSCALING_MODE, Double.MIN_VALUE, Double.MAX_VALUE);
    }

}
