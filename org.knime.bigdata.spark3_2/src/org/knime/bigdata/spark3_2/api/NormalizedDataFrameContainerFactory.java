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
 *   Created on 03.08.2015 by dwk
 */
package org.knime.bigdata.spark3_2.api;

import java.util.Arrays;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.preproc.normalize.NormalizationSettings;

/**
 *
 * @author dwk
 */
@SparkClass
public class NormalizedDataFrameContainerFactory {


    /**
     *
     * @param aScales
     * @param aTranslations
     * @return NormalizedRDDContainer with given scales and translations
     */
    public static NormalizedDataFrameContainer getNormalizedRDDContainer(final Double[] aScales, final Double[] aTranslations) {
        final double[] scales = new double[aScales.length];
        final double[] translations = new double[aTranslations.length];
        for (int i = 0; i < scales.length; i++) {
            scales[i] = aScales[i];
            translations[i] = aTranslations[i];
        }
        return new NormalizedDataFrameContainer(scales, translations);
    }

    /**
     * extracts scales and translations from given MultivariateStatisticalSummary aStats and creates NormalizedRDDContainer
     * @param aStats
     * @param aMode
     * @return NormalizedRDDContainer with extracted scales and translations
     */
   public static NormalizedDataFrameContainer getNormalizedRDDContainer(final MultivariateStatisticalSummary aStats, final NormalizationSettings aMode) {

       final double[] scale;
       final double[] translation;

        switch (aMode.m_mode) {
            case MINMAX_MODE: {
                final Vector min = aStats.min();
                final Vector max = aStats.max();

                // m_normalizationSettings.m_min + (aValue - min) / range;
                // m_normalizationSettings.m_min + aValue / range - min / range;

                scale = new double[min.size()];
                translation = new double[scale.length];
                for (int i = 0; i < scale.length; i++) {
                    final double range = (max.apply(i) - min.apply(i)) / aMode.getTargetRange();
                    if (range < 0.000000000001) {
                        scale[i] = 0d;
                        translation[i] = aMode.m_min;
                    } else {
                        scale[i] = 1d / range;
                        translation[i] = aMode.m_min - min.apply(i) * scale[i];
                    }
                }
                break;
            }
            case ZSCORE_MODE: {
                final Vector mean = aStats.mean();
                final Vector variance = aStats.variance();

                // m_normalizationSettings.m_min + (aValue - mean) / stddev;
                // aValue / stddev + m_normalizationSettings.m_min - mean / stddev;

                scale = new double[variance.size()];
                translation = new double[mean.size()];
                for (int i = 0; i < scale.length; i++) {
                    final double stddev = variance.apply(i);
                    scale[i] = 1d / Math.sqrt(stddev);
                    translation[i] = aMode.m_min - mean.apply(i) * scale[i];
                }
                break;
            }
            case DECIMALSCALING_MODE: {
                final Vector max = aStats.max();
                scale = new double[max.size()];
                translation = new double[max.size()];
                Arrays.fill(translation, 0.d);
                for (int i = 0; i < scale.length; i++) {
                    scale[i] = 1d / Math.pow(10, Math.ceil(Math.log10(max.apply(i))));
                }
                break;
            }
            default: {
                throw new UnsupportedOperationException("normalization mode " + aMode.m_mode
                    + " is not supported.");
            }
        }

        return new NormalizedDataFrameContainer(scale, translation);
    }
}
