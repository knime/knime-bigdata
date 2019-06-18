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
 *   Created on 07.08.2015 by dwk
 */
package org.knime.bigdata.spark.core.job.util;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author dwk
 */
@SparkClass
public class EnumContainer {
    /**
     * (should be identical to org.knime.base.node.preproc.sample.SamplingNodeSettings.CountMethods) Enum for the two
     * methods for setting the number of rows in the output table.
     */
    public enum CountMethod {
        /** Relative fraction. */
        Relative,
        /** Absolute number. */
        Absolute;

        /**
         * @param name the name of the KNIME CountMethods instance
         * @return the corresponding Spark {@link CountMethod}
         */
        public static CountMethod fromKNIMEMethodName(final String name) {
            return CountMethod.valueOf(name);
        }
    }

    /**
     * Enum for the four different sampling methods. (should be identical to
     * org.knime.base.node.preproc.sample.SamplingNodeSettings.SamplingMethods)
     */
    public enum SamplingMethod {
        /** Selects the first <em>x</em> rows. */
        First,
        /** Selects rows randomly. */
        Random,
        /** Select rows randomly but maintain the class distribution. */
        Stratified,
        /** Select the rows linearly over the whole table. */
        Linear;

        /**
         * @param name the name of the KNIME SamplingMethods instance
         * @return the corresponding Spark {@link SamplingMethod}
         */
        public static SamplingMethod fromKNIMEMethodName(final String name) {
            return SamplingMethod.valueOf(name);
        }
    }

    /**
     * enum for the two different correlation methods
     *
     * @author dwk
     */
    public enum CorrelationMethod {
        /** Pearson */
        Pearson,
        /** Spearman */
        Spearman;
    }

    /**
     * see RandomForestJob Number of features to consider for splits at each node. Supported: "auto", "all", "sqrt",
     * "log2", "onethird". If "auto" is set, parameter is set based on numTrees: if numTrees == 1, set to "all"; if
     * numTrees > 1 (forest) set to "sqrt".
     */
    public enum FeatureSubsetStrategy {
        /**  */
        auto,
        /**  */
        all,
        /**  */
        sqrt,
        /**  */
        log2,
        /**  */
        onethird;
    }

    /**
     *
     *
     * @author dwk
     */
    public enum LossFunction {
        /**  */
        AbsoluteError,
        /**  */
        LogLoss,
        /**  */
        SquaredError;
    }

    /**
     * Defines the
     * <a href="http://spark.apache.org/docs/1.2.1/mllib-linear-methods.html#loss-functions">linear method loss function</a>.
     * @author dwk
     */
    public enum LinearLossFunction {
        /** */
        Hinge,
        /** */
        LeastSquares,
        /** */
        Logistic;
    }

    /**
     * Defines the
     * <a href="http://spark.apache.org/docs/1.2.1/mllib-linear-methods.html#regularizers">linear methods regularizer</a>.
     * @author dwk
     */
    public enum LinearRegularizer {
        /** */
        zero,
        /** */
        L2,
        /** */
        L1;
    }

    /**
     * Criterion used for quality measure calculation in decision-tree based model learners.
     */
    public enum QualityMeasure {
        /***/
        gini,
        /***/
        entropy,
        /***/
        variance;
    }

    /**
     * Mapping types for nominal to numerical conversion.
     */
    public enum MappingType {
        /**
         * use one map for all columns so that the same value in different columns is mapped to the same number
         */
        GLOBAL,
        /**
         * use a separate map for each column to that the distinct values in each column are always numbered from 1 to N
         */
        COLUMN,
        /**
         * use a binary mapping - one new column for each distinct value
         */
        BINARY;
    }

    /**
     * @param e the Enum
     * @return the names of the Enum
     */
    public static String[] getNames(final Enum<?>... e) {
        final String[] names = new String[e.length];
        for (int i = 0, length = e.length; i < length; i++) {
            names[i] = e[i].name();
        }
        return names;
    }
}
