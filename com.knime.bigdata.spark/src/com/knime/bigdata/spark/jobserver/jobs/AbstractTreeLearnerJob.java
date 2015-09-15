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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * runs MLlib DecisionTree on a given RDD to create a decision tree, model is returned as result
 *
 * @author koetter, dwk
 */
public abstract class AbstractTreeLearnerJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Criterion used for information gain calculation. Supported values: "gini" (recommended) or "entropy".
     */
    public static final String PARAM_INFORMATION_GAIN = "impurity";

    /**
     * supported information gain criterion
     */
    public static final String VALUE_GINI = "gini";

    /**
     * supported information gain criterion
     */
    public static final String VALUE_ENTROPY = "entropy";

    /**
     * impurity for regression trees
     */
    public static final String VALUE_VARIANCE = "variance";

    /**
     * maxDepth - Maximum depth of the tree. E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf
     * nodes. (suggested value: 5)
     */
    public static final String PARAM_MAX_DEPTH = ParameterConstants.PARAM_MAX_DEPTH;

    /**
     * maxBins - maximum number of bins used for splitting features (suggested value: 32)
     */
    public static final String PARAM_MAX_BINS = ParameterConstants.PARAM_MAX_BINS;

    /** Number of classes. **/
    public static final String PARAM_NO_OF_CLASSES = "NumberOfClasses";

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;

        if (!aConfig.hasInputParameter(PARAM_MAX_DEPTH)) {
            msg = "Input parameter '" + PARAM_MAX_DEPTH + "' missing.";
        } else {
            try {
                if ((Integer)aConfig.getInputParameter(PARAM_MAX_DEPTH, Integer.class) == null) {
                    msg = "Input parameter '" + PARAM_MAX_DEPTH + "' is empty.";
                }
            } catch (Exception e) {
                msg = "Input parameter '" + PARAM_MAX_BINS + "' is not of expected type 'integer'.";
            }
        }
        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_MAX_BINS)) {
                msg = "Input parameter '" + PARAM_MAX_BINS + "' missing.";
            } else {
                try {
                    if ((Integer)aConfig.getInputParameter(PARAM_MAX_BINS, Integer.class) == null) {
                        msg = "Input parameter '" + PARAM_MAX_BINS + "' is empty.";
                    }
                } catch (Exception e) {
                    msg = "Input parameter '" + PARAM_MAX_BINS + "' is not of expected type 'integer'.";
                }
            }
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_INFORMATION_GAIN)) {
            msg = "Input parameter '" + PARAM_INFORMATION_GAIN + "' missing.";
        }

        if (msg == null) {
            msg = SupervisedLearnerUtils.checkConfig(aConfig);
        }
        //      input - Training dataset: RDD of LabeledPoint. Labels should take values {0, 1, ..., numClasses-1}.
        //      numClasses - number of classes for classification.
        //      categoricalFeaturesInfo - Map storing arity of categorical features.
        //       E.g., an entry (n -> k) indicates that feature n is categorical with k categories indexed from 0: {0, 1, ..., k-1}.

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }
}
