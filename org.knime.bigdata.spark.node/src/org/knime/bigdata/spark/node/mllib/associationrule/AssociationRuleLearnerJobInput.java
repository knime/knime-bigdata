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
 *   Created on Jan 29, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.mllib.associationrule;

import java.io.Serializable;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Association rules learner job input container.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
@SuppressWarnings("javadoc")
public class AssociationRuleLearnerJobInput extends JobInput {
    private static final String FREQ_ITEMS_MODEL = "freqItemsModel";
    private static final String MIN_CONFIDENCE = "minConfidence";

    /** Deserialization constructor */
    public AssociationRuleLearnerJobInput() {}

    AssociationRuleLearnerJobInput(final Serializable freqItemsModel, final String associationRulesOutputObject,
        final double minConfidence) {

        set(FREQ_ITEMS_MODEL, freqItemsModel);
        addNamedOutputObject(associationRulesOutputObject);
        set(MIN_CONFIDENCE, minConfidence);
    }

    public Serializable getFreqItemsModel() {
        return get(FREQ_ITEMS_MODEL);
    }

    public double getMinConfidence() {
        return getDouble(MIN_CONFIDENCE);
    }

    public String getAssociationRulesOutputObject() {
        return getFirstNamedOutputObject();
    }
}
