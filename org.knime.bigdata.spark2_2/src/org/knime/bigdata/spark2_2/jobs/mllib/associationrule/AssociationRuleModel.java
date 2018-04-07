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
 *   Created on Feb 11, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark2_2.jobs.mllib.associationrule;

import java.io.Serializable;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Model that contains key of associations rules model parameters.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class AssociationRuleModel implements Serializable  {
    private static final long serialVersionUID = 1L;

    private final String m_uid;
    private final String m_rulesObjectName;
    private final double m_minConfidence;
    private final String m_antecedentColumn;
    private final String m_consequentColumn;

    AssociationRuleModel(final String uid, final String associationRulesObjectName, final double minConfidence, final String antecedentColumn, final String consequentColumn) {
        m_uid = uid;
        m_rulesObjectName = associationRulesObjectName;
        m_minConfidence = minConfidence;
        m_antecedentColumn = antecedentColumn;
        m_consequentColumn = consequentColumn;
    }

    /** @return unique model id */
    public String getModelUid() {
        return m_uid;
    }

    /** @return key of associations rules named object */
    public String getAssociationRulesObjectName() {
        return m_rulesObjectName;
    }

    /** @return minimal confidence of the rules (value between 0 and 1 or -1 if unknown) */
    public double getMinConfidence() {
        return m_minConfidence;
    }

    /** @return name of antecedent column */
    public String getAntecedentColumn() {
        return m_antecedentColumn;
    }

    /** @return name of consequent column */
    public String getConsequentColumn() {
        return m_consequentColumn;
    }

    /** @return human readable summary of this model */
    public String[] getSummary() {
        if (m_minConfidence >= 0) {
            return new String[] {
                "Min confidence: " + m_minConfidence,
                "Antecedent column: " + m_antecedentColumn,
                "Consequent column: "+ m_consequentColumn
            };
        } else {
            return new String[] {
                "Antecedent column: " + m_antecedentColumn,
                "Consequent column: "+ m_consequentColumn
            };
        }
    }
}
