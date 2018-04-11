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

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.core.port.model.SparkModel;
import org.knime.bigdata.spark.core.port.model.interpreter.HTMLModelInterpreter;
import org.knime.bigdata.spark.node.mllib.associationrule.SparkAssociationRuleLearnerNodeModel;
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleModelMetaData;

/**
 * Association rule model interpreter.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class AssociationRuleModelInterpreter extends HTMLModelInterpreter {
    private static final long serialVersionUID = 1L;

    private static AssociationRuleModelInterpreter INSTANCE =
        new AssociationRuleModelInterpreter();

    private AssociationRuleModelInterpreter() {
        // prevent object creation
    }

    /** @return the only instance */
    public static AssociationRuleModelInterpreter getInstance() {
        return INSTANCE;
    }

    @Override
    public String getModelName() {
        return SparkAssociationRuleLearnerNodeModel.MODEL_NAME;
    }

    @Override
    public String getSummary(final SparkModel sparkModel) {
        return getSummary(sparkModel, ", ");
    }

    @Override
    protected String getHTMLDescription(final SparkModel sparkModel) {
        return getSummary(sparkModel, "<br/>");
    }

    private String getSummary(final SparkModel sparkModel, final String separator) {
        final AssociationRuleModel model = (AssociationRuleModel) sparkModel.getModel();
        final AssociationRuleModelMetaData meta = (AssociationRuleModelMetaData) sparkModel.getMetaData();
        final StringBuilder sb = new StringBuilder();
        sb.append(StringUtils.join(meta.getSummary(), separator));
        sb.append(separator);
        sb.append(StringUtils.join(model.getSummary(), separator));
        return sb.toString();
    }
}
