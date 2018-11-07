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
 *   Created on Nov 5, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.preproc.filter.row.operator;

import org.knime.core.node.rowfilter.OperatorPanel;
import org.knime.core.node.rowfilter.OperatorValidation;
import org.knime.core.node.rowfilter.panel.SingleFieldPanel;
import org.knime.core.node.rowfilter.panel.TwoFieldsPanel;
import org.knime.core.node.rowfilter.registry.AbstractOperatorRegistry;
import org.knime.core.node.rowfilter.registry.OperatorValue;
import org.knime.core.node.rowfilter.validation.SingleOperandValidation;
import org.knime.core.node.rowfilter.validation.TwoOperandsValidation;
import org.knime.database.agent.rowfilter.DBOperators;

/**
 * Spark based DB operator registry for row filter node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkOperatorRegistry extends AbstractOperatorRegistry<SparkOperatorFunction> {

    private static final SparkOperatorRegistry INSTANCE = new SparkOperatorRegistry();

    private SparkOperatorRegistry() {

        final OperatorValidation oneOperandValidation = new SingleOperandValidation();
        final OperatorValidation twoOperandsValidation = new TwoOperandsValidation();

        final OperatorPanel singleFieldPanel = new SingleFieldPanel();
        final OperatorPanel twoFieldsPanel = new TwoFieldsPanel();

        // EQUAL
        final SparkOperatorFunction equalFunction = new SparkOneParameterOperatorFunction("==");
        addBooleanOperator(DBOperators.EQUAL,
            OperatorValue.builder(equalFunction).withValidation(oneOperandValidation).withPanel(singleFieldPanel).build());
        addNumericOperator(DBOperators.EQUAL,
            OperatorValue.builder(equalFunction).withValidation(oneOperandValidation).withPanel(singleFieldPanel).build());
        addStringOperator(DBOperators.EQUAL,
            OperatorValue.builder(equalFunction).withValidation(oneOperandValidation).withPanel(singleFieldPanel).build());

        // NOT EQUAL
        final SparkOperatorFunction notEqualFunction = new SparkOneParameterOperatorFunction("!=");
        addBooleanOperator(DBOperators.NOT_EQUAL,
            OperatorValue.builder(notEqualFunction).withValidation(oneOperandValidation).withPanel(singleFieldPanel).build());
        addNumericOperator(DBOperators.NOT_EQUAL,
            OperatorValue.builder(notEqualFunction).withValidation(oneOperandValidation).withPanel(singleFieldPanel).build());
        addStringOperator(DBOperators.NOT_EQUAL,
            OperatorValue.builder(notEqualFunction).withValidation(oneOperandValidation).withPanel(singleFieldPanel).build());

        // GREATER
        final SparkOperatorFunction greaterFunction = new SparkOneParameterOperatorFunction(">");
        addNumericOperator(DBOperators.GREATER,
            OperatorValue.builder(greaterFunction).withValidation(oneOperandValidation).withPanel(singleFieldPanel).build());

        // GREATER OR EQUAL
        final SparkOperatorFunction greaterOrEqualFunction = new SparkOneParameterOperatorFunction(">=");
        addNumericOperator(DBOperators.GREATER_OR_EQUAL,
            OperatorValue.builder(greaterOrEqualFunction).withValidation(oneOperandValidation).withPanel(singleFieldPanel).build());

        // LESS
        final SparkOperatorFunction lessFunction = new SparkOneParameterOperatorFunction("<");
        addNumericOperator(DBOperators.LESS,
            OperatorValue.builder(lessFunction).withValidation(oneOperandValidation).withPanel(singleFieldPanel).build());

        // LESS OR EQUAL
        final SparkOperatorFunction lessOrEqualFunction = new SparkOneParameterOperatorFunction("<=");
        addNumericOperator(DBOperators.LESS_OR_EQUAL,
            OperatorValue.builder(lessOrEqualFunction).withValidation(oneOperandValidation).withPanel(singleFieldPanel).build());

        // LIKE and RLIKE
        final SparkOperatorFunction likeFunction = new SparkOneParameterOperatorFunction("LIKE");
        addStringOperator(DBOperators.LIKE,
            OperatorValue.builder(likeFunction).withValidation(oneOperandValidation).withPanel(singleFieldPanel).build());
        final SparkOperatorFunction rlikeFunction = new SparkOneParameterOperatorFunction("RLIKE");
        addStringOperator(DBOperators.RLIKE,
            OperatorValue.builder(rlikeFunction).withValidation(oneOperandValidation).withPanel(singleFieldPanel).build());

        // BETWEEN
        final SparkOperatorFunction betweenFunction = new SparkBetweenOperatorFunction();
        addNumericOperator(DBOperators.BETWEEN,
            OperatorValue.builder(betweenFunction).withValidation(twoOperandsValidation).withPanel(twoFieldsPanel).build());

        // NULL and not NULL
        final SparkOperatorFunction isNullFunction = new SparkNoParameterOperatorFunction("IS NULL");
        addDefaultOperator(DBOperators.IS_NULL, OperatorValue.builder(isNullFunction).build());
        final SparkOperatorFunction isNotNullFunction = new SparkNoParameterOperatorFunction("IS NOT NULL");
        addDefaultOperator(DBOperators.IS_NOT_NULL, OperatorValue.builder(isNotNullFunction).build());
    }

    /** @return registry singleton */
    public static SparkOperatorRegistry getInstance() {
        return INSTANCE;
    }
}
