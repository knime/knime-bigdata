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
 *   Created on 12.02.2015 by koetter
 */
package org.knime.bigdata.spark.node.ml.prediction.gbt;

import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JPanel;

import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeNodeDialog;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class GradientBoostedTreesLearnerNodeDialog
    extends DecisionTreeNodeDialog<GradientBoostedTreesLearnerSettings, GradientBoostedTreesLearnerComponents> {

    /**
     * @param components
     */
    public GradientBoostedTreesLearnerNodeDialog(final GradientBoostedTreesLearnerComponents components) {
        super(components);
    }

    @Override
    protected JPanel getSettingsTab() {
        final JPanel settingsTab = new JPanel(new GridBagLayout());

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx = 0;
        gbc.gridy = 0;

        addColumnSelections(settingsTab, gbc);

        addSeparatorAndLabel(settingsTab, "Boosting Options", gbc);
        addBasicBoostingOptions(settingsTab, gbc);

        addSeparatorAndLabel(settingsTab, "Tree Options", gbc);
        addTreeOptions(settingsTab, gbc);

        return settingsTab;
    }

    private void addBasicBoostingOptions(final JPanel settingsTab, final GridBagConstraints gbc) {
        gbc.gridy++;
        addLine(settingsTab, "Number of models", getComponents().getMaxNoOfModelsComponent().getComponentPanel(), gbc);

        if (getMode() != DecisionTreeLearnerMode.CLASSIFICATION) {
            gbc.gridy++;
            addLine(settingsTab, "Loss function", getComponents().getLossFunctionComponent().getComponentPanel(), gbc);
        }

        if (getMode() == DecisionTreeLearnerMode.DEPRECATED) {
            gbc.gridy++;
            addLine(settingsTab, "Learning rate", getComponents().getLearningRateComponent().getComponentPanel(), gbc);
        }
    }

    @Override
    protected JPanel getAdvancedTab() {
        final JPanel settingsTab = new JPanel(new GridBagLayout());

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx = 0;
        gbc.gridy = 0;

        addAvancedBoostingOptions(settingsTab, gbc);

        addSeparatorAndLabel(settingsTab, "Other Options", gbc);
        addOtherOptions(settingsTab, gbc);

        addVerticalGlue(settingsTab, gbc);

        return settingsTab;
    }

    private void addAvancedBoostingOptions(final JPanel settingsTab, final GridBagConstraints gbc) {
        gbc.gridy++;
        addLine(settingsTab, "Learning rate", getComponents().getLearningRateComponent().getComponentPanel(), gbc);

        if (getMode() != DecisionTreeLearnerMode.DEPRECATED) {
            gbc.gridy++;
            final JPanel dataSamplingPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
            eliminateHGapIfPossible(getComponents().getShouldSampleDataComponent().getComponentPanel());
            eliminateHGapIfPossible(getComponents().getDataSamplingRateComponent().getComponentPanel());
            dataSamplingPanel.add(getComponents().getShouldSampleDataComponent().getComponentPanel());
            dataSamplingPanel.add(getComponents().getDataSamplingRateComponent().getComponentPanel());
            addLine(settingsTab, "Data sampling (rows)", dataSamplingPanel, gbc);

            gbc.gridy++;
            addLine(settingsTab, "Feature sampling",
                getComponents().getFeatureSubsetStrategyComponent().getComponentPanel(), gbc);

        }
    }
}
