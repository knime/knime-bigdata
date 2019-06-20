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
package org.knime.bigdata.spark.node.ml.prediction.randomforest;

import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JPanel;

import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeNodeDialog;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class RandomForestLearnerNodeDialog extends DecisionTreeNodeDialog<RandomForestLearnerSettings, RandomForestLearnerComponents> {

    /**
     * Creates a new instance.
     *
     * @param components
     */
    public RandomForestLearnerNodeDialog(final RandomForestLearnerComponents components) {
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

        addSeparatorAndLabel(settingsTab, "Forest Options", gbc);
        addBasicForestOptions(settingsTab, gbc);

        addSeparatorAndLabel(settingsTab, "Tree Options", gbc);
        addTreeOptions(settingsTab, gbc);

        return settingsTab;
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

        addAdvancedForestOptions(settingsTab, gbc);

        addVerticalGlue(settingsTab, gbc);

        return settingsTab;
    }

    private void addBasicForestOptions(final JPanel settingsTab, final GridBagConstraints gbc) {
        gbc.gridy++;
        addLine(settingsTab, "Number of models", getComponents().getNoOfModelsComponent().getComponentPanel(), gbc);
    }

    private void addAdvancedForestOptions(final JPanel settingsTab, final GridBagConstraints gbc) {
        if (getMode() == DecisionTreeLearnerMode.DEPRECATED) {
            gbc.gridy++;
            addLine(settingsTab, "Feature sampling",
                getComponents().getFeatureSamplingStrategyComponent().getComponentPanel(), gbc);

            gbc.gridy++;
            final JButton seedButton = new JButton("New");
            seedButton.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(final ActionEvent e) {
                    getSettings().nextSeed();
                }
            });

            eliminateHGapIfPossible(getComponents().getSeedComponent().getComponentPanel());
            final JPanel seedPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
            seedPanel.add(getComponents().getSeedComponent().getComponentPanel());
            seedPanel.add(seedButton);

            addLine(settingsTab, "Seed", seedPanel, gbc);
        } else  {
            gbc.gridy++;
            final JPanel dataSamplingPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
            eliminateHGapIfPossible(getComponents().getShouldSampleDataComponent().getComponentPanel());
            eliminateHGapIfPossible(getComponents().getDataSamplingRateComponent().getComponentPanel());
            dataSamplingPanel.add(getComponents().getShouldSampleDataComponent().getComponentPanel());
            dataSamplingPanel.add(getComponents().getDataSamplingRateComponent().getComponentPanel());
            addLine(settingsTab, "Data sampling (rows)", dataSamplingPanel, gbc);

            gbc.gridy++;
            addLine(settingsTab, "Feature sampling",
                getComponents().getFeatureSamplingStrategyComponent().getComponentPanel(), gbc);

            addSeedingOption(settingsTab, gbc);
        }
    }
}
