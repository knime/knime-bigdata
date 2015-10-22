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
 *   Created on Sep 30, 2015 by bjoern
 */
package com.knime.bigdata.spark.node.scorer.numeric;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.data.DoubleValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnName;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 * <code>NodeDialog</code> for the "Spark Numeric Scorer" node.
 *
 * TODO: this class should have been a DefaultNodeSettingsPane, but DialogComponentColumnNameSelection requires a proper
 * DataTableSpec, which needs to be "unpacked" from the SparkDataPortObjectSpec. Unfortunately
 * {@link org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane#loadSettingsFrom(NodeSettingsRO, PortObjectSpec[])}
 * is final and cannot be overridden to do the unpacking.
 *
 * TODO: this is mostly a code duplicate to {@link org.knime.base.node.mine.scorer.numeric.NumericScorerNodeDialog}
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkNumericScorerNodeDialog extends NodeDialogPane {

    private final SettingsModelColumnName m_referenceSetting = SparkNumericScorerNodeModel.createReference();

    private final SettingsModelColumnName m_predictedSetting = SparkNumericScorerNodeModel.createPredicted();

    private final SettingsModelBoolean m_overrideSetting = SparkNumericScorerNodeModel.createOverrideOutput();

    private final SettingsModelString m_outputSetting = SparkNumericScorerNodeModel.createOutput();

    private final DialogComponentColumnNameSelection m_referenceComp;

    private final DialogComponentColumnNameSelection m_predictionComp;

    private final DialogComponentBoolean m_overrideComp;

    private final DialogComponentString m_outputComp;

    /**
     * Creates a new dialog.
     */
    @SuppressWarnings("unchecked")
    public SparkNumericScorerNodeDialog() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

        m_referenceComp =
            new DialogComponentColumnNameSelection(m_referenceSetting, "Reference column", 0, DoubleValue.class);
        panel.add(m_referenceComp.getComponentPanel());

        m_predictionComp =
            new DialogComponentColumnNameSelection(m_predictedSetting, "Predicted column", 0, DoubleValue.class);
        panel.add(m_predictionComp.getComponentPanel());

        JPanel outputPanel = createSubPanel("Output column");
        panel.add(outputPanel);

        Box box = new Box(BoxLayout.Y_AXIS);
        box.add(Box.createHorizontalGlue());
        outputPanel.add(box);

        m_overrideComp = new DialogComponentBoolean(m_overrideSetting, "Change column name");
        box.add(m_overrideComp.getComponentPanel());
        box.add(Box.createHorizontalGlue());

        m_overrideSetting.addChangeListener(new ChangeListener() {
            /**
             * {@inheritDoc}
             */
            @Override
            public void stateChanged(final ChangeEvent e) {
                m_outputSetting.setEnabled(m_overrideSetting.getBooleanValue());
            }
        });

        m_outputComp = new DialogComponentString(m_outputSetting, "Output column name");
        box.add(m_outputComp.getComponentPanel());
        box.add(Box.createHorizontalGlue());

        m_predictionComp.getModel().addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                if (!m_overrideSetting.getBooleanValue()) {
                    m_outputSetting.setStringValue(m_predictedSetting.getColumnName());
                }
            }
        });

        addTab("Options", panel);
    }

    private JPanel createSubPanel(final String title) {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), title));
        return panel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onOpen() {
        super.onOpen();
        //force update of the visual state (view model)
        String columnName = m_predictedSetting.getColumnName();
        m_predictedSetting.setSelection(null, false);
        m_predictedSetting.setSelection(columnName, false);
        boolean b = m_overrideSetting.getBooleanValue();
        m_overrideSetting.setBooleanValue(!b);
        m_overrideSetting.setBooleanValue(b);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_referenceComp.saveSettingsTo(settings);
        m_predictionComp.saveSettingsTo(settings);
        m_overrideComp.saveSettingsTo(settings);
        m_outputComp.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        final PortObjectSpec[] tableSpec = new PortObjectSpec[]{((SparkDataPortObjectSpec)specs[0]).getTableSpec()};
        m_referenceComp.loadSettingsFrom(settings, tableSpec);
        m_predictionComp.loadSettingsFrom(settings, tableSpec);
        m_overrideComp.loadSettingsFrom(settings, tableSpec);
        m_outputComp.loadSettingsFrom(settings, tableSpec);
    }
}
