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
 *   Created on Sep 30, 2015 by bjoern
 */
package com.knime.bigdata.spark.node.scorer.numeric;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JPanel;

import org.knime.base.node.mine.scorer.numeric.NumericScorerDialogComponents;
import org.knime.base.node.mine.scorer.numeric.NumericScorerSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;

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

    private final NumericScorerSettings m_settings = new NumericScorerSettings();
    private final NumericScorerDialogComponents m_numericScorerDialogComponents = new NumericScorerDialogComponents(m_settings);

    /**
     * Creates a new dialog.
     */
    public SparkNumericScorerNodeDialog() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
        panel.add(m_numericScorerDialogComponents.getReferenceComponent().getComponentPanel());
        panel.add(m_numericScorerDialogComponents.getPredictionComponent().getComponentPanel());

        JPanel outputPanel = createSubPanel("Output column");
        panel.add(outputPanel);

        Box box = new Box(BoxLayout.Y_AXIS);
        box.add(Box.createHorizontalGlue());
        outputPanel.add(box);

        box.add(m_numericScorerDialogComponents.getOverrideComponent().getComponentPanel());
        box.add(Box.createHorizontalGlue());
        box.add(m_numericScorerDialogComponents.getOutputComponent().getComponentPanel());
        box.add(Box.createHorizontalGlue());

        JPanel flowVarPanel = createSubPanel("Provide scores as flow variables");
        panel.add(flowVarPanel);

        Box box2 = new Box(BoxLayout.Y_AXIS);
        box2.add(Box.createHorizontalGlue());
        flowVarPanel.add(box2);

        box2.add(m_numericScorerDialogComponents.getFlowVarComponent().getComponentPanel());
        box2.add(Box.createHorizontalGlue());
        box2.add(m_numericScorerDialogComponents.getUseNamePrefixComponent().getComponentPanel());
        box2.add(Box.createHorizontalGlue());

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
        m_settings.onOpen();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
       m_numericScorerDialogComponents.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        final PortObjectSpec[] tableSpec = new PortObjectSpec[]{((SparkDataPortObjectSpec)specs[0]).getTableSpec()};
        m_numericScorerDialogComponents.loadSettingsFrom(settings, tableSpec);
    }
}
