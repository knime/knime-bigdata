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
 *   Created on 22.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.joiner;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JComponent;
import javax.swing.JPanel;

import org.knime.base.node.preproc.joiner.Joiner2NodeDialog;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkJoinerNodeDialog extends Joiner2NodeDialog {

    /**
     * Constructor.
     */
    public SparkJoinerNodeDialog() {
        super(true);
        init();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JPanel createJoinerSettingsTab() {
        JPanel p = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();

        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTHWEST;
        c.insets = new Insets(2, 2, 2, 2);
        c.gridx = 0;
        c.gridy = 0;
        c.gridwidth = 1;
        c.weighty = 0;
        p.add(createJoinModeUIControls(), c);

        c.gridy++;
        c.weightx = 1;
        c.weighty = 1;
        p.add(createJoinPredicateUIControls(false, false), c);

        c.gridy++;
        c.weightx = 100;
        c.weighty = 100;
        c.fill = GridBagConstraints.BOTH;
        p.add(new JPanel(), c);

        return p;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
        final PortObjectSpec[] specs) throws NotConfigurableException {
        if (specs == null || specs.length < 2 || specs[0] == null || specs[1] == null) {
            throw new NotConfigurableException("No input specification available");
        }
        final SparkDataPortObjectSpec spec0 = (SparkDataPortObjectSpec)specs[0];
        final SparkDataPortObjectSpec spec1 = (SparkDataPortObjectSpec)specs[1];
        super.loadSettingsFrom(settings, new DataTableSpec[] {spec0.getTableSpec(), spec1.getTableSpec()});
    }
}
