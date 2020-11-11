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
 *   Created on Sep 05, 2016 by Sascha
 */
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.json;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JCheckBox;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;

import org.knime.base.filehandling.NodeUtils;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.GenericDataSource2SparkNodeDialog3;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Dialog for the JSON to Spark node.
 * @author Sascha Wolke, KNIME.com
 */
public class Json2SparkNodeDialog3 extends GenericDataSource2SparkNodeDialog3<Json2SparkSettings3> {

    private JSpinner m_samplingRatio;
    private JCheckBox m_primitivesAsString;
    private JCheckBox m_allowComments;
    private JCheckBox m_allowUnquotedFieldnames;
    private JCheckBox m_allowSingleQuotes;
    private JCheckBox m_allowNumericLeadingZeros;
    private JCheckBox m_allowNonNumericNumbers;

    Json2SparkNodeDialog3(final Json2SparkSettings3 initialSettings) {
        super(initialSettings);
    }

    @Override
    protected void addSettingsPanels(final JPanel settingsPanel, final GridBagConstraints settingsPanelGbc) {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(createTitledBorder("Options"));
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);

        m_samplingRatio = new JSpinner(new SpinnerNumberModel(1, 0.0001, 1, 0.1));
        addToPanel(panel, gbc, "Sampling ratio", m_samplingRatio);
        m_primitivesAsString = new JCheckBox("Convert to string");
        addToPanel(panel, gbc, "Primitives", m_primitivesAsString);
        m_allowComments = new JCheckBox("Allow comments");
        addToPanel(panel, gbc, "Comments", m_allowComments);
        m_allowUnquotedFieldnames = new JCheckBox("Allow unquoted fieldnames");
        addToPanel(panel, gbc, "Fieldnames", m_allowUnquotedFieldnames);
        m_allowSingleQuotes = new JCheckBox("Allow single quotes");
        addToPanel(panel, gbc, "Quotes", m_allowSingleQuotes);
        m_allowNumericLeadingZeros = new JCheckBox("Allow numeric leading zeros");
        addToPanel(panel, gbc, "Numerics", m_allowNumericLeadingZeros);
        m_allowNonNumericNumbers = new JCheckBox("Allow non numeric numbers (-inf)");
        addToPanel(panel, gbc, m_allowNonNumericNumbers);

        settingsPanel.add(panel, settingsPanelGbc);
        settingsPanelGbc.gridy++;
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        super.loadSettingsFrom(settings, specs);
        m_samplingRatio.setValue(m_settings.getSchemaSamplingRatio());
        m_primitivesAsString.setSelected(m_settings.primitivesAsString());
        m_allowComments.setSelected(m_settings.allowComments());
        m_allowUnquotedFieldnames.setSelected(m_settings.allowUnquotedFieldNames());
        m_allowSingleQuotes.setSelected(m_settings.allowSingleQuotes());
        m_allowNumericLeadingZeros.setSelected(m_settings.allowNumericLeadingZeros());
        m_allowNonNumericNumbers.setSelected(m_settings.allowNonNumericNumbers());
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setSchemaSamplingRatio(((SpinnerNumberModel) m_samplingRatio.getModel()).getNumber().doubleValue());
        m_settings.setPrimitivesAsString(m_primitivesAsString.isSelected());
        m_settings.setAllowComments(m_allowComments.isSelected());
        m_settings.setAllowUnquotedFieldNames(m_allowUnquotedFieldnames.isSelected());
        m_settings.setAllowSingleQuotes(m_allowSingleQuotes.isSelected());
        m_settings.setAllowNumericLeadingZeros(m_allowNumericLeadingZeros.isSelected());
        m_settings.setAllowNonNumericNumbers(m_allowNonNumericNumbers.isSelected());
        super.saveSettingsTo(settings);
    }
}
