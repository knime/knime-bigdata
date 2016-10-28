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
 *   Created on Sep 05, 2016 by Sascha
 */
package com.knime.bigdata.spark.node.io.genericdatasource.reader.json;

import javax.swing.JCheckBox;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeDialog;

/**
 * Dialog for the JSON to Spark node.
 * @author Sascha Wolke, KNIME.com
 */
class Json2SparkNodeDialog extends GenericDataSource2SparkNodeDialog<Json2SparkSettings> {

    private final JSpinner m_samplingRatio;
    private final JCheckBox m_primitivesAsString;
    private final JCheckBox m_allowComments;
    private final JCheckBox m_allowUnquotedFieldnames;
    private final JCheckBox m_allowSingleQuotes;
    private final JCheckBox m_allowNumericLeadingZeros;
    private final JCheckBox m_allowNonNumericNumbers;

    public Json2SparkNodeDialog(final Json2SparkSettings initialSettings) {
        super(initialSettings);
        m_samplingRatio = new JSpinner(new SpinnerNumberModel(1, 0.0001, 1, 0.1));
        addToOptionsPanel("Sampling ratio", m_samplingRatio);
        m_primitivesAsString = new JCheckBox("Convert to string");
        addToOptionsPanel("Primitives", m_primitivesAsString);
        m_allowComments = new JCheckBox("Allow comments");
        addToOptionsPanel("Comments", m_allowComments);
        m_allowUnquotedFieldnames = new JCheckBox("Allow unquoted fieldnames");
        addToOptionsPanel("Fieldnames", m_allowUnquotedFieldnames);
        m_allowSingleQuotes = new JCheckBox("Allow single quotes");
        addToOptionsPanel("Quotes", m_allowSingleQuotes);
        m_allowNumericLeadingZeros = new JCheckBox("Allow numeric leading zeros");
        addToOptionsPanel("Numerics", m_allowNumericLeadingZeros);
        m_allowNonNumericNumbers = new JCheckBox("Allow non numeric numbers (-inf)");
        addToOptionsPanel(m_allowNonNumericNumbers);
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
