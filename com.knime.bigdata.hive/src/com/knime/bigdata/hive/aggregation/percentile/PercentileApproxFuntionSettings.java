/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * -------------------------------------------------------------------
 */

package com.knime.bigdata.hive.aggregation.percentile;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;


/**
 * Class that save the settings of the {@link PercentileFuntionSettingsPanel}.
 *
 * @author Tobias Koetter
 * @since 2.11
 */
public class PercentileApproxFuntionSettings extends PercentileFuntionSettings {

    private static final String CFG_OPTION = "approxNoOfRows";

    private final SettingsModelInteger m_approxOption;

    /**
     * @param percentile the default value
     * @param noOfRows the number of rows to use for approximation value
     */
    public PercentileApproxFuntionSettings(final double percentile, final int noOfRows) {
        super(percentile);
        m_approxOption = new SettingsModelIntegerBounded(CFG_OPTION, noOfRows, 0, Integer.MAX_VALUE);
    }

    /**
     * @return the approximation model
     */
    SettingsModelInteger getApproxModel() {
        return m_approxOption;
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read the settings from
     * @throws InvalidSettingsException if the settings are invalid
     */
    @Override
    public void validateSettings(final NodeSettingsRO settings)
    throws InvalidSettingsException {
        super.validateSettings(settings);
        m_approxOption.validateSettings(settings);
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read the settings from
     * @throws InvalidSettingsException if the settings are invalid
     */
    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsFrom(settings);
        m_approxOption.loadSettingsFrom(settings);
    }
    /**
     * @param settings the {@link NodeSettingsWO} to write to
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        m_approxOption.saveSettingsTo(settings);
    }

    /**
     * @return the approximation value
     */
    public int getApprox() {
        return m_approxOption.getIntValue();
    }

    /**
     * @return a clone of this settings object
     */
    @Override
    public PercentileApproxFuntionSettings createClone() {
        return new PercentileApproxFuntionSettings(getPercentile(), getApprox());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int result = super.hashCode() + ((m_approxOption == null) ? 0 : m_approxOption.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PercentileApproxFuntionSettings other = (PercentileApproxFuntionSettings)obj;
        if (m_approxOption == null) {
            if (other.m_approxOption != null) {
                return false;
            }
        } else if (!m_approxOption.equals(other.m_approxOption)) {
            return false;
        }
        return true;
    }
}
