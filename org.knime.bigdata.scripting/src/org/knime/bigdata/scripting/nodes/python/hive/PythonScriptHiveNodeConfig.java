/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
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
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.bigdata.scripting.nodes.python.hive;

import org.knime.code.generic.SourceCodeConfig;
import org.knime.code.generic.VariableNames;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

class PythonScriptHiveNodeConfig extends SourceCodeConfig {

	private static final VariableNames VARIABLE_NAMES = new VariableNames("flow_variables",
			null, null, null, null, null, new String[] {"db_util"}, null);

    /**Config key for the target folder.*/
    public static final String CFG_TARGET_FOLDER = "targetFolder";

	private String m_targetFolder = "";

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String getDefaultSourceCode() {
		final String var = VARIABLE_NAMES.getGeneralInputObjects()[0];
		return "# To prevent changes to the database in the node dialog\n" +
        "# do NOT call commit() in your script!\n" +
        "# All changes to the database are automatically\n" +
        "# committed once the node is executed.\n" +
        "# To list all functions of the db_util object call\n" +
        "# " + var + ".print_description()\n\n" +
        "df = " + var + ".get_dataframe()\n" +
        var + ".write_dataframe('resultTableName', df)";
	}

	/**
     * @return the targetFolder
     */
    public String getTargetFolder() {
        return m_targetFolder;
    }

    /**
     * @param targetFolder the targetFolder to set
     */
    public void setTargetFolder(final String targetFolder) {
        m_targetFolder = targetFolder;
    }

	/**
	 * Get the variable names for this node
	 *
	 * @return The variable names
	 */
	static VariableNames getVariableNames() {
		return VARIABLE_NAMES;
	}

	/**
     * Save configuration to the given node settings.
     *
     * @param settings
     *            The settings to save to
     */
    @Override
    public void saveTo(final NodeSettingsWO settings) {
        super.saveTo(settings);
        settings.addString(CFG_TARGET_FOLDER, m_targetFolder);
    }

    /**
     * Load configuration from the given node settings.
     *
     * @param settings
     *            The settings to load from
     * @throws InvalidSettingsException
     *             If the settings are invalid
     */
    @Override
    public void loadFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadFrom(settings);
        m_targetFolder = settings.getString(CFG_TARGET_FOLDER);
    }

    /**
     * Load configuration from the given node settings (using defaults if
     * necessary).
     *
     * @param settings
     *            The settings to load from
     */
    @Override
    public void loadFromInDialog(final NodeSettingsRO settings) {
        super.loadFromInDialog(settings);
        m_targetFolder = settings.getString(CFG_TARGET_FOLDER, "");
    }


}
