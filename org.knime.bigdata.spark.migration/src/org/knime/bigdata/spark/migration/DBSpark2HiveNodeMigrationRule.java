/*
 * ------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------
 *
 * History
 *   Jun 23, 2019 (Tobias): created
 */
package org.knime.bigdata.spark.migration;

import org.knime.bigdata.spark.node.io.database.hive.writer.DBSpark2HiveNodeFactory;
import org.knime.bigdata.spark.node.io.database.hive.writer.DBSpark2HiveSettings;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.database.migration.DBMigrationUtil;
import org.knime.database.node.component.dbrowser.SettingsModelDBMetadata;
import org.knime.workflow.migration.MigrationException;
import org.knime.workflow.migration.MigrationNodeMatchResult;
import org.knime.workflow.migration.NodeMigrationAction;
import org.knime.workflow.migration.NodeMigrationRule;
import org.knime.workflow.migration.NodeSettingsMigrationManager;
import org.knime.workflow.migration.model.MigrationNode;

/**
 * Node migration rule for the <em>Spark to Hive</em> node.
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class DBSpark2HiveNodeMigrationRule extends NodeMigrationRule {
    /**
     * {@inheritDoc}
     */
    @Override
    protected Class<? extends NodeFactory<?>> getReplacementNodeFactoryClass(final MigrationNode migrationNode,
        final MigrationNodeMatchResult matchResult) {
        return DBSpark2HiveNodeFactory.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected MigrationNodeMatchResult match(final MigrationNode migrationNode) {
        return MigrationNodeMatchResult.of(migrationNode,
            "org.knime.bigdata.spark.node.io.hive.writer.Spark2HiveNodeFactory".equals(
                migrationNode.getOriginalNodeFactoryClassName()) ? NodeMigrationAction.REPLACE : null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void migrate(final MigrationNode migrationNode, final MigrationNodeMatchResult matchResult)
        throws MigrationException {
        // Ports
        associateEveryOriginalPortWithNew(migrationNode);

        // Settings
        final NodeSettingsMigrationManager settingsManager = createSettingsManager(migrationNode);

        // Model and variable settings
        final NodeSettingsRO origVariableSettings = migrationNode.getOriginalNodeVariableSettings();
        final NodeSettingsRO origModelSettings = migrationNode.getOriginalNodeModelSettings();
        final NodeSettingsWO newModelSettings = getNewNodeModelSettings(migrationNode);
        final StringBuilder warnMsg = new StringBuilder();
        //table name migration
        final SettingsModelDBMetadata tableNameSettings = DBMigrationUtil.convertTableNameModelAndOptionalVariable(
            origModelSettings, origVariableSettings, settingsManager, "tableName", "table", warnMsg);
        tableNameSettings.saveSettingsTo(newModelSettings);
        //if exists options
        final boolean dropExisting = origModelSettings.getBoolean("dropExistingTable", false);
        final String dropExistingString =  dropExisting ? DBSpark2HiveSettings.TableExistsAction.DROP.getActionCommand()
            : DBSpark2HiveSettings.TableExistsAction.FAIL.getActionCommand();
        newModelSettings.addString("existingTable", dropExistingString);
        //file format settings
        settingsManager.copyModelAndOptionalVariableSettings("fileFormat").toIdentical();
        settingsManager.copyModelAndOptionalVariableSettings("compression").toIdentical();

        // Miscellaneous settings
        settingsManager.copyAllMiscellaneousSettings().toIdentical();
    }
}
