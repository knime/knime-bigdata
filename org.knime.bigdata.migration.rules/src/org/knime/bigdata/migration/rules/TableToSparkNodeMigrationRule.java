package org.knime.bigdata.migration.rules;
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
 */

import static java.util.Arrays.asList;

import org.knime.bigdata.fileformats.parquet.writer.ParquetWriterNodeFactory;
import org.knime.bigdata.spark.local.node.create.LocalEnvironmentCreatorNodeFactory;
import org.knime.bigdata.spark.node.io.genericdatasource.reader.parquet.Parquet2SparkNodeFactory2;
import org.knime.bigdata.spark.node.io.table.reader.Table2SparkNodeFactory2;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeSettingsWO;
import org.knime.node.workflow.migration.MigrationException;
import org.knime.node.workflow.migration.MigrationNodeMatchResult;
import org.knime.node.workflow.migration.NodeMigrationAction;
import org.knime.node.workflow.migration.NodeMigrationRule;
import org.knime.node.workflow.migration.model.MigrationNode;
import org.knime.node.workflow.migration.model.MigrationSubWorkflowBuilder;

/**
 * 
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 *
 */
public final class TableToSparkNodeMigrationRule extends NodeMigrationRule {

	@Override
	protected Class<? extends NodeFactory<?>> getReplacementNodeFactoryClass(final MigrationNode migrationNode,
			final MigrationNodeMatchResult matchResult) {
		return Parquet2SparkNodeFactory2.class;
	}

	@Override
	protected MigrationNodeMatchResult match(final MigrationNode migrationNode) {
		return MigrationNodeMatchResult.of(migrationNode,
				Table2SparkNodeFactory2.class.getName().equals(migrationNode.getOriginalNodeFactoryClassName())
						? NodeMigrationAction.REPLACE_WITH_MANY
						: null);
	}

	@Override
	protected void migrate(final MigrationNode migrationNode, final MigrationNodeMatchResult matchResult)
			throws MigrationException {

		final MigrationSubWorkflowBuilder builder = getSubWorkflowBuilder(migrationNode);
		MigrationNode parquetWriter = builder.addNode(ParquetWriterNodeFactory.class, 200, 150).getMigrationNode();
		NodeSettingsWO settings = getNewNodeModelSettings(parquetWriter);
		settings.addString("filename", "/Users/knime/t2s.parquet");
		settings.addBoolean("overwrite", true);

		builder.addNode(LocalEnvironmentCreatorNodeFactory.class, 350, 150);

		MigrationNode parquet2Spark = builder.addNode(Parquet2SparkNodeFactory2.class, 500, 300).getMigrationNode();
		settings = getNewNodeModelSettings(parquet2Spark);
		settings.addString("inputPath", "/Users/knime/t2s.parquet");
		builder.connect(1, 0, 3, 0);
		builder.connect(2, 2, 3, 1);

		builder.setMetaports(asList(parquetWriter.getNewInputPorts().get(2), parquet2Spark.getNewInputPorts().get(2)),
				asList(parquet2Spark.getNewOutputPorts().get(1)));
		associateOriginalInputPortWithNew(migrationNode, 1, 0);
		associateOriginalInputPortWithNew(migrationNode, 2, 1);
		associateOriginalOutputPortWithNew(migrationNode, 1, 0);
	}

}