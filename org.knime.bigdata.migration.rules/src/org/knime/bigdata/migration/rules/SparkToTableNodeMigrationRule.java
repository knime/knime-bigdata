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

import org.knime.bigdata.fileformats.parquet.reader.ParquetReaderNodeFactory;
import org.knime.bigdata.spark.local.node.create.LocalEnvironmentCreatorNodeFactory;
//import org.knime.bigdata.filefo
import org.knime.bigdata.spark.node.io.genericdatasource.writer.parquet.Spark2ParquetNodeFactory;
import org.knime.bigdata.spark.node.io.table.writer.Spark2TableNodeFactory;
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
public final class SparkToTableNodeMigrationRule extends NodeMigrationRule {

	@Override
	protected Class<? extends NodeFactory<?>> getReplacementNodeFactoryClass(final MigrationNode migrationNode,
			final MigrationNodeMatchResult matchResult) {
		return Spark2ParquetNodeFactory.class;
	}

	@Override
	protected MigrationNodeMatchResult match(final MigrationNode migrationNode) {
		return MigrationNodeMatchResult.of(migrationNode,
				Spark2TableNodeFactory.class.getName().equals(migrationNode.getOriginalNodeFactoryClassName())
						? NodeMigrationAction.REPLACE_WITH_MANY
						: null);
	}

	@Override
	protected void migrate(final MigrationNode migrationNode, final MigrationNodeMatchResult matchResult)
			throws MigrationException {

		final MigrationSubWorkflowBuilder builder = getSubWorkflowBuilder(migrationNode);
		builder.addNode(LocalEnvironmentCreatorNodeFactory.class, 200, 150);

		final MigrationNode spark2Parquet = builder.addNode(Spark2ParquetNodeFactory.class, 350, 300).getMigrationNode();
		NodeSettingsWO settings = getNewNodeModelSettings(spark2Parquet);
		settings.addString("outputDirectory", "/Users/knime");
		settings.addString("outputName", "s2t.parquet");
		settings.addString("saveMode", "Overwrite");
		builder.connect(1, 2, 2, 1);

		final MigrationNode parquetReader = builder.addNode(ParquetReaderNodeFactory.class, 500, 300).getMigrationNode();
		builder.connect(2, 0, 3, 0);
		settings = getNewNodeModelSettings(parquetReader);
		settings.addString("filename", "/Users/knime/s2t.parquet");

		builder.setMetaports(asList(spark2Parquet.getNewInputPorts().get(2)),
				asList(parquetReader.getNewOutputPorts().get(1)));
		associateOriginalInputPortWithNew(migrationNode, 1, 0);
		associateOriginalOutputPortWithNew(migrationNode, 1, 0);
	}

	@Override
	public String getMigrationType() {
		return "Performance optimization";
	}
	
}