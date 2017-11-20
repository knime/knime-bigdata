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
 * ---------------------------------------------------------------------
 *
 * History
 *   Apr 30, 2015 (budiyanto): created
 */
package org.knime.bigdata.spark.node.preproc.rename;

import java.util.List;

import org.knime.base.node.preproc.rename.RenameConfiguration;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.ConvenienceMethods;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.core.util.SparkIDs;

/**
 * @author Tobias Koetter, KNIME GmbH
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkRenameColumnNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkRenameColumnNodeModel.class.getCanonicalName();

    /**
     * Config identifier for the NodeSettings object contained in the NodeSettings which contains the settings.
     */
    public static final String CFG_SUB_CONFIG = "all_columns";

    /** contains settings for each individual column. */
    private RenameConfiguration m_config;

    /**
     * Constructor for the node model.
     */
    protected SparkRenameColumnNodeModel() {

        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{SparkDataPortObject.TYPE}, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (m_config == null) {
            throw new InvalidSettingsException("No configuration available");
        }
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final DataTableSpec outSpec = m_config.getNewSpec(sparkSpec.getTableSpec());
        final List<String> missingColumnNames = m_config.getMissingColumnNames();
        if (missingColumnNames != null && !missingColumnNames.isEmpty()) {
            setWarningMessage("The following columns are configured but no longer exist: "
                + ConvenienceMethods.getShortStringFrom(missingColumnNames, 5));
        }
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outSpec)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject inputPort = (SparkDataPortObject) inObjects[0];
        final DataTableSpec outputTableSpec = m_config.getNewSpec(inputPort.getTableSpec());
        return new PortObject[] { executeRenameColumnJob(inputPort, outputTableSpec, exec) };
    }

    /**
     * Executes the rename column spark job.
     * @param inputPort - input RDD/data frame
     * @param outputTableSpec - output table spec with renamed columns
     * @param exec
     * @return output port with renamed columns
     * @throws Exception
     */
    public static PortObject executeRenameColumnJob(final SparkDataPortObject inputPort,
        final DataTableSpec outputTableSpec, final ExecutionContext exec)
        throws Exception {

        final SparkContextID contextID = inputPort.getContextID();
        final String namedInputObject = inputPort.getData().getID();
        final String namedOutputObject = SparkIDs.createSparkDataObjectID();
        final IntermediateSpec outputSpec =
            SparkDataTableUtil.toIntermediateSpec(outputTableSpec);

        final RenameColumnJobInput jobInput = new RenameColumnJobInput(namedInputObject, namedOutputObject, outputSpec);
        SparkContextUtil.getJobRunFactory(contextID, JOB_ID).createRun(jobInput).run(contextID, exec);

        final SparkDataTable outputTable = new SparkDataTable(contextID, namedOutputObject, outputTableSpec);
        final SparkDataPortObject outputPort = new SparkDataPortObject(outputTable);
        return outputPort;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        if (m_config != null) {
            final NodeSettingsWO subSettings = settings.addNodeSettings(CFG_SUB_CONFIG);
            m_config.save(subSettings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        new RenameConfiguration(settings.getNodeSettings(CFG_SUB_CONFIG));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_config = new RenameConfiguration(settings.getNodeSettings(CFG_SUB_CONFIG));
    }
}
