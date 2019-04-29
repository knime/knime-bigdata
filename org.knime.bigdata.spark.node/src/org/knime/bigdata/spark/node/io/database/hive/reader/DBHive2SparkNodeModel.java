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
 *   Created on 25.04.2019 by Mareike Hoeger
 */
package org.knime.bigdata.spark.node.io.database.hive.reader;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.node.io.hive.reader.Hive2SparkJobInput;
import org.knime.bigdata.spark.node.io.hive.reader.Hive2SparkJobOutput;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.database.DBType;
import org.knime.database.port.DBDataPortObject;
import org.knime.database.port.DBDataPortObjectSpec;

/**
 *
 * @author Tobias Koetter, Mareike Hoeger, KNIME.com
 */
public class DBHive2SparkNodeModel extends SparkSourceNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DBHive2SparkNodeModel.class);

    /** The unique Spark job id. */
    public static final String JOB_ID =
        org.knime.bigdata.spark.node.io.hive.reader.Hive2SparkNodeModel.class.getCanonicalName();

    /**
     * Constructor.
     *
     */
    public DBHive2SparkNodeModel() {
        super(new PortType[]{DBDataPortObject.TYPE}, false, new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Hive query found");
        }
        final DBDataPortObjectSpec spec = (DBDataPortObjectSpec)inSpecs[0];

        checkDatabaseType(spec);

        return new PortObjectSpec[]{null};
    }

    /**
     * Checks whether the input Database is compatible.
     *
     * @param spec the {@link DBDataPortObjectSpec} from the input port
     * @throws InvalidSettingsException If the wrong database is connected
     */
    protected void checkDatabaseType(final DBDataPortObjectSpec spec) throws InvalidSettingsException {
        DBType dbType = spec.getDBSession().getDriver().getDriverDefinition().getDBType();
        //FIXME compare with HIVE Type once connector is merged
        if (!dbType.getId().equalsIgnoreCase("hive")) {
            throw new InvalidSettingsException("Input must be a Hive connection");
        }

        //        if(Hive.DB_TYPE != dbType) {
        //            throw new InvalidSettingsException("Input must be a Hive connection");
        //        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting Spark job");
        final SparkContextID contextID = getContextID(inData);

        final DBDataPortObject db = (DBDataPortObject)inData[0];
        final String hiveQuery = db.getData().getQuery().getQuery();

        final String namedOutputObject = SparkIDs.createSparkDataObjectID();

        LOGGER.debug("Original sql: " + hiveQuery);
        final Hive2SparkJobInput jobInput = new Hive2SparkJobInput(namedOutputObject, hiveQuery);
        LOGGER.debug("Cleaned sql: " + jobInput.getQuery());
        final Hive2SparkJobOutput jobOutput =
            SparkContextUtil.<Hive2SparkJobInput, Hive2SparkJobOutput> getJobRunFactory(contextID, JOB_ID)
                .createRun(jobInput).run(contextID, exec);

        final DataTableSpec outputSpec =
            KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(namedOutputObject));

        return new PortObject[]{new SparkDataPortObject(new SparkDataTable(contextID, namedOutputObject, outputSpec))};

    }

}