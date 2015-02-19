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
 *   Created on 12.02.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.clustering.assigner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import javax.swing.JOptionPane;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CellFactory;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.workflow.CredentialsProvider;

import com.knime.bigdata.hive.utility.HiveUtility;
import com.knime.bigdata.spark.port.MLlibModel;
import com.knime.bigdata.spark.port.MLlibPortObject;

/**
 *
 * @author koetter
 */
public class MLlibClusterAssignerNodeModel extends NodeModel {

    private static final String DATABASE_IDENTIFIER = HiveUtility.DATABASE_IDENTIFIER;
//    private final SettingsModelIntegerBounded m_noOfCluster = createNoOfClusterModel();
//    private final SettingsModelIntegerBounded m_noOfIteration = createNoOfIterationModel();
    private final SettingsModelString m_tableName = createTableNameModel();
    private final SettingsModelString m_colName = createColumnNameModel();

    /**
     *
     */
    public MLlibClusterAssignerNodeModel() {
        super(new PortType[]{MLlibPortObject.TYPE, DatabasePortObject.TYPE},
            new PortType[]{DatabasePortObject.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelString createTableNameModel() {
        return new SettingsModelString("tableName", "result");
    }

    /**
     * @return
     */
    static SettingsModelString createColumnNameModel() {
        return new SettingsModelString("columnName", "Cluster");
    }

    /**
     * @return
     */
    static SettingsModelIntegerBounded createNoOfClusterModel() {
        return new SettingsModelIntegerBounded("noOfCluster", 3, 1, Integer.MAX_VALUE);
    }

    /**
     * @return
     */
    static SettingsModelIntegerBounded createNoOfIterationModel() {
        return new SettingsModelIntegerBounded("noOfIteration", 30, 1, Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        DatabasePortObjectSpec spec = (DatabasePortObjectSpec) inSpecs[1];
        if (!spec.getDatabaseIdentifier().equals(DATABASE_IDENTIFIER)) {
            throw new InvalidSettingsException("Only Hive connections are supported");
        }
        return new PortObjectSpec[] {createSQLSpec(spec, getCredentialsProvider(), m_tableName.getStringValue(),
            m_colName.getStringValue())};
    }

    /**
         * {@inheritDoc}
         */
        @Override
        protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
            @SuppressWarnings("unchecked")
            final MLlibModel<KMeansModel> model = ((MLlibPortObject<KMeansModel>)inObjects[0]).getModel();
            final DatabasePortObject db = (DatabasePortObject)inObjects[1];
            final DataTableSpec tableSpec = db.getSpec().getDataTableSpec();
            final String resultTableName = m_tableName.getStringValue();
            final CredentialsProvider cp = getCredentialsProvider();
            final DatabaseQueryConnectionSettings connSettings = db.getConnectionSettings(cp);
            exec.setMessage("Drop result tabe if exists");
            connSettings.execute("DROP TABLE IF EXISTS " + resultTableName , cp);
            final String jdbcUrl = connSettings.getJDBCUrl();
            System.out.println(jdbcUrl);
            exec.setMessage("Connecting to Spark...");
            final SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("knimeTest");
    //        conf.set("hive.metastore.uris", "thrift://sandbox.hortonworks.com:9083");
            try (final JavaSparkContext sc = new JavaSparkContext(conf);) {
            exec.checkCanceled();
            exec.setMessage("Connecting to Hive...");
            final JavaHiveContext sqlsc = new JavaHiveContext(sc);
            exec.setMessage("Execute Hive Query...");
            final Collection<Integer> numericColIdx = new LinkedHashSet<>();
            int counter = 0;
            for (DataColumnSpec colSpec : tableSpec) {
                if (colSpec.getType().isCompatible(DoubleValue.class)) {
                    numericColIdx.add(Integer.valueOf(counter));
                }
                counter++;
            }
            final String sql = connSettings.getQuery();
            final StructType resultSchema = createSchema(tableSpec, numericColIdx, m_colName.getStringValue());
            final AssignTask task = new AssignTask(sql, numericColIdx, resultTableName);
            task.execute(sqlsc, resultSchema, model.getModel());
            JOptionPane.showMessageDialog(null, "End execution.");
    //        KMeansModel clusters = new KMeansModel(new Vector[] {new DenseVector(new double[] {1,0,1})});
            return new PortObject[] {new DatabasePortObject(createSQLSpec(db.getSpec(), getCredentialsProvider(),
                m_tableName.getStringValue(), m_colName.getStringValue()))};
            }
        }

    public static DatabasePortObjectSpec createSQLSpec(final DatabasePortObjectSpec spec, final CredentialsProvider cp,
        final String tableName, final String colName) throws InvalidSettingsException {
        final  DataTableSpec tableSpec = spec.getDataTableSpec();
        final ColumnRearranger rearranger = new ColumnRearranger(tableSpec);
        rearranger.append(new CellFactory() {
            @Override
            public void setProgress(final int curRowNr, final int rowCount, final RowKey lastKey,
                final ExecutionMonitor exec) {
            }
            @Override
            public DataColumnSpec[] getColumnSpecs() {
                final DataColumnSpecCreator creator =
                        new DataColumnSpecCreator(colName, StringCell.TYPE);
                return new DataColumnSpec[] {creator.createSpec()};
            }
            @Override
            public DataCell[] getCells(final DataRow row) {
                return null;
            }
        });
        final DatabaseQueryConnectionSettings conn = spec.getConnectionSettings(cp);
        final DatabaseUtility utility = DatabaseUtility.getUtility(DATABASE_IDENTIFIER);
        final StatementManipulator sm = utility.getStatementManipulator();
        conn.setQuery("select * from " + sm.quoteIdentifier(tableName));
        return new DatabasePortObjectSpec(rearranger.createSpec(), conn);
    }

    public static StructType createSchema(final DataTableSpec tableSpec, final Collection<Integer> numericColIdx,
        final String colName) {
        final List<StructField> fields = new ArrayList<>(tableSpec.getNumColumns() + 1);
        for (final DataColumnSpec colSpec : tableSpec) {
            fields.add(DataType.createStructField(colSpec.getName(), getSparkType(colSpec.getType()), true));
        }
        fields.add(DataType.createStructField(colName, DataType.StringType, false));
        StructType schema = DataType.createStructType(fields);
        return schema;
    }

    /**
     * @param type
     * @return
     */
    private static DataType getSparkType(final org.knime.core.data.DataType type) {
        if (type.isCompatible(DoubleValue.class)) {
            return DataType.DoubleType;
        }
        return DataType.StringType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
//        m_noOfCluster.saveSettingsTo(settings);
//        m_noOfIteration.saveSettingsTo(settings);
        m_tableName.saveSettingsTo(settings);
        m_colName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
//        m_noOfCluster.validateSettings(settings);
//        m_noOfIteration.validateSettings(settings);
        m_tableName.validateSettings(settings);
        m_colName.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
//        m_noOfCluster.loadSettingsFrom(settings);
//        m_noOfIteration.loadSettingsFrom(settings);
        m_tableName.loadSettingsFrom(settings);
        m_colName.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
    CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
    CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // nothing to do
    }
}
