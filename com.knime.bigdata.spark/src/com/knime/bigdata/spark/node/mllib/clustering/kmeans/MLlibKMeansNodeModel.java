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
 *   Created on Feb 12, 2015 by knime
 */
package com.knime.bigdata.spark.node.mllib.clustering.kmeans;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
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
import org.knime.core.data.def.IntCell;
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

import com.knime.bigdata.hive.utility.HiveUtility;
import com.knime.bigdata.spark.port.MLlibModel;
import com.knime.bigdata.spark.port.MLlibPortObject;
import com.knime.bigdata.spark.port.MLlibPortObjectSpec;


/**
 *
 * @author knime
 */
public class MLlibKMeansNodeModel extends NodeModel {

    private static final String DATABASE_IDENTIFIER = HiveUtility.DATABASE_IDENTIFIER;
    private final SettingsModelIntegerBounded m_noOfCluster = createNoOfClusterModel();
    private final SettingsModelIntegerBounded m_noOfIteration = createNoOfIterationModel();
    private final SettingsModelString m_tableName = createTableNameModel();
    private final SettingsModelString m_colName = createColumnNameModel();

    /**
     *
     */
    public MLlibKMeansNodeModel() {
        super(new PortType[]{DatabasePortObject.TYPE},
            new PortType[]{DatabasePortObject.TYPE, MLlibPortObject.TYPE});
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
        DatabasePortObjectSpec spec = (DatabasePortObjectSpec) inSpecs[0];
        if (!spec.getDatabaseIdentifier().equals(DATABASE_IDENTIFIER)) {
            throw new InvalidSettingsException("Only Hive connections are supported");
        }
        return new PortObjectSpec[] {createSQLSpec(spec), createMLSpec()};
    }

    private DatabasePortObjectSpec createSQLSpec(final DatabasePortObjectSpec spec) throws InvalidSettingsException {
        final DataTableSpec tableSpec = spec.getDataTableSpec();
        final ColumnRearranger rearranger = new ColumnRearranger(tableSpec);
        rearranger.append(new CellFactory() {
            @Override
            public void setProgress(final int curRowNr, final int rowCount, final RowKey lastKey,
                final ExecutionMonitor exec) {
            }
            @Override
            public DataColumnSpec[] getColumnSpecs() {
                final DataColumnSpecCreator creator =
                        new DataColumnSpecCreator(m_colName.getStringValue(), IntCell.TYPE);
                return new DataColumnSpec[] {creator.createSpec()};
            }
            @Override
            public DataCell[] getCells(final DataRow row) {
                return null;
            }
        });
        final DatabaseQueryConnectionSettings conn = spec.getConnectionSettings(getCredentialsProvider());
        final DatabaseUtility utility = DatabaseUtility.getUtility(DATABASE_IDENTIFIER);
        final StatementManipulator sm = utility.getStatementManipulator();
        conn.setQuery("select * from " + sm.quoteIdentifier(m_tableName.getStringValue()));
        return new DatabasePortObjectSpec(rearranger.createSpec(), conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final DatabasePortObject db = (DatabasePortObject)inObjects[0];
        final DataTableSpec tableSpec = db.getSpec().getDataTableSpec();
        final Collection<Integer> numericColIdx = new LinkedList<>();
        int counter = 0;
        for (DataColumnSpec colSpec : tableSpec) {
            if (colSpec.getType().isCompatible(DoubleValue.class)) {
                numericColIdx.add(Integer.valueOf(counter));
            }
            counter++;
        }
        final String sql = db.getConnectionSettings(getCredentialsProvider()).getQuery();
        exec.setMessage("Connecting to Spark...");
        final SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("knimeTest");
//        conf.set("hive.metastore.uris", "thrift://192.168.56.101:9083");
        try (final JavaSparkContext sc = new JavaSparkContext(conf);) {
        exec.checkCanceled();
        exec.setMessage("Connecting to Hive...");
        final JavaHiveContext sqlsc = new JavaHiveContext(sc);
        exec.setMessage("Execute Hive Query...");
        final JavaSchemaRDD inputData = sqlsc.sql(sql);
        final JavaRDD<Vector> parsedData = inputData.map(
            new Function<Row, Vector>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Vector call(final Row r) {
                    final double[] vals = new double[numericColIdx.size()];
                    int colCount = 0;
                    for (Integer id : numericColIdx) {
                        vals[colCount++] = r.getDouble(id.intValue());
                    }
                    return Vectors.dense(vals);
                }
            });
        parsedData.cache();
     // Cluster the data into two classes using KMeans
        int numClusters = 4;
        int numIterations = 20;
        final KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
        final JavaRDD<Row> predictedData = parsedData.map(new Function<Vector, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Row call(final Vector v) {
                final int cluster = clusters.predict(v);
                final Object[] vals = new Double[v.size() + 1];
                int valCount = 0;
                for (Object o : v.toArray()) {
                    vals[valCount++] = o;
                }
                vals[valCount++] = cluster;
                return Row.create(vals);
            }
        });
        StructType schema = createSchema(tableSpec, numericColIdx);
        JavaSchemaRDD schemaPredictedData = sqlsc.applySchema(predictedData, schema);
        schemaPredictedData.saveAsTable(m_tableName.getStringValue());
//        KMeansModel clusters = new KMeansModel(new Vector[] {new DenseVector(new double[] {1,0,1})});
        return new PortObject[] {new DatabasePortObject(createSQLSpec(db.getSpec())),
            new MLlibPortObject<>(new MLlibModel<>("KMeans", clusters))};
        }
    }

    /**
     * @param numericColIdx
     * @param tableSpec
     * @return
     */
    private StructType createSchema(final DataTableSpec tableSpec, final Collection<Integer> numericColIdx) {
        final List<StructField> fields = new ArrayList<>(numericColIdx.size() + 1);
        for (final Integer id : numericColIdx) {
            final DataColumnSpec colSpec = tableSpec.getColumnSpec(id.intValue());
            fields.add(DataType.createStructField(colSpec.getName(), getSparkType(colSpec.getType()), true));
        }
        fields.add(DataType.createStructField(m_colName.getStringValue(), DataType.IntegerType, false));
        StructType schema = DataType.createStructType(fields);
        return schema;
    }

    /**
     * @param type
     * @return
     */
    private DataType getSparkType(final org.knime.core.data.DataType type) {
        if (type.isCompatible(DoubleValue.class)) {
            return DataType.DoubleType;
        }
        return DataType.StringType;
    }

    /**
     * @return
     */
    private MLlibPortObjectSpec createMLSpec() {
        return new MLlibPortObjectSpec("kmeans");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_noOfCluster.saveSettingsTo(settings);
        m_noOfIteration.saveSettingsTo(settings);
        m_tableName.saveSettingsTo(settings);
        m_colName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noOfCluster.validateSettings(settings);
        m_noOfIteration.validateSettings(settings);
        m_tableName.validateSettings(settings);
        m_colName.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noOfCluster.loadSettingsFrom(settings);
        m_noOfIteration.loadSettingsFrom(settings);
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
