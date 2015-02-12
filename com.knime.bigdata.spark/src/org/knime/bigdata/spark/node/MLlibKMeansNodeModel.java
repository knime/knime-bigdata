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
package org.knime.bigdata.spark.node;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;
import org.knime.bigdata.spark.port.MLlibPortObject;
import org.knime.bigdata.spark.port.MLlibPortObjectSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;

import com.knime.bigdata.hive.utility.HiveUtility;


/**
 *
 * @author knime
 */
public class MLlibKMeansNodeModel extends NodeModel {

    /**
     *
     */
    public MLlibKMeansNodeModel() {
        super(new PortType[]{DatabaseConnectionPortObject.TYPE},
            new PortType[]{MLlibPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        DatabasePortObjectSpec spec = (DatabasePortObjectSpec) inSpecs[0];
        if (!spec.getDatabaseIdentifier().equals(HiveUtility.DATABASE_IDENTIFIER)) {
            throw new InvalidSettingsException("Only Hive connections are supported");
        }
        return new PortObjectSpec[] {createSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final DatabasePortObject db = (DatabasePortObject)inObjects[0];
        exec.setMessage("Connecting to Spark...");
        final SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("knimeTest");
//        conf.set("hive.metastore.uris", "thrift://192.168.56.101:9083");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        exec.checkCanceled();
        exec.setMessage("Connecting to Hive...");
        final JavaHiveContext sqlsc = new JavaHiveContext(sc);
        exec.setMessage("Execute Hive Query...");
        final String sql = db.getConnectionSettings(getCredentialsProvider()).getQuery();
        JavaSchemaRDD rdd = sqlsc.sql(sql);
        final List<Row> rows = rdd.collect();
        return new PortObject[] {new MLlibPortObject<KMeansModel>()};
    }

    /**
     * @return
     */
    private MLlibPortObjectSpec createSpec() {
        return new MLlibPortObjectSpec("kmeans");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // TODO Auto-generated method stub

    }

}
