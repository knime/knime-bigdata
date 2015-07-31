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

import org.apache.spark.mllib.clustering.KMeansModel;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.util.SparkIDs;

/**
 *
 * @author koetter
 */
public class MLlibClusterAssignerNodeModel extends AbstractSparkNodeModel {

    private final SettingsModelString m_colName = createColumnNameModel();

    /**Constructor.*/
    public MLlibClusterAssignerNodeModel() {
        super(new PortType[]{SparkModelPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelString createColumnNameModel() {
        return new SettingsModelString("columnName", "Cluster");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("Input missing");
        }
        final SparkDataPortObjectSpec inputRDD = (SparkDataPortObjectSpec)inSpecs[1];
        final DataTableSpec resultTableSpec = createSpec(inputRDD.getTableSpec());
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(inputRDD.getContext(), resultTableSpec)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        @SuppressWarnings("unchecked")
        final SparkModel<KMeansModel> model = ((SparkModelPortObject<KMeansModel>)inObjects[0]).getModel();
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[1];
        exec.checkCanceled();
        exec.setMessage("Starting Spark Predictor");
        final DataTableSpec inputSpec = data.getTableSpec();
        final Integer[] colIdxs = model.getLearningColumnIndices(inputSpec);
        final DataTableSpec resultSpec = createSpec(inputSpec);
        final String aOutputTableName = SparkIDs.createRDDID();
        final SparkDataTable resultRDD = new SparkDataTable(data.getContext(), aOutputTableName, resultSpec);
        final AssignTask task = new AssignTask();
        task.execute(exec, data.getData(), model.getModel(), colIdxs, resultRDD);
        exec.setMessage("Spark Predictor finished.");
        return new PortObject[]{new SparkDataPortObject(resultRDD)};
    }

    /**
     * @param inputSpec the input data spec
     * @return the result spec
     */
    public DataTableSpec createSpec(final DataTableSpec inputSpec) {
        return createSpec(inputSpec, m_colName.getStringValue());
    }

    /**
     * Creates the typical cluster assignment spec with the cluster column as last column of type string.
     * @param inputSpec the input data spec
     * @param resultColName the name of the result column
     * @return the result spec
     */
    public static DataTableSpec createSpec(final DataTableSpec inputSpec, final String resultColName) {
        //TK_TODO: return Cluster instead of cluster
        final String clusterColName = DataTableSpec.getUniqueColumnName(inputSpec, resultColName);
        final DataColumnSpecCreator creator = new DataColumnSpecCreator(clusterColName, StringCell.TYPE);
        final DataColumnSpec labelColSpec = creator.createSpec();
        return new DataTableSpec(inputSpec, new DataTableSpec(labelColSpec));
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_colName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_colName.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_colName.loadSettingsFrom(settings);
    }
}
