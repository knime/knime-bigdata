/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
 *   Created on 16.05.2016 by koetter
 */
package org.knime.bigdata.spark.node.preproc.filter.column;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.job.ColumnsJobInput;
import org.knime.bigdata.spark.core.job.SimpleJobRunFactory;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.util.SparkUtil;

/**
 * Abstract class that provides a SparkJob to filter columns from a given Spark object.
 * Extending classes only need to provide the {@link ColumnRearranger} to use.
 * @author Tobias Koetter, KNIME.com
 */
public abstract class AbstractSparkColumnFilterNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = AbstractSparkColumnFilterNodeModel.class.getCanonicalName();

    private int m_sparkInputIdx;

    /**
     * @param inPortTypes
     * @param outPortTypes
     * @param sparkInputIdx
     */
    protected AbstractSparkColumnFilterNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
        final int sparkInputIdx) {
        super(inPortTypes, outPortTypes);
        m_sparkInputIdx = sparkInputIdx;
    }

    /**
     * @return the sparkInputIdx
     */
    public int getSparkInputIdx() {
        return m_sparkInputIdx;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length <= getSparkInputIdx() || inSpecs[getSparkInputIdx()] == null) {
            throw new InvalidSettingsException("No input Spark RDD available");
        }
        final ColumnRearranger c = createColumnRearranger(inSpecs);
        if (c == null) {
            return null;
        }
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec)inSpecs[m_sparkInputIdx];
        return new PortObjectSpec[]{
            new SparkDataPortObjectSpec(sparkSpec.getContextID(), c.createSpec())};
    }

    /**
     * Called from the {@link #configureInternal(PortObjectSpec[])} method
     * @param inSpecs the input {@link PortObjectSpec} array
     * @return the {@link ColumnRearranger} or <code>null</code> if it can not be determined
     * @throws InvalidSettingsException if the settings aren't valid
     */
    protected abstract ColumnRearranger createColumnRearranger(final PortObjectSpec[] inSpecs)
    throws InvalidSettingsException;

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject) inData[m_sparkInputIdx];
        final SparkDataTable sparkRDD = rdd.getData();
        final SparkContextID context = sparkRDD.getContextID();
        final ColumnRearranger c = createColumnRearranger(inData);
        final Integer[] columnIndices =
                SparkUtil.getColumnIndices(rdd.getTableSpec(), c.createSpec().getColumnNames());
        final DataTableSpec resultSpec = c.createSpec();
        final SparkDataTable resultTable = new SparkDataTable(context, resultSpec);
        final SimpleJobRunFactory<ColumnsJobInput> runfactory =
                SparkContextUtil.getSimpleRunFactory(context, JOB_ID);
        final ColumnsJobInput jobInput = new ColumnsJobInput(sparkRDD.getID(), resultTable.getID(), columnIndices);
        runfactory.createRun(jobInput).run(context, exec);

        return new PortObject[]{new SparkDataPortObject(resultTable)};
    }

    /**
     * Called from the {@link #executeInternal(PortObject[], ExecutionContext)} method
     * @param inData the {@link PortObject} input array
     * @return the {@link ColumnRearranger}
     * @throws InvalidSettingsException if the settings aren't valid
     */
    protected abstract ColumnRearranger createColumnRearranger(final PortObject[] inData)
    throws InvalidSettingsException;
}