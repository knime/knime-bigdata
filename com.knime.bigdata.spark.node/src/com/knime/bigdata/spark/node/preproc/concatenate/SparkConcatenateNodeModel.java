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
 *   Created on 13.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.concatenate;

import java.util.LinkedList;
import java.util.List;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.job.SimpleJobRunFactory;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkConcatenateNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkConcatenateNodeModel.class.getCanonicalName();

    /**
     * Constructor.
     */
    SparkConcatenateNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE_OPTIONAL,
            SparkDataPortObject.TYPE_OPTIONAL, SparkDataPortObject.TYPE_OPTIONAL},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1) {
            return null;
        }
        return new PortObjectSpec[] {inSpecs[0]};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final List<SparkDataTable> rdds = new LinkedList<>();
        final List<String> rddIds = new LinkedList<>();
        SparkContextID context = null;
        DataTableSpec spec = null;
        for (PortObject in : inData) {
            if (in instanceof SparkDataPortObject) {
                final SparkDataTable sparkRDD = ((SparkDataPortObject)in).getData();
                if (context == null || spec == null) {
                    context = sparkRDD.getContextID();
                    spec = sparkRDD.getTableSpec();
                } else {
                    context.equals(sparkRDD.getContextID());
                    if (!spec.equalStructure(sparkRDD.getTableSpec())) {
                        throw new InvalidSettingsException("Incompatible input specs found.");
                    }
                }
                rdds.add(sparkRDD);
                rddIds.add(sparkRDD.getID());
            }
        }
        if (rdds.isEmpty()) {
            throw new InvalidSettingsException("No valid input RDDs found");
        }
        final SparkDataTable resultTable;
        if (rdds.size() == 1) {
            setWarningMessage("Only one valid input RDD found. Node returns unaltered input RDD.");
            resultTable = rdds.get(0);
            setDeleteOnReset(false);
        } else {
            resultTable = new SparkDataTable(context, spec);
            final SimpleJobRunFactory<ConcatenateRDDsJobInput> runFactory = SparkContextUtil.getSimpleRunFactory(context, JOB_ID);
            final ConcatenateRDDsJobInput jobInput = new ConcatenateRDDsJobInput(rddIds.toArray(new String[0]), resultTable.getID());
            runFactory.createRun(jobInput).run(context, exec);
            setDeleteOnReset(true);
        }
        return new PortObject[] {new SparkDataPortObject(resultTable)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        //nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        //nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        //nothing to do
    }
}
