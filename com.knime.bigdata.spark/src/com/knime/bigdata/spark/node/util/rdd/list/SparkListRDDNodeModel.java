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
 *   Created on 28.08.2015 by koetter
 */
package com.knime.bigdata.spark.node.util.rdd.list;

import java.util.Set;

import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.port.SparkContextProvider;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkListRDDNodeModel extends SparkNodeModel {

    SparkListRDDNodeModel() {
        //TODO: Change to SparkContextPortObject once all other SparkObjects implement it
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[] {BufferedDataTable.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new PortObjectSpec[] {createSpec()};
    }

    /**
     * @return the result spec
     */
    private DataTableSpec createSpec() {
        final DataColumnSpecCreator creator = new DataColumnSpecCreator("RDD ID", StringCell.TYPE);
        return new DataTableSpec(creator.createSpec());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkContextProvider provider = (SparkContextProvider)inData[0];
        exec.setMessage("Fetching RDD list...");
        final Set<String> namedRDDs = KnimeContext.listNamedRDDs(provider.getContext());
        final BufferedDataContainer dc = exec.createDataContainer(createSpec());
        int idx = 0;
        for (String rdd : namedRDDs) {
            exec.setProgress(idx / (double) namedRDDs.size(), "Writing row " + idx + " of " + namedRDDs.size());
            exec.checkCanceled();
            dc.addRowToTable(new DefaultRow(RowKey.createRowKey(idx), new StringCell(rdd)));
            idx++;
        }
        dc.close();
        return new PortObject[] {dc.getTable()};
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
