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
 *   Created on 28.08.2015 by koetter
 */
package org.knime.bigdata.spark.node.util.rdd.list;

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
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.SparkContextProvider;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkListRDDNodeModel extends SparkNodeModel {

    SparkListRDDNodeModel() {
        super(new PortType[]{SparkContextPortObject.TYPE}, new PortType[] {BufferedDataTable.TYPE}, false);
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
        final SparkContextID contextID = provider.getContextID();
        final SparkContext context = SparkContextManager.getOrCreateSparkContext(contextID);
        Set<String> namedObjects = context.getNamedObjects();
        final BufferedDataContainer dc = exec.createDataContainer(createSpec());
        long idx = 0;
        for (final String object : namedObjects) {
            exec.setProgress(idx / (double) namedObjects.size(), "Writing row " + idx + " of " + namedObjects.size());
            exec.checkCanceled();
            dc.addRowToTable(new DefaultRow(RowKey.createRowKey(idx), new StringCell(object)));
            idx++;
        }
        dc.close();
        return new PortObject[] {dc.getTable()};
    }
}
