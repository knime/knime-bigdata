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
 *   Created on 29.05.2015 by koetter
 */
package com.knime.bigdata.spark.node.scripting.snippet;

import java.util.LinkedList;
import java.util.List;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.node.scripting.AbstractSparkSnippetNodeModel;
import com.knime.bigdata.spark.port.data.AbstractSparkRDD;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDGenerator;

/**
 *
 * @author koetter
 */
public class SparkSnippetNodeModel extends AbstractSparkSnippetNodeModel {

    /**Constructor. */
    public SparkSnippetNodeModel() {
        super(new PortType[] {SparkDataPortObject.TYPE, SparkDataPortObject.TYPE_OPTIONAL},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        SparkDataPortObject data1 = (SparkDataPortObject)inData[0];
        AbstractSparkRDD table1 = data1.getData();
        SparkDataPortObject data2 = (SparkDataPortObject)inData[1];
        SparkDataTable table2 = null;
        if (data2 != null) {
            //we have two incoming RDDs
            table2 = data2.getData();
            if (!table1.compatible(table2)) {
                throw new InvalidSettingsException("Input objects belong to two different Spark contexts");
            }
        }
        String tableName = SparkIDGenerator.createID();
        String jobDescription = getCode();

        //call the Spark job with the two rdds and use the tableName as id for the result RDD and
        //the job description as source code for the job


        final List<DataColumnSpec> specs = new LinkedList<>();
        final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("Test", StringCell.TYPE);
        specs.add(specCreator.createSpec());
        specCreator.setName("Test2");
        specCreator.setType(DoubleCell.TYPE);
        specs.add(specCreator.createSpec());
        final DataTableSpec resultSpec = new DataTableSpec(specs.toArray(new DataColumnSpec[0]));
        SparkDataTable resultTable = new SparkDataTable(data1.getContext(), tableName, resultSpec);
        final SparkDataPortObject resultObject = new SparkDataPortObject(resultTable);
        return new PortObject[] {resultObject};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new PortObjectSpec[] {null};
    }

}
