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
 *   Created on 20.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.sorter;

import java.util.ArrayList;
import java.util.List;

import org.knime.base.node.preproc.sorter.SorterNodeDialogPanel2;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.ConvenienceMethods;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.job.SimpleJobRunFactory;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;
import com.knime.bigdata.spark.core.util.SparkIDs;
import com.knime.bigdata.spark.core.util.SparkUtil;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkSorterNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkSorterNodeModel.class.getCanonicalName();

    /**
     * The key for the IncludeList in the NodeSettings.
     */
    static final String INCLUDELIST_KEY = "incllist";

    /**
     * The key for the Sort Order Array in the NodeSettings.
     */
    static final String SORTORDER_KEY = "sortOrder";

    /**
     * Settings key: Sort missings always to end.
     * @since 2.6
     */
    static final String MISSING_TO_END_KEY = "missingToEnd";

    /*
     * List contains the data cells to include.
     */
    private List<String> m_inclList = null;

    /**
     * Array containing information about the sort order for each column. true:
     * ascending; false: descending
     */
    private boolean[] m_sortOrder = null;

    /** Move missing values always to end (overwrites natural ordering according
     * to which they are the smallest item).
     * @since 2.6
     */
    private boolean m_missingToEnd = false;

    /**
     * Constructor.
     */
    SparkSorterNodeModel() {
        super(new PortType[] {SparkDataPortObject.TYPE},  new PortType[] {SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input data available");
        }
        if (m_inclList == null) {
            throw new InvalidSettingsException("No selected columns to sort");
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec tableSpec = spec.getTableSpec();
        // check if the values of the include List exist in the DataTableSpec
        final List<String> notAvailableCols = new ArrayList<>();
        for (String ic : m_inclList) {
            if (!ic.equals(SorterNodeDialogPanel2.NOSORT.getName())
                    && !ic.equals(SorterNodeDialogPanel2.ROWKEY.getName())) {
                if (!tableSpec.containsName(ic)) {
                    notAvailableCols.add(ic);
                }
            }
        }
        if (!notAvailableCols.isEmpty()) {
            throw new InvalidSettingsException("The input table has "
               + "changed. Some columns are missing: "
               + ConvenienceMethods.getShortStringFrom(notAvailableCols, 3));
        }
        //the table structure does not change only the row sort order so we can return the input spec
        return inSpecs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec)
            throws Exception {
        if (inData == null || inData.length < 1 || inData[0] == null) {
            throw new InvalidSettingsException("No input data available");
        }
        exec.setMessage("Start sorting (Spark)");
        final SparkDataPortObject spark = (SparkDataPortObject) inData[0];
        final DataTableSpec tableSpec = spark.getTableSpec();
        final SparkDataTable data = spark.getData();
        String resultTableName = SparkIDs.createSparkDataObjectID();
        final Integer[] cols = SparkUtil.getColumnIndices(tableSpec, m_inclList);
        final Boolean[] sortDirection = SparkUtil.convert(m_sortOrder);

        final SparkContextID contextID = data.getContextID();
        final SimpleJobRunFactory<SortJobInput> runFactory = SparkContextUtil.getSimpleRunFactory(contextID, JOB_ID);
        final SortJobInput jobInput = new SortJobInput(data.getID(), resultTableName, cols, sortDirection, m_missingToEnd);
        runFactory.createRun(jobInput).run(contextID, exec);

        return new PortObject[] {createSparkPortObject(spark, resultTableName)};
    }

    /**
     * The list of included columns and their sort order are stored in the
     * settings.
     *
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        if (m_inclList != null) {
            settings.addStringArray(INCLUDELIST_KEY, m_inclList.toArray(new String[0]));
        }
        if (!(m_sortOrder == null)) {
            settings.addBooleanArray(SORTORDER_KEY, m_sortOrder);
        }
        settings.addBoolean(MISSING_TO_END_KEY, m_missingToEnd);
    }

    /**
     * Valid settings should contain the list of columns and a corresponding
     * sort order array of same size.
     *
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        final String[] inclList = settings.getStringArray(INCLUDELIST_KEY);
        if (inclList == null) {
            throw new InvalidSettingsException("No column selected.");
        }
        // scan fur duplicate entries in include list
        for (int i = 0; i < inclList.length; i++) {
            String entry = inclList[i];
            for (int j = i + 1; j < inclList.length; j++) {
                if (entry.equals(inclList[j])) {
                    throw new InvalidSettingsException("Duplicate column '"
                            + entry + "' at positions " + i + " and " + j);
                }
            }
        }
        final boolean[] sortorder = settings.getBooleanArray(SORTORDER_KEY);
        if (sortorder == null) {
            throw new InvalidSettingsException("No sort order specified.");
        }
    }

    /**
     * Load the settings (includelist and sort order) in the SorterNodeModel.
     *
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        assert (settings != null);
        m_inclList = new ArrayList<>();
        final String[] inclList = settings.getStringArray(INCLUDELIST_KEY);
        for (int i = 0; i < inclList.length; i++) {
            m_inclList.add(inclList[i]);
        }
        m_sortOrder = settings.getBooleanArray(SORTORDER_KEY);
        m_missingToEnd = settings.getBoolean(MISSING_TO_END_KEY, false);
    }
}
