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
package com.knime.bigdata.spark.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.knime.base.pmml.translation.CompiledModel;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;

import com.knime.pmml.compilation.java.compile.CompiledModelPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public final class SparkPMMLUtil {

    /**
     *
     */
    private SparkPMMLUtil() {
        // perfent object creation
    }

    /**
     * @param inputSpec {@link DataTableSpec}
     * @param model PMML {@link CompiledModel}
     * @return the indices of the columns required by the compiled PMML model
     * @throws InvalidSettingsException if a required column is not present in the input table
     */
    public static Integer[] getColumnIndices(final DataTableSpec inputSpec, final CompiledModel model)
            throws InvalidSettingsException {
        return getColumnIndices(inputSpec, model, null);
    }

    /**
     * @param inputSpec {@link DataTableSpec}
     * @param model PMML {@link CompiledModel}
     * @param missingFieldNames <code>null</code> if missing indices should raise an exception. If not <code>null</code>
     * the list will contain all missing fields
     * @return the indices of the columns required by the compiled PMML model
     * @throws InvalidSettingsException if a required column is not present in the input table
     */
    public static Integer[] getColumnIndices(final DataTableSpec inputSpec, final CompiledModel model,
        final Collection<String> missingFieldNames)
            throws InvalidSettingsException {
        final String[] inputFields = model.getInputFields();
        final Integer[] colIdxs = new Integer[inputFields.length];
        for (String fieldName : inputFields) {
            final int colIdx = inputSpec.findColumnIndex(fieldName);
            if (colIdx < 0) {
                if (missingFieldNames == null) {
                    throw new InvalidSettingsException("Column with name " + fieldName + " not found in input data");
                } else {
                    missingFieldNames.add(fieldName);
                }
            }
            colIdxs[model.getInputFieldIndex(fieldName)] = Integer.valueOf(colIdx);
        }
        return colIdxs;
    }

    /**
     * @param inSpec input {@link DataTableSpec}
     * @param cms the {@link CompiledModelPortObjectSpec}
     * @param colIdxs the indices of the columns in the input spec
     * @return the result {@link DataTableSpec}
     */
    public static DataTableSpec createResultSpec(final DataTableSpec inSpec, final CompiledModelPortObjectSpec cms,
        final Integer[] colIdxs) {
        final DataColumnSpec[] specs = cms.getTransformationsResultColSpecs(inSpec);
        List<DataColumnSpec> pmmlSpecs = new ArrayList<>(specs.length);
        for (int i = 0; i < colIdxs.length; i++) {
            if (colIdxs[i] >= 0) {
                //add only result columns to the output column list that are also present in the input table
                pmmlSpecs.add(specs[i]);
            }
        }
        return new DataTableSpec(inSpec, new DataTableSpec(pmmlSpecs.toArray(new DataColumnSpec[0])));
    }
}
