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
package org.knime.bigdata.spark.core.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;

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
     * @param model PMML {@link CompiledModelPortObjectSpec}
     * @return the indices of the columns required by the compiled PMML model
     * @throws InvalidSettingsException if a required column is not present in the input table
     */
    public static Integer[] getColumnIndices(final DataTableSpec inputSpec, final CompiledModelPortObjectSpec model)
            throws InvalidSettingsException {
        return getColumnIndices(inputSpec, model, null);
    }

    /**
     * @param inputSpec {@link DataTableSpec}
     * @param model PMML {@link CompiledModelPortObjectSpec}
     * @param missingFieldNames <code>null</code> if missing indices should raise an exception. If not <code>null</code>
     * the list will contain all missing fields
     * @return the indices of the columns required by the compiled PMML model
     * @throws InvalidSettingsException if a required column is not present in the input table
     */
    public static Integer[] getColumnIndices(final DataTableSpec inputSpec, final CompiledModelPortObjectSpec model,
        final Collection<String> missingFieldNames)
            throws InvalidSettingsException {
        final HashMap<String, Integer> inputFields = model.getInputIndices();
        final Integer[] colIdxs = new Integer[inputFields.size()];
        for (Entry<String, Integer> entry : inputFields.entrySet()) {
            final String fieldName = entry.getKey();
            final int colIdx = inputSpec.findColumnIndex(fieldName);
            if (colIdx < 0) {
                if (missingFieldNames == null) {
                    throw new InvalidSettingsException("Column with name " + fieldName + " not found in input data");
                } else {
                    missingFieldNames.add(fieldName);
                }
            }
            colIdxs[entry.getValue()] = Integer.valueOf(colIdx);
        }
        return colIdxs;
    }

    /**
     * @param inSpec input {@link DataTableSpec}
     * @param cms the {@link CompiledModelPortObjectSpec}
     * @param predColName the name of the prediction column or <code>null</code> if the default should be used
     * @param outputProbabilities <code>true</code> if the probability columns should be appended
     * @param probColSuffix the suffix for the probability columns
     * @return the result {@link DataTableSpec}
     */
    public static DataTableSpec createPredictionResultSpec(final DataTableSpec inSpec,
        final CompiledModelPortObjectSpec cms, final String predColName, final boolean outputProbabilities,
        final String probColSuffix) {
        final DataColumnSpec[] pmmlColSpecs = cms.getResultColSpecs(inSpec, predColName, outputProbabilities, probColSuffix);
        return new DataTableSpec(inSpec, new DataTableSpec(pmmlColSpecs));
    }
}
