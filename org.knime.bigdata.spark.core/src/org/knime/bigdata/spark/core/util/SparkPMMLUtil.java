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
package com.knime.bigdata.spark.core.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
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
    public static Integer[] getColumnIndices(final DataTableSpec inputSpec, final CompiledModelPortObjectSpec model)
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
     * @param colIdxs the indices of the columns in the input spec
     * @param addCols
     * @param replace <code>true</code> if the transformed columns should be replaced
     * @param skipCols
     * @return the result {@link DataTableSpec}
     */
    public static DataTableSpec createTransformationResultSpec(final DataTableSpec inSpec,
        final CompiledModelPortObjectSpec cms, final Integer[] colIdxs, final List<Integer> addCols,
        final boolean replace, final List<Integer> skipCols) {
        final DataColumnSpec[] pmmlResultColSpecs = cms.getTransformationsResultColSpecs(inSpec);
        final Integer[] matchingColIdxs = findMatchingCols(inSpec, cms);
        if (replace) {
            final Set<Integer> skipColIdxs = new HashSet<Integer>(Arrays.asList(matchingColIdxs));
            List<DataColumnSpec> resultCols = new LinkedList<>();
            for (int i = 0; i < inSpec.getNumColumns(); i++) {
                final Integer colIdx = Integer.valueOf(i);
                if (skipColIdxs.contains(colIdx)) {
                    skipColIdxs.add(colIdx);
                    continue;
                }
                resultCols.add(inSpec.getColumnSpec(i));
            }
            for (int i = 0, length = matchingColIdxs.length; i < length; i++) {
                final int idx = matchingColIdxs[i];
                if (idx >= 0) {
                    //add only the specs to the result that have a matching input column
                    final DataColumnSpec pmmlSpec = pmmlResultColSpecs[i];
                    String pmmlName = pmmlSpec.getName();
                    if (pmmlName.endsWith("*")) {
                        DataColumnSpecCreator creator = new DataColumnSpecCreator(pmmlSpec);
                        creator.setName(pmmlName.substring(0, pmmlName.length() - 1));
                        resultCols.add(creator.createSpec());
                    } else {
                        resultCols.add(pmmlSpec);
                    }
                    addCols.add(Integer.valueOf(i));
                }
            }
            return new DataTableSpec(resultCols.toArray(new DataColumnSpec[0]));
        }
        final List<DataColumnSpec> appendSpecs = new ArrayList<>(pmmlResultColSpecs.length);
        for (int i = 0, length = matchingColIdxs.length; i < length; i++) {
            final int idx = matchingColIdxs[i];
            if (idx >= 0) {
                //add only the specs to the result that have a matching input column
                appendSpecs.add(pmmlResultColSpecs[i]);
                addCols.add(Integer.valueOf(i));
            }
        }
        return new DataTableSpec(inSpec, new DataTableSpec(appendSpecs.toArray(new DataColumnSpec[0])));
    }

    private static Integer[] findMatchingCols(final DataTableSpec inSpec, final CompiledModelPortObjectSpec cms) {
        final DataColumnSpec[] pmmlResultColSpecs = cms.getTransformationsResultColSpecs(inSpec);
        final Set<String> inputColNames = cms.getInputIndices().keySet();
        final Integer[] idxs = new Integer[pmmlResultColSpecs.length];
        Arrays.fill(idxs, Integer.valueOf(-1));
        for (int pmmlIdx = 0; pmmlIdx < pmmlResultColSpecs.length; pmmlIdx++) {
            final DataColumnSpec pmmlColSpec = pmmlResultColSpecs[pmmlIdx];
            final String pmmlColName = pmmlColSpec.getName();
            for (int specIdx = 0; specIdx < inSpec.getNumColumns(); specIdx++) {
                final DataColumnSpec colSpec = inSpec.getColumnSpec(specIdx);
                final String inputColName = colSpec.getName();
                if (pmmlColName.startsWith(inputColName) && inputColNames.contains(inputColName)) {
                    idxs[pmmlIdx] = Integer.valueOf(specIdx);
                    break;
                }
            }
        }
        return idxs;
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
