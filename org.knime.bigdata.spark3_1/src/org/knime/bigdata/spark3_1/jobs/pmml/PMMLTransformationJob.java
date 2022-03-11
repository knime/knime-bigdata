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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark3_1.jobs.pmml;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.pmml.transformation.PMMLTransformationJobInput;
import org.knime.bigdata.spark3_1.api.RowBuilder;

/**
 * Job that applies a compiled PMML transformation to its input data.
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class PMMLTransformationJob extends PMMLAssignJob<PMMLTransformationJobInput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(PMMLTransformationJob.class.getName());

    @Override
    protected MapFunction<Row, Row> createMapFunction(final Map<String, byte[]> bytecode, final PMMLTransformationJobInput input) {

        final String mainClass = input.getMainClass();

        // indices of input columns that should be fed to the PMML eval method.
        final ArrayList<Integer> inputColIdxs = new ArrayList<>(input.getColumnIdxs());

        LOGGER.debug("Creating PMML transformation function");

        final int noOfOutputColumns = input.getSpec(input.getFirstNamedOutputObject()).getNoOfFields();
        // these two array maps each output column index to its source which is either (a) an input column
        // or (b) a column from the PMML result.
        // In other words, a number in outputColumnSrcIdxs is either a column index from the input row,
        // or a column from the PMML result array.
        final int[] outputColumnSrcIdxs = new int[noOfOutputColumns];
        final boolean[] inputColumIsSource = new boolean[noOfOutputColumns];
        fillColumnMappingArrays(outputColumnSrcIdxs, inputColumIsSource, input);
        final boolean[] longColumns = input.getInputLongFields();

        return new MapFunction<Row, Row>() {
            private static final long serialVersionUID = 1L;

            //use transient since a Method can not be serialized
            private transient Method m_evalMethod;

            // use transient because we have to initialize this array per task anyway
            private transient Object[] m_evalMethodInput;

            @Override
            public Row call(final Row inputRow) throws Exception {
                if (m_evalMethod == null) {
                    m_evalMethod = loadCompiledPMMLEvalMethod(bytecode, mainClass);
                    m_evalMethodInput = new Object[inputColIdxs.size()];
                }

                fillEvalMethodInputFromRow(inputColIdxs, inputRow, m_evalMethodInput, longColumns);
                final Object[] pmmlResult = (Object[]) m_evalMethod.invoke(null, (Object)m_evalMethodInput);

                final RowBuilder rowBuilder = RowBuilder.emptyRow();
                for (int i=0; i<noOfOutputColumns; i++) {
                    final int srcIdx = outputColumnSrcIdxs[i];
                    if (inputColumIsSource[i]) {
                        rowBuilder.add(inputRow.get(srcIdx));
                    } else {
                        rowBuilder.add(pmmlResult[srcIdx]);
                    }
                }

                return rowBuilder.build();
            }
        };
    }

    /**
     * Computes how the output columns are derived. It analyzes {@link PMMLTransformationJobInput} and fills the given
     * arrays such that each number in outputColumnSrcIdxs is either a column index from the input row, or a column from
     * the PMML result array.
     *
     * @param outputColumnSrcIdxs Array to be filled with column indices from the input row or the PMML result array.
     * @param inputColumIsSource Array to be filled so as to specify which column indices in outputColumnSrcIdxs in
     *            reference the input row columns.
     * @param input The {@link PMMLTransformationJobInput} to analyze.
     */
    private void fillColumnMappingArrays(final int[] outputColumnSrcIdxs, final boolean[] inputColumIsSource,
        final PMMLTransformationJobInput input) {

        final boolean replace = input.replace();
        final int noOfInputCols = outputColumnSrcIdxs.length - input.getAdditionalColIdxs().size();

        // i is the current output column index
        int i = 0;

        // if we don't do column replacement, we will copy all columns from the input row
        // and only append new columns from the PMML result.
        if (!replace) {
            // the first noOfInputFields columns are taken directly from the input
            while(i < noOfInputCols) {
                outputColumnSrcIdxs[i] = i;
                inputColumIsSource[i] = true;
                i++;
            }
        } else {
            // if do column replacement, we will copy all columns from the input row
            // unless they should be
            //  (a) skipped (e.g. when doing Category2Number transformation with BINARY mode) or
            //  (b) replaced with a PMML result column.

            final Set<Integer> inputColIdxs2Skip = new HashSet<>(input.getSkippedColIdxs());
            final Map<Integer, Integer> inputColIdx2Replace = input.getReplaceColIdxs();

            for (int inputColIdx = 0; i < noOfInputCols; inputColIdx++) {
                if (inputColIdxs2Skip.contains(inputColIdx)) {
                    // skip the input column
                } else if (inputColIdx2Replace.containsKey(inputColIdx)) {
                    outputColumnSrcIdxs[i] = inputColIdx2Replace.get(inputColIdx);
                    inputColumIsSource[i] = false;
                    i++;
                } else {
                    outputColumnSrcIdxs[i] = inputColIdx;
                    inputColumIsSource[i] = true;
                    i++;
                }
            }
        }

        for (int pmmlResultColIdx : input.getAdditionalColIdxs()) {
            outputColumnSrcIdxs[i] = pmmlResultColIdx;
            inputColumIsSource[i] = false;
            i++;
        }
    }
}
