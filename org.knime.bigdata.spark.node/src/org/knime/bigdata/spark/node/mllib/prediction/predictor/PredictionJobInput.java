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
package org.knime.bigdata.spark.node.mllib.prediction.predictor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class PredictionJobInput extends JobInput {

    private final static String KEY_INCLUDE_COLUMN_INDICES = "includeColIdxs";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public PredictionJobInput() {
    }

    /**
     * @param namedInputObject the unique name of the input object
     * @param colIdxs the column indices to us for prediction
     * @param namedOutputObject the unique name of the output object
     * @param outputSpec output table spec
     */
    public PredictionJobInput(final String namedInputObject, final Integer[] colIdxs,
            final String namedOutputObject, final IntermediateSpec outputSpec) {

        if (colIdxs == null || colIdxs.length < 1) {
            throw new IllegalArgumentException("Prediction columns must not be null or empty");
        }

        addNamedInputObject(namedInputObject);
        set(KEY_INCLUDE_COLUMN_INDICES, Arrays.asList(colIdxs));
        addNamedOutputObject(namedOutputObject);
        withSpec(namedOutputObject, outputSpec);
    }


    /**
     * @return the indices of the columns to use for prediction
     */
    public List<Integer> getIncludeColumnIndices() {
        return get(KEY_INCLUDE_COLUMN_INDICES);
    }

    /**
     * @param columns - All column names in data frame
     * @return The column names of the columns to use for predictions
     */
    public String[] getIncludeColumnNames(final String columns[]) {
        final List<Integer> columnIndices = getIncludeColumnIndices();
        final String columnNames[] = new String[columnIndices.size()];
        for (int i = 0; i < columnIndices.size(); i++) {
            columnNames[i] = columns[columnIndices.get(i)];
        }
        return columnNames;
    }

    /**
     * @return the name of the result column, containing predictions
     */
    public String getPredictionColumnName() {
        final IntermediateField fields[] = getSpec(getFirstNamedOutputObject()).getFields();
        return fields[fields.length - 1].getName();
    }

    /**
     * @param model the {@link Serializable} model
     * @return temporary file contains serializable model
     * @throws KNIMESparkException
     */
    public File writeModelIntoTemporaryFile(final Serializable model) throws KNIMESparkException {
        try {
            final File outFile = File.createTempFile("knime-sparkModel", ".tmp");
            try (final ObjectOutputStream out
                    = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)));){
                out.writeObject(model);
                out.close();
            }
            return outFile;
        } catch(IOException e) {
            throw new KNIMESparkException("Failed to write model into temporary file.", e);
        }
    }

    /**
     * @param inFile file contains serializable model
     * @return the {@link Serializable} that represents the Spark model
     * @throws KNIMESparkException
     */
    public Serializable readModelFromTemporaryFile(final File inFile) throws KNIMESparkException {
        try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(inFile)))) {
            Serializable model = (Serializable) in.readObject();
            in.close();
            return model;

        } catch(IOException|ClassNotFoundException e) {
            throw new KNIMESparkException("Failed to read model from " + inFile.getAbsolutePath(), e);
        }
    }
}
