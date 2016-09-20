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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.predictor;

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

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;

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
     * @param model the MLlib model to use for prediction
     * @param colIdxs the column indices to us for prediction
     * @param namedOutputObject the unique name of the output object
     */
    public PredictionJobInput(final String namedInputObject,
        final Integer[] colIdxs, final String namedOutputObject) {
        if (colIdxs == null || colIdxs.length < 1) {
            throw new IllegalArgumentException("Prediction columns must not be null or empty");
        }
        addNamedInputObject(namedInputObject);
        addNamedOutputObject(namedOutputObject);
        set(KEY_INCLUDE_COLUMN_INDICES, Arrays.asList(colIdxs));
    }


    /**
     * @return the indices of the columns to use for prediction
     */
    public List<Integer> getIncludeColumnIndices() {
        return get(KEY_INCLUDE_COLUMN_INDICES);
    }

    /**
     * @return temporary file contains serializable model
     */
    public File writeModelIntoTemporaryFile(final Serializable model) throws KNIMESparkException {
        try {
            File outFile = File.createTempFile("knime-sparkModel", ".tmp");
            ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)));
            out.writeObject(model);
            out.close();

            return outFile;
        } catch(IOException e) {
            throw new KNIMESparkException("Failed to write model into temporary file.", e);
        }
    }

    /**
     * @param inFile file contains serializable model
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
