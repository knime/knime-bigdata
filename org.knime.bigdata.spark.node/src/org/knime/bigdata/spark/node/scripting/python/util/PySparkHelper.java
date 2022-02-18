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
 *   Created on 31.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark.node.scripting.python.util;

import java.io.IOException;
import java.util.Optional;

import org.knime.bigdata.spark.core.version.SparkProvider;
import org.knime.core.node.InvalidSettingsException;
import org.knime.rsyntaxtextarea.guarded.GuardedDocument;

/**
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public interface PySparkHelper extends SparkProvider {

    /**
     * Creates a guarded PySparkDocument
     * @param inCount number of inputs
     * @param outCount number of outputs
     * @return the guarded Document
     */
    GuardedDocument createGuardedPySparkDocument(final int inCount, final int outCount);

    /**
     * Updates the dataFrame in and out section with the given UID
     * @param doc the document to update
     * @param inCount number of inputs
     * @param outCount number of outputs
     * @param uid the uid suffix for the dataframes in the exchanger
     */
    void updateGuardedSectionsUIDs(PySparkDocument doc, final int inCount, final int outCount, String uid);

    /**
     * Updates the Guarded sections of the given document
     * @param doc the document to update
     * @param inCount number of inputs
     * @param outCount number of outputs
     */
    void updateGuardedSection(PySparkDocument doc, final int inCount, final int outCount);

    /**
     * Checks whether all needed resultDataframes are in the UDF.
     * @param doc the Document to check
     * @param outCount the number of outpurFrames
     * @throws InvalidSettingsException thrown if the user code does not contain the result frame names
     */
    void checkUDF(PySparkDocument doc, int outCount) throws InvalidSettingsException;

    /**
     * Returns the path to the local PySpark libs if present
     * @return String with the path
     * @throws IOException if if an error occurs during the url conversion
     */
    Optional<String> getLocalPySparkPath() throws IOException;

}