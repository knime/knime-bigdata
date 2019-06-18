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
 *   Created on May 31, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction;

import java.util.LinkedList;
import java.util.List;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.InvalidSettingsException;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class MLPredictionUtils {

    /**
     * Checks that all feature columns given by learningColumns are present with a compatible type in the
     * given inputSpec.
     *
     * @param learningColumns
     * @param inputSpec
     * @throws InvalidSettingsException
     */
    public static void checkFeatureColumns(final DataTableSpec learningColumns, final DataTableSpec inputSpec)
        throws InvalidSettingsException {

        // first ensure presence of all required feature columns
        final List<String> missingFeatures = new LinkedList<>();
        for (String requiredFeature : learningColumns.getColumnNames()) {
            if (!inputSpec.containsName(requiredFeature)) {
                missingFeatures.add(requiredFeature);
            }
        }

        if (!missingFeatures.isEmpty()) {
            String msg = String.format("%d feature columns are missing in input data: %s", missingFeatures.size(),
                summarizeList(missingFeatures));
            throw new InvalidSettingsException(msg);
        }

        for (String requiredFeature : learningColumns.getColumnNames()) {
            final DataColumnSpec requiredSpec = learningColumns.getColumnSpec(requiredFeature);
            final DataColumnSpec actualSpec = inputSpec.getColumnSpec(requiredFeature);
            // we have a problem if this column to predict on is string-based,
            // but the column we learned on was not. In this case the pipeline model
            // does not have the neccessary StringIndexer model.
            if (StringCell.TYPE.isASuperTypeOf(actualSpec.getType())
                && DoubleCell.TYPE.isASuperTypeOf(requiredSpec.getType())) {
                throw new InvalidSettingsException(
                    String.format("Column %s must not be of type %s, as it was numeric during model training.",
                        requiredFeature, actualSpec.getType().toPrettyString()));
            }
        }

    }

    private static String summarizeList(final List<String> list) {
        if (list.size() <= 5) {
            return String.join(", ", list);
        } else {
            return String.join(", ", list.subList(0, 5)) + ", ...";
        }

    }
}
