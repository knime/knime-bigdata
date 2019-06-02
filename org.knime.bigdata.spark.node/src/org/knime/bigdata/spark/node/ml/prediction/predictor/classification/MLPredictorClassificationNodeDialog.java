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
 *   Created on 30.07.2015 by koetter
 */
package org.knime.bigdata.spark.node.ml.prediction.predictor.classification;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentString;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class MLPredictorClassificationNodeDialog extends DefaultNodeSettingsPane {

    private final MLPredictorClassificationNodeSettings m_settings = new MLPredictorClassificationNodeSettings();

    MLPredictorClassificationNodeDialog() {
        addDialogComponent(new DialogComponentBoolean(m_settings.getOverwritePredictionColumnModel(),
            "Change prediction column name"));
        addDialogComponent(
            new DialogComponentString(m_settings.getPredictionColumnModel(), "Prediction column: ", true, 30));

        addDialogComponent(new DialogComponentBoolean(m_settings.getAppendClassProbabilityColumnsModel(),
            "Append individual class probabilities"));
        addDialogComponent(
            new DialogComponentString(m_settings.getProbabilityColumnSuffixModel(), "Suffix for probability columns: ", false, 30));
    }
}
