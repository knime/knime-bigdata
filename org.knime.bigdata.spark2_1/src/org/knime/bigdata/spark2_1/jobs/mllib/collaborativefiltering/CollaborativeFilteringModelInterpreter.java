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
 *   Created on 17.09.2015 by koetter
 */
package org.knime.bigdata.spark2_1.jobs.mllib.collaborativefiltering;

import org.knime.bigdata.spark.core.port.model.MLlibModel;
import org.knime.bigdata.spark.core.port.model.interpreter.HTMLModelInterpreter;
import org.knime.bigdata.spark.node.mllib.collaborativefiltering.MLlibCollaborativeFilteringNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class CollaborativeFilteringModelInterpreter extends HTMLModelInterpreter<MLlibModel> {

    private static final long serialVersionUID = 1L;

    private static volatile CollaborativeFilteringModelInterpreter instance =
            new CollaborativeFilteringModelInterpreter();

    private CollaborativeFilteringModelInterpreter() {
        // avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static CollaborativeFilteringModelInterpreter getInstance() {
        if (instance == null) {
            synchronized (CollaborativeFilteringModelInterpreter.class) {
                if (instance == null) {
                    instance = new CollaborativeFilteringModelInterpreter();
                }
            }
        }
        return instance;
    }

    @Override
    public String getModelName() {
        return MLlibCollaborativeFilteringNodeModel.MODEL_NAME;
    }

    @Override
    public String getSummary(final MLlibModel sparkModel) {
        final CollaborativeFilteringModel model = (CollaborativeFilteringModel) sparkModel.getModel();
        return "Rank: " + model.getRank() + ", User column: " + model.getUserFeaturesColumnName()
            + ", Product column: " + model.getProductFeaturesColumnName();
    }

    @Override
    protected String getHTMLDescription(final MLlibModel sparkModel) {
        return getSummary(sparkModel);
    }
}
