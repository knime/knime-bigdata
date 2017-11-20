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
 *   Created on 29.09.2015 by koetter
 */
package org.knime.bigdata.spark.node.pmml.transformation;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkTransformationPMMLApplyNodeDialog extends DefaultNodeSettingsPane {


    /**
     * Constructor.
     */
    public SparkTransformationPMMLApplyNodeDialog() {
        addDialogComponent(new DialogComponentBoolean(AbstractSparkTransformationPMMLApplyNodeModel.createReplaceModel(),
            "Replace original columns"));
    }
}
