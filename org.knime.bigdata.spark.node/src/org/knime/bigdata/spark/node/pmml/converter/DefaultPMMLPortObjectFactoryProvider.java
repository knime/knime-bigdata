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
 *   Created on 16.05.2016 by koetter
 */
package org.knime.bigdata.spark.node.pmml.converter;

import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.DefaultSparkProvider;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class DefaultPMMLPortObjectFactoryProvider extends DefaultSparkProvider<PMMLPortObjectFactory>
implements PMMLPortObjectFactoryProvider {

    /**
     * @param checker the {@link CompatibilityChecker}
     * @param elements of type {@link PMMLPortObjectFactory}
     */
    protected DefaultPMMLPortObjectFactoryProvider(final CompatibilityChecker checker, final PMMLPortObjectFactory... elements) {
        super(checker, elements);
    }

}
