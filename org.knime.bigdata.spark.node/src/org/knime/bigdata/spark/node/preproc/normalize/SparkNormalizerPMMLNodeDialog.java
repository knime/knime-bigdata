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
 *   Created on 03.08.2015 by dwk
 */
package org.knime.bigdata.spark.node.preproc.normalize;

import org.knime.base.node.preproc.normalize3.Normalizer3NodeDialog;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;

/**
 * Extends the {@link Normalizer3NodeDialog} for the {@link SparkNormalizerPMMLNodeModel}.
 * 
 * @author Ole Ostergaard
 */
public class SparkNormalizerPMMLNodeDialog  extends Normalizer3NodeDialog {

	/**
	 * This overrides the getTableSpec since we need the {@link DataTableSpec} from the {@link SparkDataPortObjectSpec}
	 */
    @Override
    protected DataTableSpec getTableSpec(final PortObjectSpec[] specs) throws NotConfigurableException{
        if (specs == null || specs.length <= 0 || specs[0] == null) {
            throw new NotConfigurableException("No input Spark RDD available");
        }
        final DataTableSpec spec = ((SparkDataPortObjectSpec)specs[0]).getTableSpec();
        if (spec == null || spec.getNumColumns() == 0) {
            throw new NotConfigurableException("No columns available for "
                    + "selection.");
        }
        return spec;
    }
}
