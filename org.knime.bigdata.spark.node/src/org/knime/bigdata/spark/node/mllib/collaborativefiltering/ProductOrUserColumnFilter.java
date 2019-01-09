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
 *   Created on Jan 9, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.mllib.collaborativefiltering;

import java.util.Objects;

import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.LongValue;
import org.knime.core.node.util.ColumnFilter;

/**
 * Column filter that includes integer and long columns for products or users, but does not include boolean columns.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class ProductOrUserColumnFilter implements ColumnFilter {

    @Override
    public boolean includeColumn(final DataColumnSpec colSpec) {
        Objects.requireNonNull(colSpec, "Column spec required");
        return colSpec.getType().isCompatible(LongValue.class) && !colSpec.getType().isCompatible(BooleanValue.class);
    }

    @Override
    public String allFilteredMsg() {
        return "No compatible integer or long column for users and products found in input table spec.";
    }
}
