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
 *   Created on 29.04.2016 by koetter
 */
package org.knime.bigdata.spark3_3.jobs.preproc.filter.column;

import org.knime.bigdata.spark.core.job.ColumnsJobInput;
import org.knime.bigdata.spark.core.job.DefaultSimpleJobRunFactory;
import org.knime.bigdata.spark.node.preproc.filter.column.AbstractSparkColumnFilterNodeModel;

/**
 *
 * @author Ole Ostergaard
 */
public class ColumnFilterJobRunFactory extends DefaultSimpleJobRunFactory<ColumnsJobInput> {

    /**
     *
     */
    public ColumnFilterJobRunFactory() {
        super(AbstractSparkColumnFilterNodeModel.JOB_ID, ColumnFilterJob.class);
    }
}
