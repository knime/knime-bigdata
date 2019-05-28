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
 *   Created on May 30, 2016 by bjoern
 */
package org.knime.bigdata.spark2_1.jobs.mllib.prediction.decisiontree;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;
import org.knime.bigdata.spark.core.job.util.ColumnBasedValueMappings;
import org.knime.bigdata.spark2_1.api.Spark_2_1_MLlibModelHelper;

/**
 * Abstract super class for decision tree model helpers, that handles metadata loading/saving.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public abstract class AbstractDecisionTreeModelHelper extends Spark_2_1_MLlibModelHelper {

    /**
     * @param modelName
     */
    public AbstractDecisionTreeModelHelper(final String modelName) {
        super(modelName);
    }

    @Override
    public Serializable loadMetaData(final InputStream inputStream) throws IOException {
        return ColumnBasedValueMappings.load(inputStream);
    }

    @Override
    public void saveModelMetadata(final OutputStream outputStream, final Serializable modelMetadata)
        throws IOException {
        ColumnBasedValueMappings.save(outputStream, (ColumnBasedValueMapping)modelMetadata);
    }
}
