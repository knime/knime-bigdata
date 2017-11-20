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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark2_1.jobs.preproc.convert.category2number;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;
import org.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobInput;
import org.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobOutput;
import org.knime.bigdata.spark2_1.api.MappedDatasetContainer;
import org.knime.bigdata.spark2_1.api.NamedObjects;
import org.knime.bigdata.spark2_1.api.RDDUtilsInJava;

/**
 * converts nominal values from a set of columns to numbers and adds corresponding new columns
 *
 * @author dwk
 */
@SparkClass
public class Category2NumberJob extends AbstractStringMapperJob {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(Category2NumberJob.class.getName());

    @Override
    protected Category2NumberJobOutput execute(final SparkContext aContext, final Category2NumberJobInput input, final NamedObjects namedObjects,
            final Dataset<Row> dataset, final int[] colIds) throws KNIMESparkException {

        final MappingType mappingType = input.getMappingType();

        //use only the column indices when converting
        final MappedDatasetContainer mappedData =
            RDDUtilsInJava.convertNominalValuesForSelectedIndices(dataset, colIds, mappingType, input.keepOriginalCols());

        final String outputName = input.getFirstNamedOutputObject();
        LOGGER.info("Storing mapped data under key: " + outputName);
        namedObjects.addDataFrame(outputName, mappedData.getDatasetWithConvertedValues());

        return new Category2NumberJobOutput(mappedData.getAppendedColumnNames(), mappedData.getMappings());
    }
}
