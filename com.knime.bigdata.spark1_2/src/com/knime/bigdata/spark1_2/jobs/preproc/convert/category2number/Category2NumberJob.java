/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
package com.knime.bigdata.spark1_2.jobs.preproc.convert.category2number;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;
import com.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobInput;
import com.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobOutput;
import com.knime.bigdata.spark1_2.api.MappedRDDContainer;
import com.knime.bigdata.spark1_2.api.NamedObjects;
import com.knime.bigdata.spark1_2.api.RDDUtilsInJava;

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
    protected Category2NumberJobOutput execute(final SparkContext context, final Category2NumberJobInput input,
            final NamedObjects namedObjects, final JavaRDD<Row> rowRDD, final int[] colIds, final String[] colNames)
            throws KNIMESparkException {

        final MappingType mappingType = input.getMappingType();
        final MappedRDDContainer mappedData = RDDUtilsInJava.convertNominalValuesForSelectedIndices(rowRDD, colIds,
            colNames, mappingType, input.keepOriginalCols());

        final String outputName = input.getFirstNamedOutputObject();
        LOGGER.info("Storing mapped data under key: " + outputName);
        namedObjects.addJavaRdd(outputName, mappedData.getRddWithConvertedValues());

        return new Category2NumberJobOutput(mappedData.getAppendedColumnNames(), mappedData.getMappings());
    }
}
