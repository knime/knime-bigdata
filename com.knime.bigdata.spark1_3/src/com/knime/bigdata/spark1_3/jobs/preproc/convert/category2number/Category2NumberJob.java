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
package com.knime.bigdata.spark1_3.jobs.preproc.convert.category2number;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;
import com.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobInput;
import com.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobOutput;
import com.knime.bigdata.spark1_3.base.MappedRDDContainer;
import com.knime.bigdata.spark1_3.base.NamedObjects;
import com.knime.bigdata.spark1_3.base.RDDUtilsInJava;

/**
 * converts nominal values from a set of columns to numbers and adds corresponding new columns
 *
 * @author dwk
 */
@SparkClass
public class Category2NumberJob extends AbstractStringMapperJob {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(Category2NumberJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    protected Category2NumberJobOutput execute(final SparkContext aContext, final Category2NumberJobInput input, final NamedObjects namedObjects,
        final JavaRDD<Row> rowRDD, final int[] colIds, final Map<Integer, String> colNameForIndex) throws KNIMESparkException {
        final MappingType mappingType = input.getMappingType();

        //use only the column indices when converting
        final MappedRDDContainer mappedData =
            RDDUtilsInJava.convertNominalValuesForSelectedIndices(rowRDD, colIds, mappingType, input.keepOriginalCols());

        final String outputName = input.getFirstNamedOutputObject();
        LOGGER.info("Storing mapped data under key: " + outputName);
        namedObjects.addJavaRdd(outputName, mappedData.m_RddWithConvertedValues);

        //number of all (!)  columns in input data table
        //TODO: get number of all columns from input
        int offset = rowRDD.take(1).get(0).length();
        mappedData.createMappingTable(colNameForIndex, offset);
        return new Category2NumberJobOutput(mappedData.getColumnNames(), mappedData.m_Mappings);
    }
}
