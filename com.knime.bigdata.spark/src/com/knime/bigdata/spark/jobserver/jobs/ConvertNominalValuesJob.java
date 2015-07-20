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
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.MappedRDDContainer;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.jobserver.server.MyRecord;
import com.knime.bigdata.spark.jobserver.server.NominalValueMapping;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.InvalidSchemaException;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

/**
 * converts nominal values from a set of columns to numbers and adds corresponding new columns
 *
 * @author dwk
 */
public class ConvertNominalValuesJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String PARAM_INPUT_TABLE = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

    private static final String PARAM_MAPPING_TYPE = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_STRING;

    private static final String PARAM_COL_IDXS = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_COL_IDXS;

    private static final String PARAM_RESULT_TABLE = ParameterConstants.PARAM_OUTPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

    private static final String PARAM_RESULT_MAPPING = ParameterConstants.PARAM_OUTPUT + "."
        + ParameterConstants.PARAM_TABLE_2;

    private final static Logger LOGGER = Logger.getLogger(ConvertNominalValuesJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final Config aConfig) {
        String msg = null;

        if (!aConfig.hasPath(PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
        }

        if (msg == null) {
            if (!aConfig.hasPath(PARAM_COL_IDXS)) {
                msg = "Input parameter '" + PARAM_COL_IDXS + "' missing.";
            } else {
                try {
                    List<Integer> vals = aConfig.getIntList(PARAM_COL_IDXS);
                    if (vals.size() < 1) {
                        msg = "Input parameter '" + PARAM_COL_IDXS + "' is empty.";
                    }
                } catch (ConfigException e) {
                    msg = "Input parameter '" + PARAM_COL_IDXS + "' is not of expected type 'integer list'.";
                }
            }
        }

        if (msg == null) {
            if (!aConfig.hasPath(PARAM_MAPPING_TYPE)) {
                msg = "Input parameter '" + PARAM_MAPPING_TYPE + "' missing.";
            } else {
                try {
                    MappingType.valueOf(aConfig.getString(PARAM_MAPPING_TYPE));
                } catch (Exception e) {
                    msg = "Input parameter '" + PARAM_MAPPING_TYPE + "' has an invalid value.";
                }
            }
        }

        if (msg == null && !aConfig.hasPath(PARAM_RESULT_TABLE)) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE + "' missing.";
        }

        if (msg == null) {
            if (!aConfig.hasPath(PARAM_RESULT_MAPPING)) {
                msg = "Output parameter '" + PARAM_RESULT_MAPPING + "' missing.";
            }
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    private void validateInput(final Config aConfig) throws GenericKnimeSparkException {
        String msg = null;
        final String key = aConfig.getString(PARAM_INPUT_TABLE);
        if (key == null) {
            msg = "Input parameter at port 1 is missing!";
        } else if (!validateNamedRdd(key)) {
            msg = "Input data table missing!";
        }
        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    /**
     * run the actual job, the result is serialized back to the client
     *
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final Config aConfig) throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.INFO, "starting job to convert nominal values...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getString(PARAM_INPUT_TABLE));
        final List<Integer> colIdxs = aConfig.getIntList(PARAM_COL_IDXS);
        final int[] colIds = new int[colIdxs.size()];
        int i = 0;
        for (Integer ix : colIdxs) {
            colIds[i++] = ix;
        }
        final MappingType type = MappingType.valueOf(aConfig.getString(PARAM_MAPPING_TYPE));

        //use only the column indices when converting
        final MappedRDDContainer mappedData =
            RDDUtilsInJava.convertNominalValuesForSelectedIndices(rowRDD, colIds, type);

        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(mappedData.m_Mappings);

        LOGGER.log(Level.INFO, "Storing mapped data under key: " + aConfig.getString(PARAM_RESULT_TABLE));
        try {
            addToNamedRdds(aConfig.getString(PARAM_RESULT_TABLE), mappedData.m_RddWithConvertedValues);

            try {
                {
                    final StructType schema =
                        StructTypeBuilder.fromRows(mappedData.m_RddWithConvertedValues.take(10)).build();
                    res = res.withTable(aConfig.getString(PARAM_RESULT_TABLE), schema);
                }
                {
                    JavaRDD<Row> mappingRdd =
                        storeMappingsInRdd(sc, mappedData.m_Mappings, aConfig.getString(PARAM_RESULT_MAPPING));
                    final StructType schema = StructTypeBuilder.fromRows(mappingRdd.take(10)).build();
                    res = res.withTable(aConfig.getString(PARAM_RESULT_MAPPING), schema);
                }
            } catch (InvalidSchemaException e) {
                return JobResult.emptyJobResult().withMessage("ERROR: " + e.getMessage());
            }
        } catch (Exception e) {
            LOGGER.severe("ERROR: failed to predict and store results for training data.");
            LOGGER.severe(e.getMessage());
        }

        LOGGER.log(Level.INFO, "done");
        return res;
    }

    private JavaRDD<Row> storeMappingsInRdd(final SparkContext aSparkContext, final NominalValueMapping aMappings,
        final String aRddName) {
        @SuppressWarnings("resource")
        JavaSparkContext javaContext = new JavaSparkContext(aSparkContext);

        Iterator<MyRecord> iter = aMappings.iterator();
        List<Row> rows = new ArrayList<Row>();
        while (iter.hasNext()) {

            MyRecord record = iter.next();
            RowBuilder builder = RowBuilder.emptyRow();
            builder.add(record.m_nominalColumnIndex).add(record.m_nominalValue).add(record.m_numberValue);
            rows.add(builder.build());
        }

        JavaRDD<Row> mappingRdd = javaContext.parallelize(rows);
        LOGGER.log(Level.INFO, "Storing mapping under key: " + aRddName);
        addToNamedRdds(aRddName, mappingRdd);
        return mappingRdd;
    }
}
