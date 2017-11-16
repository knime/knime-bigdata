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
 *   Created on Nov 16, 2017 by Sascha Wolke, KNIME GmbH
 */
package com.knime.bigdata.spark.core.sql_function;

import java.io.Serializable;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;

/**
 * Spark SQL function job input.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class SparkSQLFunctionJobInput implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String m_function;
    private final String m_factoryName;
    private final String m_outputName;
    private final Serializable m_args[];
    private final IntermediateDataType m_argTypes[];

    /**
     * @param function name of aggregation function
     * @param factoryName class name of spark function factory
     * @param outputName output column name or null
     * @param args intermediate converted list of function arguments
     * @param argTypes intermediate types of function arguments
     */
    public SparkSQLFunctionJobInput(final String function, final String factoryName, final String outputName,
        final Serializable args[], final IntermediateDataType argTypes[]) {

        if (args.length != argTypes.length) {
            throw new IllegalArgumentException("Argument and type count does not match.");
        }

        this.m_function = function;
        this.m_factoryName = factoryName;
        this.m_outputName = outputName;
        this.m_args = args;
        this.m_argTypes = argTypes;
    }

    /** @return aggregation function name */
    public String getFunction() {
        return m_function;
    }

    /** @return class name of spark function factory */
    public String getFactoryName() {
        return m_factoryName;
    }

    /** @return output column name */
    public String getOutputName() {
        return m_outputName;
    }

    /** @return argument count */
    public int getArgCount() {
        return m_args.length;
    }

    /**
     * @param i argument number
     * @return aggregation function argument
     */
    public Serializable getArg(final int i) {
        return m_args[i];
    }

    /**
     * @param i argument number
     * @return intermediate data type of aggregation function argument
     */
    public IntermediateDataType getArgType(final int i) {
        return m_argTypes[i];
    }
}