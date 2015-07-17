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
 *   Created on 17.07.2015 by Dietrich
 */
package com.knime.bigdata.spark.jobserver.server;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.Row;

/**
 *
 * @author dwk
 */
public class MappedRDDContainer implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * mapped data (original + converted dasta)
     */
    public final JavaRDD<Row> m_RddWithConvertedValues;

    /**
     * the mappings of nominal values to numbers
     */
    public final NominalValueMapping m_Mappings;

    /**
     * @param aRddWithConvertedValues
     * @param aMappings
     */
    public MappedRDDContainer(final JavaRDD<Row> aRddWithConvertedValues, final NominalValueMapping aMappings) {
        m_RddWithConvertedValues = aRddWithConvertedValues;
        m_Mappings = aMappings;
    }

}
