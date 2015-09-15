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
 *   Created on 07.08.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.server;

/**
 *
 * @author dwk
 */
public class EnumContainer {
    /**
     * (should be identical to org.knime.base.node.preproc.sample.SamplingNodeSettings.CountMethods)
     * Enum for the two methods for setting the number of rows in the output
     * table.
     */
    public enum CountMethods {
        /** Relative fraction. */
        Relative,
        /** Absolute number. */
        Absolute;

        /**
         * convert string representation of KNIME count method to this count method
         * @param aString
         * @return Enum value corresponding to given string
         */
        public static CountMethods fromKnimeEnum(final String aString) {
            if (Relative.toString().equals(aString)) {
                return Relative;
            }
            if (Absolute.toString().equals(aString)) {
                return Absolute;
            }
            return valueOf(aString);
        }
    }


    /**
     * Enum for the four different sampling methods.
     * (should be identical to org.knime.base.node.preproc.sample.SamplingNodeSettings.SamplingMethods)
     */
    public enum SamplingMethods {
        /** Selects the first <em>x</em> rows. */
        First,
        /** Selects rows randomly. */
        Random,
        /** Select rows randomly but maintain the class distribution. */
        Stratified,
        /** Select the rows linearly over the whole table. */
        Linear;

        /**
         * convert string representation of KNIME sampling method to this sampling method
         * @param aString
         * @return Enum value corresponding to given string
         */
        public static SamplingMethods fromKnimeEnum(final String aString) {
            for (SamplingMethods v : values()) {
                if (v.toString().equals(aString)) {
                    return v;
                }

            }
            return valueOf(aString);
        }
    }

    /**
     * enum for the two different correlation methods
     *
     * @author dwk
     */
    public enum CorrelationMethods {
        /** Pearson */
        pearson,
        /** Spearman */
        spearman;

        /**
         * convert string representation of KNIME correlation method to this correlation method
         * @param aString
         * @return Enum value corresponding to given string
         */
        public static CorrelationMethods fromKnimeEnum(final String aString) {
            if (pearson.toString().equals(aString)) {
                return pearson;
            }
            if (spearman.toString().equals(aString)) {
                return spearman;
            }
            return valueOf(aString);
        }
    }


}
