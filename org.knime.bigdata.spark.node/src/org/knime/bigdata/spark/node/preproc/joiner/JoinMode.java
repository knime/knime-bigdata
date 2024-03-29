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
 *   Created on 16.07.2015 by dwk
 */
package org.knime.bigdata.spark.node.preproc.joiner;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * This enum holds all ways of joining the two tables.
 * please note that this must be equal to 'org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode'
 *
 * we use a copy here to minimize dependencies
 *
 * @author Thorsten Meinl, University of Konstanz
 */
@SparkClass
public enum JoinMode {
    /** Make an INNER JOIN. */
    InnerJoin("Inner Join"),
    /** Make a LEFT OUTER JOIN. */
    LeftOuterJoin("Left Outer Join"),
    /** Make a RIGHT OUTER JOIN. */
    RightOuterJoin("Right Outer Join"),
    /** Make a FULL OUTER JOIN. */
    FullOuterJoin("Full Outer Join");

    private final String m_text;

    private JoinMode(final String text) {
        m_text = text;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return m_text;
    }

    /**
     * convert string representation of KNIME join mode to this join mode
     * @param aString
     * @return Enum value corresponding to given string
     */
    public static JoinMode fromKnimeJoinMode(final String aString) {
        if (InnerJoin.toString().equals(aString)) {
            return InnerJoin;
        }
        if (LeftOuterJoin.toString().equals(aString)) {
            return LeftOuterJoin;
        }
        if (RightOuterJoin.toString().equals(aString)) {
            return RightOuterJoin;
        }
        if (FullOuterJoin.toString().equals(aString)) {
            return FullOuterJoin;
        }
        return valueOf(aString);
    }

    /**
     * Convert this join mode into a Spark join type (see Sparks JoinTypes.scala).
     *
     * @return Spark join type
     */
    public String toSparkJoinType() {
        switch(this) {
            case InnerJoin: return "inner";
            case LeftOuterJoin: return "left_outer";
            case RightOuterJoin: return "right_outer";
            case FullOuterJoin: return "outer";
            default: throw new RuntimeException("Unsupported spark join type: " + this);
        }
    }
}