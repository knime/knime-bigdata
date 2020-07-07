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
 *   Created on Jun 8, 2020 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.core.types.converter.knime;

import java.time.ZoneId;

/**
 * Context specific converter parameter.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class KNIMEToIntermediateConverterParameter {

    /**
     * Default converter parameter that does not use time shifting.
     */
    public static final KNIMEToIntermediateConverterParameter DEFAULT = new KNIMEToIntermediateConverterParameter();

    private final ZoneId m_zoneId;

    private final boolean m_timeShift;

    private KNIMEToIntermediateConverterParameter() {
        m_zoneId = ZoneId.of("UTC");
        m_timeShift = false;
    }

    /**
     * Converter parameter constructor that uses time shifting into a given time zone.
     *
     * @param zoneId Zone to use in time shift
     */
    public KNIMEToIntermediateConverterParameter(final ZoneId zoneId) {
        m_zoneId = zoneId;
        m_timeShift = true;
    }

    /**
     * @return {@code true} if time shifting should be used
     */
    public boolean useTimeShift() {
        return true;
    }

    /**
     * @return Zone to use in time shift
     */
    public ZoneId getTimShiftZoneId() {
        return m_zoneId;
    }

    /**
     * @return human readable description of this parameter
     */
    public String getTimeShiftDescription() {
        if (m_timeShift) {
            return "Convert to zone " + m_zoneId;
        } else {
            return "No conversion";
        }
    }

}
