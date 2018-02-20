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
 *   Created on Apr 7, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.sparkjobserver.context;

import java.io.Serializable;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME.COM
 */
@SparkClass
public class LogMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int m_log4jLogLevel;

    private final long m_timestamp;

    private final String m_loggerName;

    private final String m_message;

    /**
     * @param log4jLogLevel The log4j log level as an int
     * @param timestamp The timestamp or the log message (server time)
     * @param loggerName The name of the logger
     * @param message The logged message
     */
    public LogMessage(final int log4jLogLevel, final long timestamp, final String loggerName, final String message) {
        m_log4jLogLevel = log4jLogLevel;
        m_timestamp = timestamp;
        m_loggerName = loggerName;
        m_message = message;
    }

    /**
     * @return the log4j log level as an int
     */
    public int getLog4jLogLevel() {
        return m_log4jLogLevel;
    }

    /**
     * @return the name of the logger
     */
    public String getLoggerName() {
        return m_loggerName;
    }

    /**
     * @return the logged message
     */
    public String getMessage() {
        return m_message;
    }

    /**
     * @return the timestamp or the log message (server time)
     */
    public long getTimestamp() {
        return m_timestamp;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) { return false; }
        if (obj == this) { return true; }
        if (obj.getClass() != getClass()) {
          return false;
        }
        LogMessage rhs = (LogMessage) obj;

        return (m_log4jLogLevel == rhs.m_log4jLogLevel)
                && (m_timestamp == rhs.m_timestamp)
                && (m_loggerName.equals(rhs.m_loggerName))
                && (m_message.equals(rhs.m_message));
    }

    @Override
    public int hashCode() {
        final int constant = 13;

        int hashCode = 7 * constant + m_log4jLogLevel;
        hashCode = hashCode * constant + ((int) (m_timestamp ^ (m_timestamp >> 32)));
        hashCode = hashCode * constant + m_loggerName.hashCode();
        hashCode = hashCode * constant + m_message.hashCode();

        return hashCode;
    }
}
