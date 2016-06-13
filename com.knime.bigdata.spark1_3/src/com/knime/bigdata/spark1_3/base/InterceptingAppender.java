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
 *   Created on May 2, 2016 by bjoern
 */
package com.knime.bigdata.spark1_3.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.jobserver.LogMessage;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class InterceptingAppender implements Appender {

    final List<LogMessage> m_logMessages = new ArrayList<>();

    private final int m_log4jLogLevelThreshold;

    public InterceptingAppender(final int log4jLogLevelThreshold) {
        m_log4jLogLevelThreshold = log4jLogLevelThreshold;
    }

    @Override
    public void setName(final String name) {
    }

    @Override
    public void setLayout(final Layout layout) {
    }

    @Override
    public void setErrorHandler(final ErrorHandler errorHandler) {
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    @Override
    public String getName() {
        return "KnimeJobLogger";
    }

    @Override
    public Layout getLayout() {
        return null;
    }

    @Override
    public Filter getFilter() {
        return null;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return null;
    }

    @Override
    public void doAppend(final LoggingEvent event) {
    	System.out.println("event: "  + event.getLevel().toInt() + " threhold: " +  m_log4jLogLevelThreshold);
        if (event != null && event.getLevel().toInt() >= m_log4jLogLevelThreshold && m_logMessages.size() <= 100) {
            m_logMessages.add(new LogMessage(event.getLevel().toInt(), event.getTimeStamp(), event.getLoggerName(),
                event.getRenderedMessage()));
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void clearFilters() {
    }

    @Override
    public void addFilter(final Filter newFilter) {
    }

    /**
     * @return the collected log messages
     */
    public List<LogMessage> getLogMessages() {
        return m_logMessages;
    }
}
