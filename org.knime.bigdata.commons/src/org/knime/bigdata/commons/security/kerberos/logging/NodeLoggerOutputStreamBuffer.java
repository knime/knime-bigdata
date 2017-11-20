/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   01.02.2017 (koetter): created
 */
package org.knime.bigdata.commons.security.kerberos.logging;

import java.io.IOException;
import java.io.OutputStream;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeLogger.LEVEL;

/**
 * {@link OutputStream} implementation that writes to the given {@link NodeLogger} with the defined level.
 * @author Tobias Koetter, KNIME.com
 */
public class NodeLoggerOutputStreamBuffer extends OutputStream {
    private static final String SEPERATOR = System.getProperty("line.separator");
    private boolean m_closed = false;
    private final StringBuilder m_buf = new StringBuilder();
    private final NodeLogger m_logger;
    private LEVEL m_level;

    /**
     * @param logger the {@link NodeLogger} to use
     * @param level the logging {@link LEVEL} to use
     */
    public NodeLoggerOutputStreamBuffer(final NodeLogger logger, final LEVEL level) {
        if (logger == null) {
            throw new NullPointerException("logger must not be null");
        }
        if (level == null) {
            throw new NullPointerException("level must not be null");
        }
        m_logger = logger;
        m_level = level;
    }

    /**
     * @param level the logging {@link LEVEL} to use
     */
    public void setLevel(final LEVEL level) {
        if (level == null) {
            throw new NullPointerException("level must not be null");
        }
        m_level = level;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
      flush();
      m_closed = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final int b) throws IOException {
      if (m_closed) {
        throw new IOException("The stream has been closed.");
      }
      // don't log nulls
      if (b == 0) {
        return;
      }
      m_buf.append((char)b);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() {
        if (m_buf.length() < 1) {
            return;
        }
        // ignore blank lines
        if (m_buf.length() == SEPERATOR.length()) {
          if ((m_buf.charAt(0)) == SEPERATOR.charAt(0)  &&
               ((m_buf.length() == 1) ||
                 ((m_buf.length() == 2) && (m_buf.charAt(1) == SEPERATOR.charAt(1))))) {
              m_buf.setLength(0);
            return;
          }
        }
        final String msg = m_buf.toString();
        switch (m_level) {
            case ERROR:
                m_logger.error(msg);
                break;
            case FATAL:
                m_logger.fatal(msg);
                break;
            case INFO:
                m_logger.info(msg);
                break;
            case WARN:
                m_logger.warn(msg);
                break;
            default:
                m_logger.debug(msg);
                break;
        }
  /*++++++++++++++++++++++++++++++++++++++++++++++++++*/
      m_buf.setLength(0);
    }
}