/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
package com.knime.bigdata.commons.security.kerberos.logging;

import java.io.PrintStream;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeLogger.LEVEL;

/**
 * This singleton class handles the Kerberos logging by setting the right system properties and redirecting the
 * System.out to the {@link NodeLoggerOutputStreamBuffer} class.
 * @author Tobias Koetter, KNIME.com
 */
public class KerberosLogger {

    static final NodeLogger LOGGER = NodeLogger.getLogger(KerberosLogger.class);
    private static volatile KerberosLogger instance = new KerberosLogger();

    private PrintStream m_stream;
    private PrintStream m_origStream;
    private LEVEL m_level;
    private NodeLoggerOutputStreamBuffer m_kerberosLogger;

    private KerberosLogger() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
    */
    public static KerberosLogger getInstance() {
        return instance;
    }

    /**
     * @param level the logging {@link LEVEL} to set
     */
    public void setLevel(final LEVEL level) {
        if (m_level != level) {
            LOGGER.debug("Keberos logging level changed to " + level);
            m_level = level;
            if (m_kerberosLogger != null) {
                m_kerberosLogger.setLevel(m_level);
            }
        }
    }

    /**
     * @return the logging {@link LEVEL}
     */
    public LEVEL getLevel() {
        return m_level;
    }

    /**
     * @return <code>true</code> if logging is enabled
     */
    public boolean isEnabled() {
        return m_stream != null;
    }

    /**
     * Enable or disable the Kerberos logging.
     * @param enable <code>true</code> if logging should be enabled
     * @param level
     */
    public void setEnable(final boolean enable, final LEVEL level) {
        setLevel(level);
        if (enable == isEnabled()) {
            return;
        }
        if (enable) {
            enable();
        } else {
            disable();
        }
    }

    /**
     * Calling this method enables the Kerberos logging.
     */
    private void enable() {
        LOGGER.debug("Enable Kerberos logging");
        System.setProperty("sun.security.krb5.debug", "true");
        LOGGER.debug("Setting sun.security.krb5.debug to true");
        System.setProperty("sun.security.jgss.debug", "true");
        LOGGER.debug("Setting sun.security.jgss.debug to true");
        m_kerberosLogger = new NodeLoggerOutputStreamBuffer(LOGGER, m_level);
        m_stream = new PrintStream(m_kerberosLogger, true);
        //save the original output stream in order to reset it if the logging is disabled
        m_origStream = System.out;
        System.setOut(m_stream);
        LOGGER.info("Kerberos log redirected to KNIME log with level " + m_level.name());
    }

    /**
     * Calling this method disables the Kerberos logging.
     */
    private void disable() {
        LOGGER.debug("Disable Kerberos logging");
        System.setProperty("sun.security.krb5.debug", "false");
        LOGGER.debug("Setting sun.security.krb5.debug to false");
        System.setProperty("sun.security.jgss.debug", "false");
        LOGGER.debug("Setting sun.security.jgss.debug to false");
        System.setOut(m_origStream);
        LOGGER.info("Kerberos logging disabled");
        m_stream.flush();
        m_stream.close();
        m_kerberosLogger = null;
        m_stream = null;
        m_origStream = null;
    }

}
