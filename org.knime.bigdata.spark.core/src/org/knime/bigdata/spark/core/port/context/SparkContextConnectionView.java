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
 *   Created on Feb 12, 2015 by knime
 */
package org.knime.bigdata.spark.core.port.context;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextPane;
import javax.swing.event.HyperlinkEvent;
import javax.swing.event.HyperlinkListener;

import org.apache.log4j.Logger;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.browser.IWebBrowser;
import org.eclipse.ui.browser.IWorkbenchBrowserSupport;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;

/**
 * Port view that shows the content of the Spark context port.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkContextConnectionView extends JPanel {

    private final static Logger LOG = Logger.getLogger(SparkContextConnectionView.class);

    private static final long serialVersionUID = 1L;

    /**
     * @param contextID
     */
    SparkContextConnectionView(final SparkContextID contextID) {
        super(new GridBagLayout());
        super.setName("Spark");
        StringBuilder buf = new StringBuilder("<html><body>");
        buf.append(SparkContextManager.getOrCreateSparkContext(contextID).getHTMLDescription());
        buf.append("</body></html>");
        final JTextPane textArea = new JTextPane();
        textArea.setContentType("text/html");
        textArea.setEditable(false);
        textArea.setText(buf.toString());
        textArea.setCaretPosition(0);

        textArea.addHyperlinkListener(new HyperlinkListener() {
            @Override
            public void hyperlinkUpdate(final HyperlinkEvent event) {
                if (event.getEventType() == HyperlinkEvent.EventType.ACTIVATED) {
                    Display.getDefault().asyncExec(new Runnable() {
                        @Override
                        public void run() {
                            IWorkbenchBrowserSupport support = PlatformUI.getWorkbench().getBrowserSupport();
                            try {
                                IWebBrowser browser = support.createBrowser(IWorkbenchBrowserSupport.AS_EDITOR,
                                    "sparkWebUI", "Spark WebUI", null);
                                browser.openURL(event.getURL());
                            } catch (PartInitException e) {
                                LOG.error("Failed to open browser", e);
                            }

                        }
                    });
                }
            }
        });

        final JScrollPane jsp = new JScrollPane(textArea);
        jsp.setPreferredSize(new Dimension(300, 300));
        final GridBagConstraints c = new GridBagConstraints();
        c.gridx = 0;
        c.gridy = 0;
        c.anchor = GridBagConstraints.CENTER;
        c.fill = GridBagConstraints.BOTH;
        c.weightx = 1;
        c.weighty = 1;
        super.add(jsp, c);
    }

}
