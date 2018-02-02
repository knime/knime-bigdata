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
 *   Created on Jan 25, 2018 by bjoern
 */
package org.knime.bigdata.spark.local.ui;

import org.eclipse.core.filesystem.EFS;
import org.eclipse.core.filesystem.IFileStore;
import org.eclipse.core.runtime.Path;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.ui.IWorkbenchPage;
import org.eclipse.ui.IWorkbenchWindow;
import org.eclipse.ui.IWorkbenchWindowActionDelegate;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.ide.IDE;
import org.knime.bigdata.spark.local.SparkLocalPlugin;
import org.knime.core.node.NodeLogger;

/**
 * SWT Action that opens the local Spark log file in the workbench.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class OpenLocalSparkLogfileAction implements IWorkbenchWindowActionDelegate {
    private static final NodeLogger logger = NodeLogger
            .getLogger(OpenLocalSparkLogfileAction.class);

    private IWorkbenchWindow m_window;

    /**
     * {@inheritDoc}
     */
    @Override
    public void run(final IAction action) {
        Path path = new Path(SparkLocalPlugin.SPARK_LOG_FILE.getAbsolutePath());
        IFileStore fileStore = EFS.getLocalFileSystem().getStore(path);
        IWorkbenchPage page = m_window.getActivePage();

        try {
            IDE.openEditorOnFileStore(page, fileStore);
        } catch (PartInitException e) {
            logger.error("Could not open local Spark log file", e);
        }
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void selectionChanged(final IAction action,
            final ISelection selection) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void dispose() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final IWorkbenchWindow window) {
        m_window = window;
    }
}
