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
 *   Created on Mar 18, 2016 by dietzc
 */
package com.knime.bigdata.spark.node.io.table.reader;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Dummy implementation of a blocking queue that serializes object arrays to a given file.
 *
 * TODO: use kryo serialization instead of plain java
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
final class FileSerializingQueue extends AbstractQueue<Serializable[]> implements BlockingQueue<Serializable[]> {

    private final ObjectOutputStream m_out;

    FileSerializingQueue(final File outFile, final int noOfColumns) throws FileNotFoundException, IOException {
        this(outFile, noOfColumns, -1);
    }

    FileSerializingQueue(final File outFile, final int noOfColumns, final long noOfRows) throws FileNotFoundException, IOException {
        m_out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)));
        m_out.writeLong(noOfRows);
        m_out.writeInt(noOfColumns);
    }


    @Override
    public Serializable[] poll() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Serializable[] peek() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Serializable[]> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(final Serializable[] e) throws InterruptedException {
        offer(e);
    }

    @Override
    public boolean offer(final Serializable[] row) {
        if (row == null || row.length == 0) {
            closeSafely();
        } else {
            writeSafely(row);
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean offer(final Serializable[] e, final long timeout, final TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    private void writeSafely(final Object[] row) {
        try {
            for (int colIdx = 0; colIdx < row.length; colIdx++) {
                m_out.writeObject(row[colIdx]);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error while writing to disk: " + e.getMessage(), e);
        }
    }

    private void closeSafely() {
        try {
            m_out.flush();
            m_out.close();
        } catch (IOException e) {
            throw new RuntimeException("Error while writing to disk: " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Serializable[] take() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Serializable[] poll(final long timeout, final TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int drainTo(final Collection<? super Serializable[]> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int drainTo(final Collection<? super Serializable[]> c, final int maxElements) {
        throw new UnsupportedOperationException();
    }
}
