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
 *   Created on 05.08.2014 by koetter
 */
package com.knime.bigdata.hdfs.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;

import com.knime.bigdata.hdfs.filehandler.HDFSConnection;

/**
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public class HDFSConnectionTest {

    private static final Random RND = new SecureRandom();

    private static final String JUNIT_PATH = "hdfs://192.168.56.101:8020/user/cloudera/" + createDir("junit");

    private HDFSConnection m_connection;

    /**
     * @throws Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        final HDFSConnection con = createConnection();
        con.open();
        try {
            //remove all potential left overs
            con.delete(new URI(JUNIT_PATH), true);
        } catch (Exception e) {
            fail("Exception during cleanup: " + e.getMessage());
        }
        con.close();
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        m_connection = createConnection();
    }

    private static HDFSConnection createConnection() {
        final ConnectionInformation conf = new ConnectionInformation();
        conf.setHost("192.168.56.101");
        conf.setUser("cloudera");
        conf.setProtocol("hdfs");
        conf.setPort(8020);
        return new HDFSConnection(conf);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        //remove all potential left overs
        m_connection.delete(new URI(JUNIT_PATH), true);
        m_connection.close();
    }

    private void closeConnection(){
        try {
            m_connection.close();
        } catch (Exception e) {
            fail("Closing connection failed: " + e.getMessage());
        }
    }

    private HDFSConnection getOpenedConnection() {
        try {
            m_connection.open();
            return m_connection;
        } catch (IOException e) {
            fail("Could not open connection: " + e.getMessage());
        }
        return null;
    }

    private File createTempFile() throws IOException, FileNotFoundException {
        final File tempFile = File.createTempFile("hdfsTest", ".txt");
        OutputStream out = new FileOutputStream(tempFile);
        new PrintStream(out).println("This is my hdfs test file");
        out.close();
        return tempFile;
    }

    private static String createDir() {
        return createDir("mydir");
    }

    private static String createDir(final String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace('-', '_') + "/";
    }

    private static String createFile() {
        return createFile(".tmp");
    }
    private static String createFile(final String suffix) {
        return "file" + RND.nextInt() + suffix;
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#getHDFSPath4URI(URI)}.
     * @throws URISyntaxException if the URI is invalid
     */
    @Test
    public void testGetPath4URI() throws URISyntaxException {
        HDFSConnection con = getOpenedConnection();
        assertEquals(new Path("hdfs://192.168.56.101:8020/user/cloudera"),
            con.getHDFSPath4URI(new URI("hdfs://cloudera@192.168.56.101:8020")));
        assertEquals(new Path("hdfs://192.168.56.101:8020/user/cloudera"), con.getHDFSPath4URI(new URI("")));
        assertEquals(new Path("hdfs://192.168.56.101:8020/test"), con.getHDFSPath4URI(new URI("/test")));
        assertEquals(new Path("hdfs://192.168.56.101:8020/user/cloudera/test"), con.getHDFSPath4URI(new URI("test")));
        final Path junitPath = new Path(JUNIT_PATH);
        assertEquals(junitPath, con.getHDFSPath4URI(new URI(JUNIT_PATH)));
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#open()}.
     */
    @Test
    public void testOpen() {
        try {
            m_connection.open();
        } catch (IOException e) {
            fail(e.getMessage());
        }
        try {
            m_connection.open();
        } catch (IOException e) {
            fail("Second call to open failed: " + e.getMessage());
        }
        closeConnection();
        try {
            m_connection.open();
        } catch (IOException e) {
            fail("Reopening open failed: " + e.getMessage());
        }
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#isOpen()}.
     * @throws IOException
     */
    @Test
    public void testIsOpen() throws IOException {
        assertFalse(m_connection.isOpen());
        getOpenedConnection();
        assertTrue(m_connection.isOpen());
        closeConnection();
        assertFalse(m_connection.isOpen());
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#close()}.
     */
    @Test
    public void testClose() {
        try {
            m_connection.close();
        } catch (IOException e) {
            fail(e.getMessage());
        }
        getOpenedConnection();
        try {
            m_connection.close();
        } catch (IOException e) {
            fail(e.getMessage());
        }
        try {
            m_connection.close();
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#getFileSystem()}.
     */
    @Test
    public void testGetFileSystem() {
        try {
            assertNotNull(getOpenedConnection().getFileSystem());
        } catch (IOException e) {
            // TK_TODO Auto-generated catch block
        }
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#copyFromLocalFile(boolean, boolean, java.net.URI, java.net.URI)}.
     * @throws Exception if something went wrong
     */
    @Test
    public void testCopyFromLocalFile() throws Exception {
        final File tempFile = createTempFile();
        final URI hdfsURI = getURI(tempFile);
        final HDFSConnection con = getOpenedConnection();
        assertFalse(con.exists(hdfsURI));
        con.copyFromLocalFile(false, true, tempFile.toURI(), hdfsURI);
        assertTrue(tempFile.exists());
        assertTrue(con.exists(hdfsURI));
        con.delete(hdfsURI, false);
        assertFalse(con.exists(hdfsURI));
        con.copyFromLocalFile(true, true, tempFile.toURI(), hdfsURI);
        assertTrue(con.exists(hdfsURI));
        assertFalse(tempFile.exists());
    }

    private static URI getURI(final File tempFile) throws URISyntaxException {
        return getURI(tempFile.getName());
    }

    private static URI getURI(final String name) throws URISyntaxException {
        return new URI(JUNIT_PATH + name);
    }
    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#copyToLocalFile(boolean, java.net.URI, java.net.URI, boolean)}.
     * @throws Exception if something went wrong
     */
    @Test
    public void testCopyToLocalFile() throws Exception {
        final File tempFile = createTempFile();
        final URI hdfsURI = getURI(tempFile);
        final HDFSConnection con = getOpenedConnection();
        con.copyFromLocalFile(true, true, tempFile.toURI(), hdfsURI);
        assertFalse(tempFile.exists());
        con.copyToLocalFile(false, hdfsURI, tempFile.toURI(), true);
        assertTrue(tempFile.exists());
        assertTrue(con.exists(hdfsURI));
        tempFile.delete();
        con.copyToLocalFile(true, hdfsURI, tempFile.toURI(), true);
        assertTrue(tempFile.exists());
        assertFalse(con.exists(hdfsURI));
        tempFile.delete();
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#delete(java.net.URI, boolean)}.
     * @throws Exception if something went wrong
     */
    @Test
    public void testDelete() throws Exception {
        final File tempFile = createTempFile();
        final URI hdfsURI = getURI(tempFile);
        final HDFSConnection con = getOpenedConnection();
        con.copyFromLocalFile(false, true, tempFile.toURI(), hdfsURI);
        assertTrue(con.exists(hdfsURI));
        assertTrue(con.delete(hdfsURI, false));
        assertFalse(con.exists(hdfsURI));
        final URI hdfsDir = getURI(createDir());
        con.mkDir(hdfsDir);
        URI hdfsRecursiveFile = new URI(hdfsDir.toString() + tempFile.getName());
        con.copyFromLocalFile(true, true, tempFile.toURI(), hdfsRecursiveFile);
        assertTrue(con.exists(hdfsRecursiveFile));
        try {
            con.delete(hdfsDir, false);
            fail("Deleting none empty directory should fail in none recursive mode");
        } catch (Exception e) {
            // this should happen
//            System.out.println(e.getMessage());
        }
        assertTrue(con.exists(hdfsRecursiveFile));
        assertTrue(con.delete(hdfsDir, true));
        assertFalse(con.exists(hdfsRecursiveFile));
        assertFalse(con.exists(hdfsDir));
    }


    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#mkDir(java.net.URI)}.
     * @throws Exception if something went wrong
     */
    @Test
    public void testMkDir() throws Exception {
        final URI hdfsURI = getURI(createDir());
        final HDFSConnection con = getOpenedConnection();
        assertFalse(con.exists(hdfsURI));
        con.mkDir(hdfsURI);
        assertTrue(con.exists(hdfsURI));
        assertTrue(con.isDirectory(hdfsURI));
        con.delete(hdfsURI, true);
        assertFalse(con.exists(hdfsURI));
    }

    private void testFileContent(final HDFSConnection con, final URI hdfsURI, final File tempFile)
            throws IOException, FileNotFoundException {
        try (
            final FSDataInputStream hdfsIS = con.open(hdfsURI);
            final BufferedReader reader1 = new BufferedReader(new InputStreamReader(hdfsIS));
            final BufferedReader reader2 = new BufferedReader(new InputStreamReader(new FileInputStream(tempFile)));
        ) {
            String line1 = null;
            String line2 = null;
            int flag=1;
            while ((flag==1) &&((line1 = reader1.readLine()) != null)&&((line2 = reader2.readLine()) != null))
            {
                if (!line1.equalsIgnoreCase(line2)) {
                    fail("file content differs");
                }
            }
        }
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#open(java.net.URI)}.
     * @throws Exception
     */
    @Test
    public void testOpenURI() throws Exception {
        final File tempFile = createTempFile();
        final URI hdfsURI = getURI(tempFile);
        final HDFSConnection con = getOpenedConnection();
        con.copyFromLocalFile(false, true, tempFile.toURI(), hdfsURI);
        testFileContent(con, hdfsURI, tempFile);
        tempFile.delete();
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#create(java.net.URI, boolean)}.
     * @throws Exception
     */
    @Test
    public void testCreate() throws Exception {
        final String testString = "This is my test file for HDFS";
        final URI hdfsUri = getURI(createDir() + "file.txt");
        HDFSConnection con = getOpenedConnection();
        try(final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(con.create(hdfsUri, true)));) {
            writer.write(testString);
        }
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(con.open(hdfsUri)));){
            assertEquals(reader.readLine(), testString);
        }
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#exists(java.net.URI)}.
     * @throws Exception
     */
    @Test
    public void testExists() throws Exception {
        final File tempFile = createTempFile();
        final URI hdfsURI = getURI(tempFile);
        final HDFSConnection con = getOpenedConnection();
        assertFalse(con.exists(hdfsURI));
        con.copyFromLocalFile(false, true, tempFile.toURI(), hdfsURI);
        assertTrue(con.exists(hdfsURI));
        con.delete(hdfsURI, false);
        assertFalse(con.exists(hdfsURI));
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#isDirectory(java.net.URI)}.
     * @throws Exception
     */
    @Test
    public void testIsDirectory() throws Exception {
        final URI hdfsDirURI = getURI(createDir());
        final HDFSConnection con = getOpenedConnection();
        assertFalse(con.isDirectory(hdfsDirURI));
        con.mkDir(hdfsDirURI);
        assertTrue(con.isDirectory(hdfsDirURI));
        con.delete(hdfsDirURI, true);
        final File tempFile = createTempFile();
        final URI hdfsURI = getURI(tempFile);
        con.copyFromLocalFile(false, true, tempFile.toURI(), hdfsURI);
        assertFalse(con.isDirectory(hdfsURI));
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#getFileStatus(java.net.URI)}.
     * @throws Exception
     */
    @Test
    public void testGetFileStatus() throws Exception {
        final URI hdfsDirURI = getURI(createDir());
        final HDFSConnection con = getOpenedConnection();
        try {
            con.getFileStatus(hdfsDirURI);
            fail("None existing file should throw an exception");
        } catch (FileNotFoundException e) {
            // this should happen
        }
        con.mkDir(hdfsDirURI);
        FileStatus fileStatus = con.getFileStatus(hdfsDirURI);
        assertTrue(fileStatus.isDirectory());
        final File tempFile = createTempFile();
        final URI hdfsFileURI = getURI(tempFile);
        con.copyFromLocalFile(true, true, tempFile.toURI(), hdfsFileURI);
        fileStatus = con.getFileStatus(hdfsFileURI);
        assertTrue(fileStatus.isFile());
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#listFiles(java.net.URI)}.
     * @throws Exception
     */
    @Test
    public void testListFiles() throws Exception {
        final HDFSConnection con = getOpenedConnection();
        try {
            con.listFiles(new URI(JUNIT_PATH));
            fail("None existing file should throw an exception");
        } catch (FileNotFoundException e) {
            // this should happen
        }
        final File tempFile = createTempFile();
        URI hdfsFile1Uri = getURI(tempFile);
        con.copyFromLocalFile(false, true, tempFile.toURI(), hdfsFile1Uri);
        final File tempFile2 = createTempFile();
        URI hdfsFile2Uri = getURI(tempFile2);
        con.copyFromLocalFile(false, true, tempFile.toURI(), hdfsFile2Uri);
        final URI hdfsDirURI = getURI(createDir());
        con.mkDir(hdfsDirURI);
        final FileStatus[] files = con.listFiles(new URI(JUNIT_PATH));
        assertEquals(3, files.length);
        for (FileStatus file : files) {
            final URI fileUri = file.getPath().toUri();
            if (fileUri.equals(hdfsFile1Uri)) {

            } else if (fileUri.equals(hdfsFile2Uri)) {

            } else if ((fileUri.getPath() + "/").equals(hdfsDirURI.getPath())) {

            } else {
                fail("Invalid file found: " + file);
            }
        }
        tempFile.delete();
        tempFile2.delete();
    }

    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#getContentSummary(URI)}.
     * @throws Exception
     */
    @Test
    public void testgetContentSummary() throws Exception {
        final HDFSConnection con = getOpenedConnection();
        final File tempFile = createTempFile();
        URI tempUri = getURI(tempFile);
        con.copyFromLocalFile(false, true, tempFile.toURI(), tempUri);
        ContentSummary contentSummary = con.getContentSummary(tempUri);
        assertEquals(tempFile.length(), contentSummary.getLength());
    }
    /**
     * Test method for {@link com.knime.bigdata.hdfs.filehandler.HDFSConnection#setPermission(URI, String)}.
     * @throws Exception
     */
    @Test
    public void testSetPermission() throws Exception {
        final HDFSConnection con = getOpenedConnection();
        final File tempFile = createTempFile();
        URI tempUri = getURI(tempFile);
        con.copyFromLocalFile(false, true, tempFile.toURI(), tempUri);
        con.setPermission(tempUri, "--wx-wx-wx");
    }
}
