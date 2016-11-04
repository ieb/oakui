/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sling.oakui.nslucene;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by ieb on 02/11/2016.
 */
public class OakIndexInputTest {

    @Mock
    private NodeState nodeStateFile;
    private byte[] testFile = new byte[100*1024+152];
    @Mock
    private PropertyState dataProperty;
    private int blobSize = 349; // prime blobsize might cause more failures, than non prime.
    @Mock
    private PropertyState blobSizeProperty;
    private ArrayList<Blob> blobs;
    private int lastBlockSize;

    public OakIndexInputTest() {
        MockitoAnnotations.initMocks(this);
        Random r = new Random(100);
        r.nextBytes(testFile);
    }

    @Before
    public void before() {
        Mockito.when(nodeStateFile.hasProperty("blobSize")).thenReturn(true);
        Mockito.when(nodeStateFile.getProperty("blobSize")).thenReturn(blobSizeProperty);
        Mockito.when(blobSizeProperty.getValue(Type.LONG)).thenReturn((long) blobSize);
        Mockito.when(nodeStateFile.getProperty("jcr:data")).thenReturn(dataProperty);
        blobs = new ArrayList<Blob>();
        int blobS = 0;
        for(blobS = 0; blobS < (testFile.length-blobSize); blobS+=blobSize) {
            blobs.add(createBlob(blobS, blobSize));
        }
        System.err.println("Start of final blob is at "+blobS);
        System.err.println("Start of final blob is at "+blobS);
        System.err.println("Start of final length of file is  "+testFile.length);
        if ( blobS  < testFile.length) {
            lastBlockSize = testFile.length-blobS;
            System.err.println("Additional bytes required  "+lastBlockSize);
            blobs.add(createBlob(blobS, lastBlockSize));
        }

        Mockito.when(dataProperty.getValue(Type.BINARIES)).thenReturn(blobs);
    }

    @After
    public void after() {
        System.err.println("Created "+blobs.size()+" blobs");
        System.err.println("Last block size is "+lastBlockSize);
        System.err.println("BlogSize  "+(((blobs.size()-1)*blobSize)+lastBlockSize));
        System.err.println("FileSize  " + testFile.length);
    }

    private Blob createBlob(final int blobStart, final int sizeOfBlob) {
        return new Blob() {

            @Nonnull
            @Override
            public InputStream getNewStream () {
                return new ByteArrayInputStream(testFile, blobStart, sizeOfBlob);
            }

            @Override
            public long length () {
                return sizeOfBlob;
            }

            @Override
            public String getReference () {
                return "BlobStart_" + blobStart;
            }

            @Override
            public String getContentIdentity () {
                return getReference();
            }
        };
    }


    @Test
    public void testReadByte() throws IOException {
        Assert.assertEquals(testFile.length, (blobs.size()-1)*blobSize+lastBlockSize);
        OakIndexInput oakIndexInput = new OakIndexInput("lucene", nodeStateFile, "testfile.gen");
        Assert.assertEquals(testFile.length, oakIndexInput.length());
        for (int i = 0; i < testFile.length; i++) {
            Assert.assertEquals(oakIndexInput.readByte(), testFile[i]);
        }
    }

    @Test
    public void testReadBytBuffere() throws IOException {
        Assert.assertEquals(testFile.length, (blobs.size()-1)*blobSize+lastBlockSize);
        OakIndexInput oakIndexInput = new OakIndexInput("lucene", nodeStateFile, "testfile.gen");
        byte[] readBuffer = new byte[10240];
        Random toRead = new Random();
        int nr = 0;
        while (nr < testFile.length) {
            int n = Math.min(toRead.nextInt(readBuffer.length - 1), testFile.length-nr);
            oakIndexInput.readBytes(readBuffer, 0, n);
            for ( int i = 0; i < n; i++) {
                Assert.assertEquals(testFile[nr], readBuffer[i]);
                nr++;
            }
        }
    }
    @Test
    public void testMixedReadByteBuffer() throws IOException {
        Assert.assertEquals(testFile.length, (blobs.size()-1)*blobSize+lastBlockSize);
        OakIndexInput oakIndexInput = new OakIndexInput("lucene", nodeStateFile, "testfile.gen");
        byte[] readBuffer = new byte[10240];
        Random toRead = new Random();
        int nr = 0;
        while (nr < testFile.length) {
            int n = Math.min(toRead.nextInt(readBuffer.length - 1), testFile.length-nr);
            oakIndexInput.readBytes(readBuffer, 0, n);
            for ( int i = 0; i < n; i++) {
                Assert.assertEquals(testFile[nr], readBuffer[i]);
                nr++;
            }
            n = Math.min(toRead.nextInt(20), testFile.length-nr);
            for(int i = 0; i < n; i++) {
                Assert.assertEquals(testFile[nr],oakIndexInput.readByte());
                nr++;
            }

        }
    }

    @Test
    public void testMixedReadWithSeek() throws IOException {
        Assert.assertEquals(testFile.length, (blobs.size()-1)*blobSize+lastBlockSize);
        OakIndexInput oakIndexInput = new OakIndexInput("lucene", nodeStateFile, "testfile.gen");
        byte[] readBuffer = new byte[1024];
        Random toRead = new Random();
        for (int t = 0; t < 1000; t++) {
            int nr = toRead.nextInt(testFile.length - 1);
            oakIndexInput.seek(nr);
            int n = Math.min(toRead.nextInt(readBuffer.length - 1), testFile.length-nr);
            oakIndexInput.readBytes(readBuffer, 0, n);
            for ( int i = 0; i < n; i++) {
                Assert.assertEquals(testFile[nr], readBuffer[i]);
                nr++;
            }
            n = Math.min(toRead.nextInt(20), testFile.length-nr);
            for(int i = 0; i < n; i++) {
                Assert.assertEquals(testFile[nr],oakIndexInput.readByte());
                nr++;
            }

        }
    }

}
