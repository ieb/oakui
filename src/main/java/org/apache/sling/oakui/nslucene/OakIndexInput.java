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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by ieb on 21/10/2016.
 */
public class OakIndexInput extends IndexInput {
    private static final Logger LOGGER = LoggerFactory.getLogger(OakIndexInput.class);
    private final List<Blob> blobs;
    private final int blobSize;


    /**
     * Size of the blob entries to which the Lucene files are split.
     * Set to higher than the 4kB inline limit for the BlobStore,
     */
    static final int DEFAULT_BLOB_SIZE = 32 * 1024;
    private final byte[] buffer;
    private final String name;
    private final long length;
    private long pos;
    private int loadedBlobNo;
    private int blobLength;


    public OakIndexInput(String name, NodeState file, String indexName) {
        super(indexName+"/"+name);
        this.name = indexName+"/"+name;
        blobs = ImmutableList.copyOf(file.getProperty("jcr:data").getValue(Type.BINARIES));
        blobSize = determineBlobSize(file);
        byte[] uniqueKey = readUniqueKey(file);
        buffer = new byte[blobSize];
        loadedBlobNo = -1;
        blobLength = 0;


        long l = (long)blobs.size() * blobSize;
        if (!blobs.isEmpty()) {
            Blob last = blobs.get(blobs.size() - 1);
            l -= blobSize - last.length();
            if (uniqueKey != null) {
                l -= uniqueKey.length;
            }
        }
        length = l;
    }

    private static byte[] readUniqueKey(NodeState file) {
        if (file.hasProperty("uniqueKey")) {
            String key = file.getString("uniqueKey");
            return StringUtils.convertHexToBytes(key);
        }
        return null;
    }
    private static int determineBlobSize(NodeState file){
        if (file.hasProperty("blobSize")){
            return Ints.checkedCast(file.getProperty("blobSize").getValue(Type.LONG));
        }
        return DEFAULT_BLOB_SIZE;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public long getFilePointer() {
        return pos;
    }

    @Override
    public void seek(long pos) throws IOException {
        this.pos = pos;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte readByte() throws IOException {
        int blobNo = (int)pos/blobSize;
        byte[] blobData = loadBlob(blobNo);
        int p = ((int)pos) - (blobNo*blobSize);
        if ( p >= blobLength) {
            throw new IOException("End of file");
        }
        //LOGGER.info("Read 1 byte from {}:{}:{}:{} ", new Object[]{name, blobNo, pos, p});
        pos++;
        return blobData[p];
    }

    private byte[] loadBlob(int blobNo) throws IOException {
        if ( blobNo >= blobs.size()) {
            throw new IOException("Attempt to read beyond end of stream");
        }
        if ( blobNo != loadedBlobNo ) {
            Blob b = blobs.get(blobNo);
            InputStream in = b.getNewStream();

            // strangely, b.length seems to exceed blobLength. Reading causes b.length causes an exception.
            long bl = Math.min(blobSize, b.length());
            LOGGER.info("Loading Blob no {} block {} size {}", name, blobNo, bl);
            IOUtils.readFully(in, buffer, 0, (int) bl);
            blobLength = (int)bl;
            in.close();
            loadedBlobNo = blobNo;
            LOGGER.info("Loaded");
        }
        return buffer;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        while(len > 0) {
            int blobNo = (int) pos / blobSize;
            byte[] blobData = loadBlob(blobNo);
            int start = ((int) pos) - (blobNo * blobSize);
            if ( start >= blobLength) {
                LOGGER.info("EOF  {}:{} {} bytes from {} bytes starting at {} {} ", new Object[]{ name, blobNo, len, blobLength, start, length});
                throw new IOException("End of file");
            }
            LOGGER.info("Reading from {}:{}/{} bytes:{} blobLength:{} bytes starting at {} {} ", new Object[]{name, blobNo, blobs.size(), len, blobLength, start, length});
            int copyLen = Math.min(len, blobLength - start);
            System.arraycopy(blobData, start, b, offset, copyLen);
            pos = pos + copyLen;
            offset = offset + copyLen;
            len = len - copyLen;
        }
    }

    @Override
    public Set<String> readStringSet() throws IOException {
        final Set<String> set = new HashSet<String>();
        final int count = readInt();
        LOGGER.info("REading {} strings",count);
        for(int i=0;i<count;i++) {
            set.add(readString());
        }

        return set;
    }

    @Override
    public String readString() throws IOException {
        int length = readVInt();
        LOGGER.info("String length is {} ", length);
        final byte[] bytes = new byte[length];
        readBytes(bytes, 0, length);
        String r =  new String(bytes, 0, length, org.apache.lucene.util.IOUtils.CHARSET_UTF_8);
        LOGGER.info("String is {} ", r);
        return r;
    }



}
