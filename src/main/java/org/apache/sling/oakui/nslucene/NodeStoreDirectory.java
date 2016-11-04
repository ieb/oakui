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
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * Created by ieb on 21/10/2016.
 */
public class NodeStoreDirectory extends Directory {


    private static final Logger LOGGER = LoggerFactory.getLogger(NodeStoreDirectory.class);
    private final NodeState directory;
    private final Set<String> fileSet;
    private final String indexName;
    private String[] files;
    private LockFactory lockFactory;

    public NodeStoreDirectory(NodeState oakIndex, String indexName) {
        super();
        this.lockFactory = NoLockFactory.getNoLockFactory();
        LOGGER.info("Loading {} ",  indexName);
        NodeState ns = oakIndex;
        for (String child : indexName.split("/")) {
            ns = ns.getChildNode(child);
        }
        directory = ns.getChildNode(":data");
        fileSet = ImmutableSet.copyOf(directory.getChildNodeNames());
        files = fileSet.toArray(new String[fileSet.size()]);
        LOGGER.info("Directory initialsed with {} ", fileSet);
        this.indexName = indexName;
    }

    @Override
    public String[] listAll() throws IOException {
        return files;
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return fileSet.contains(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        throw new UnsupportedOperationException("Directory is read only");
    }

    @Override
    public long fileLength(String name) throws IOException {
        if ( fileExists(name)) {
            long l = 0;
            for ( Blob b : directory.getChildNode(name).getProperty("jcr:data").getValue(Type.BINARIES) ) {
                l += b.length();
            }
            LOGGER.info("File {} length is  {} ", name, l);
            return l;
        }
        return 0;
    }

    public long getLastModified(String name) throws IOException {
        if ( fileExists(name) && directory.getChildNode(name).hasProperty("jcr:lastModified") ) {
            PropertyState ps = directory.getChildNode(name).getProperty("jcr:lastModified");
            return ps.getValue(Type.LONG);
        }
        return 0;
    }


    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        throw new UnsupportedOperationException("Directory is read only");
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        throw new UnsupportedOperationException("Directory is read only");
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return openInput(name);
    }

    public OakIndexInput openInput(String name) throws IOException {
        NodeState file = directory.getChildNode(name);
        if (file != null && file.exists()) {
            return new OakIndexInput(name, file, indexName);
        } else {
            String msg = String.format("[%s] %s", indexName, name);
            throw new FileNotFoundException(msg);
        }
    }

    @Override
    public Lock makeLock(String name) {
        return  lockFactory.makeLock(name);
    }

    @Override
    public void clearLock(String name) throws IOException {
        lockFactory.clearLock(name);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void setLockFactory(LockFactory lockFactory) throws IOException {
        this.lockFactory = lockFactory;
    }

    @Override
    public LockFactory getLockFactory() {
        return lockFactory;
    }

}
