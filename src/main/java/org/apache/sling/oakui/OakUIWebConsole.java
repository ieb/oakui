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


package org.apache.sling.oakui;

import java.io.*;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.apache.sling.jcr.api.SlingRepository;
import org.apache.sling.oakui.nslucene.NodeStoreDirectory;
import org.apache.sling.oakui.nslucene.OakIndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Supports an an Oak UI in the WebConsole.
 */
@Component(immediate=true)
@Properties(value={
        @Property(name="felix.webconsole.label", value="oakui"),
        @Property(name="felix.webconsole.title", value="Oak UI"),
        @Property(name="felix.webconsole.category", value="Sling"),
})
@Service(value=Servlet.class)
public class OakUIWebConsole extends HttpServlet {

    private static final String UTF_8 = "UTF-8";
    /**
     *
     */
    private static final long serialVersionUID = 8582250327069097646L;
    private static final Logger LOGGER = LoggerFactory.getLogger(OakUIWebConsole.class);

    @Reference
    private SlingRepository slingRepository;
    private Pattern operationPattern = Pattern.compile("^\\/oakui\\/lucene\\/(?<index>.*)\\/(?<file>.*)\\.(?<op>.*).json$");
    private Pattern zipOperationPattern = Pattern.compile("^\\/oakui\\/lucene\\/(?<index>.*)\\/(?<file>.*)\\.(?<op>.*).zip$");
    private File localFileLocation = new File("fsresources");


    @Activate
    private void activate(Map<String, Object> properties) {
        LOGGER.info("Performing init");
        LOGGER.info("Local FL {} ",localFileLocation.getAbsolutePath());
    }


    @Override
    public void init() throws ServletException {
    }

    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws ServletException,
            IOException {
        // the webconsole is rendered client side using AngularJS which gets feeds from the server.
        // The page itself is static, as are all the assets.
        // either serve the static request, or the data requests.
        InputStream in = null;
        try {
            String pathInfo = request.getPathInfo();
            if ( "/oakui/res/ui/oakui.js".equals(pathInfo)) {
                response.setContentType("text/javascript");
                in = getResourceAsStream("oakui.js");
                IOUtils.copy(in, response.getWriter(), UTF_8);
            } else if ( "/oakui/res/ui/angular-1.2.26.js".equals(pathInfo) ) {
                response.setContentType("text/javascript");
                in = getResourceAsStream("angular-1.2.26.js");
                IOUtils.copy(in, response.getWriter(), UTF_8);
            } else if ( "/oakui".equals(pathInfo)) {
                response.setContentType("text/html");
                in = getResourceAsStream("oakui.html");
                IOUtils.copy(in, response.getWriter(), UTF_8);
            } else if ( "/oakui/lucene.json".equals(pathInfo)) {
                sendLuceneInfo(request, response);
            } else {
                Matcher matchOperation = operationPattern.matcher(pathInfo);
                Matcher zipmatchOperation = zipOperationPattern.matcher(pathInfo);
                if ( matchOperation.matches() ) {
                    String index = matchOperation.group("index");
                    String operation = matchOperation.group("op");
                    String file = matchOperation.group("file");
                    LOGGER.info("Operation {} {} {} ", new Object[]{index, file, operation});
                    if ("an".equals(operation)) {
                        analyseSegment(request, response, index, file);
                    } else if ("do".equals(operation)) {
                        downloadSegment(request, response, index, file);
                    } else {
                        response.sendError(HttpServletResponse.SC_BAD_REQUEST);
                    }
                } else if ( zipmatchOperation.matches() ) {
                    String index = zipmatchOperation.group("index");
                    String operation = zipmatchOperation.group("op");
                    String file = zipmatchOperation.group("file");
                    LOGGER.info("Operation {} {} {} ", new Object[]{index, file, operation});
                    if ("do".equals(operation)) {
                        downloadSegment(request, response, index, file);
                    } else {
                        response.sendError(HttpServletResponse.SC_BAD_REQUEST);
                    }
                } else {
                    response.sendError(HttpServletResponse.SC_NOT_FOUND);
                }
            }
        } catch (Exception e) { // NOSONAR
            LOGGER.error(e.getMessage(),e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Check logs for details");
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                LOGGER.debug(e.getMessage(), e);
            } catch (NullPointerException e) {
                LOGGER.debug(e.getMessage(), e);
            }
        }

    }

    private InputStream getResourceAsStream(String name) {
        try {
            File f = new File(localFileLocation, name);
            return new FileInputStream(f);
        } catch (FileNotFoundException e) {
            return this.getClass().getResourceAsStream("/res/ui/"+name);
        }
    }

    @Nonnull
    private NodeStore getNodeStore()  {
        try {
            // this could be static, but as is doesnt cost much.
            Field managerStore  = null;
            try {
                managerStore = slingRepository.getClass().getDeclaredField("manager");
            } catch ( NoSuchFieldException e) {
                LOGGER.info("Sling Repository (type {}) does not have a field manager ", slingRepository.getClass());
                throw new RuntimeException("Unable to get the NodeStore from the repository", e);
            }
            if (!managerStore.isAccessible()) {
                managerStore.setAccessible(true);
            }
            // no idea what manager is, so get the class on the fly and find the field
            Object o = managerStore.get(slingRepository);
            Field storeField = null;
            try {
                storeField = o.getClass().getDeclaredField("store");
            } catch ( NoSuchFieldException e) {
                LOGGER.info("Sling Repository Manager (type:{}) does not have a field store ",o.getClass());
                throw new RuntimeException("Unable to get the NodeStore from the repository", e);
            }
            if (!storeField.isAccessible()) {
                storeField.setAccessible(true);
            }
            return (NodeStore) storeField.get(o);
        } catch (Exception e) {
            throw new RuntimeException("Unable to get the NodeStore from the repository", e);
        }
    }


    private void sendLuceneInfo(@Nonnull HttpServletRequest request,
                                @Nonnull HttpServletResponse response) throws JSONException, IOException, LoginException {
        // already authenticated as admin. The requests are not Sling Requests, no option here.
        NodeStore ns = getNodeStore();
        NodeState oakIndex = ns.getRoot().getChildNode("oak:index");
        JSONArray definitions = new JSONArray();
        collectIndexDefinitions(oakIndex, "", definitions);
        JSONObject j = new JSONObject();
        j.put("indexes", definitions);
        response.setContentType("application/json; charset=utf-8");
        response.getWriter().println(j.toString(4));
    }

    private void collectIndexDefinitions(@Nonnull NodeState node, @Nonnull String path, @Nonnull JSONArray definitions) throws JSONException {
        for (String n : node.getChildNodeNames()) {
            if ( ! n.startsWith(":") ) {
                NodeState cn = node.getChildNode(n);
                if ( cn.hasProperty("type") && "lucene".equals(cn.getProperty("type").getValue(Type.STRING)) ) {
                    definitions.put(buildDefinition(path + "/" + n, cn));
                } else {
                    collectIndexDefinitions(cn, path+"/"+n, definitions);
                }
            }
        }
    }

    @Nonnull
    private JSONObject buildDefinition(@Nonnull String path,
                                       @Nonnull NodeState definitionNode) throws JSONException {
        JSONObject definition = getProperties(definitionNode);
        definition.put("oakui_path", "lucene"+path);
        JSONArray files = new JSONArray();
        definition.put("files", files);
        if ( definitionNode.hasChildNode(":data")) {
            NodeState directoryNode = definitionNode.getChildNode(":data");
            for(String name : directoryNode.getChildNodeNames()) {
                JSONObject fileDefinition = getProperties(directoryNode.getChildNode(name));
                fileDefinition.put("name", name);
                // assume that names dont clash in files.
                files.put(fileDefinition);
            }
        }

        return  definition;
    }

    @Nonnull
    private JSONObject getProperties(@Nonnull NodeState definitionNode) throws JSONException {
        JSONObject definition = new JSONObject();
        for ( PropertyState ps : definitionNode.getProperties()) {
            String jsonName = ps.getName().replace(":","_"); // : causes all sorts of problems processing json in a browser, so convert it.
            Type t = ps.getType();
            if ( Type.BINARIES.equals(t)) {
                JSONArray pv = new JSONArray();
                Iterable<Blob> io = ps.getValue(Type.BINARIES);
                for (Blob o : io) {
                    JSONObject bo = new JSONObject();
                    bo.put("length", o.length());
                    bo.put("cid", o.getContentIdentity());
                    pv.put(bo);
                }
                definition.put(jsonName, pv);
            } else if ( Type.BINARY.equals(t)) {
                JSONObject bo = new JSONObject();
                Blob o = ps.getValue(Type.BINARY);
                bo.put("length", o.length());
                bo.put("cid", o.getContentIdentity());
                definition.put(jsonName, bo);
            } else if (ps.isArray()) {
                JSONArray pv = new JSONArray();
                Iterable<? extends Object> io = (Iterable<? extends Object>) ps.getValue(ps.getType());
                for (Object o : io) {
                    pv.put(o);
                }
                definition.put(jsonName, pv);
            } else {
                definition.put(jsonName, ps.getValue(ps.getType()));
            }
        }
        return definition;
    }


    private void downloadSegment(@Nonnull HttpServletRequest request,
                                 @Nonnull HttpServletResponse response,
                                 @Nonnull String index,
                                 @Nonnull String file) throws IOException {
        NodeStore ns = getNodeStore();
        NodeState oakIndex = ns.getRoot().getChildNode("oak:index");
        NodeStoreDirectory nsDirectory = new NodeStoreDirectory(oakIndex, index);

        response.setContentType("application/zip");
        response.setHeader("Content-Disposition", "attachment; filename=\""+index+".zip\";");

        ZipOutputStream zipOutputStream = new ZipOutputStream(response.getOutputStream());
        for ( String f : nsDirectory.listAll()) {
            ZipEntry ze = new ZipEntry(f);
            ze.setSize(nsDirectory.fileLength(f));
            ze.setTime(nsDirectory.getLastModified(f));
            zipOutputStream.putNextEntry(ze);
            OakIndexInput input = nsDirectory.openInput(f);
            transferBytes(input, zipOutputStream);
            zipOutputStream.closeEntry();
        }
        zipOutputStream.finish();
    }

    private void transferBytes(OakIndexInput input, ZipOutputStream zipOutputStream) throws IOException {
        byte[] buffer = new byte[4096];
        long l = input.length();
        while(l > 0) {
            int rl = (int) Math.min(buffer.length, l);
            input.readBytes(buffer,0,rl);
            zipOutputStream.write(buffer, 0, rl);
            l = l - rl;
        }
    }


    private void analyseSegment(@Nonnull HttpServletRequest request,
                                @Nonnull HttpServletResponse response,
                                @Nonnull String index,
                                @Nonnull String file) throws JSONException, IOException {
        // load the commit info and get all the files.
        // Need a directory implementation that works off the location, OakDirectory isn't exported, so will have to re-create.
        NodeStore ns = getNodeStore();
        NodeState oakIndex = ns.getRoot().getChildNode("oak:index");
        JSONObject out = new JSONObject();
        NodeStoreDirectory nsDirectory = new NodeStoreDirectory(oakIndex, index);
        //FSDirectory fsDirectory = new SimpleFSDirectory(new File("/Users/ieb/Adobe/CQ/6.2/crx-quickstart/repository/index/lucene-1476792800724/data"));
        SegmentInfos segmentCommitInfos = new SegmentInfos();
        try {
            if ("segments.gen".equals(file)) {
                // segments.gen is actually a pre v3 segments file, to get the current commit open without specifying the segments file.
                segmentCommitInfos.read(nsDirectory);
            } else {
                segmentCommitInfos.read(nsDirectory, file);
            }
            Iterator<SegmentCommitInfo> sci =  segmentCommitInfos.iterator();
            JSONArray commits = new JSONArray();
            while(sci.hasNext()) {
                SegmentCommitInfo sc = sci.next();
                JSONObject commitInfo = new JSONObject();
                JSONArray files = new JSONArray();
                for (String f : sc.files() ) {
                    files.put(f);
                }

                commitInfo.put("files",files);
                commitInfo.put("delcount",sc.getDelCount());
                commitInfo.put("delgen",sc.getDelGen());
                commitInfo.put("hasDeletions",sc.hasDeletions());
                commitInfo.put("hasFieldUpdates",sc.hasFieldUpdates());
                commitInfo.put("sizeInBytes",sc.sizeInBytes());
                commitInfo.put("fieldInfosGen",sc.getFieldInfosGen());
                commitInfo.put("nextDelGen",sc.getNextDelGen());
                commitInfo.put("version",sc.info.getVersion());
                commitInfo.put("diagnostics",sc.info.getDiagnostics());
                commitInfo.put("doccount",sc.info.getDocCount());
                commitInfo.put("name",sc.info.name);
                commitInfo.put("useCompoundFile",sc.info.getUseCompoundFile());

                commits.put(commitInfo);
            }
            out.put("commits", commits);
        } catch ( CorruptIndexException e) {
            LOGGER.info(e.getMessage(), e);
            out.put("corruption", e.getMessage());
        }
        response.setContentType("application/json; charset=utf-8");
        response.getWriter().println(out.toString(4));
    }


    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        // the webconsole is rendered client side using AngularJS which gets feeds from the server.
        // The page itself is static, as are all the assets.
        // either serve the static request, or the data requests.
        InputStream in = null;
        try {
            String pathInfo = request.getPathInfo();
            Matcher matchOperation = operationPattern.matcher(pathInfo);
            if ( matchOperation.matches() ) {
                String index = matchOperation.group("index");
                String operation = matchOperation.group("op");
                String file = matchOperation.group("file");
                LOGGER.info("Operation {} {} {} ",new Object[]{index,file, operation});
                if ("da".equals(operation)) {
                    damageSegment(request, response, index, file);
                } else if ( "re".equals(operation)) {
                    revertSegment(request, response, index, file);
                } else {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST);
                }
            } else {
                response.sendError(HttpServletResponse.SC_NOT_FOUND);
            }
        } catch (Exception e) { // NOSONAR
            LOGGER.error(e.getMessage(),e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Check logs for details");
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                LOGGER.debug(e.getMessage(), e);
            } catch (NullPointerException e) {
                LOGGER.debug(e.getMessage(), e);
            }
        }

    }

    private void revertSegment(@Nonnull HttpServletRequest request,
                                @Nonnull HttpServletResponse response,
                                @Nonnull String index,
                                @Nonnull String file) throws JSONException, IOException {
        // This needs a IndexWriter to be opened, with the commit in the IndexConfguration and then the index saved.
        // Since there can be only one index writer in the JVM and there is no access to the locks that control that
        // from this bundle, revertSegement cant be implemented here.
        response.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED);
    }

    private void damageSegment(@Nonnull HttpServletRequest request,
                               @Nonnull HttpServletResponse response,
                               @Nonnull String index,
                               @Nonnull String file) throws JSONException, IOException {
        // This would be good for testing, just not certain how to implement it.
        response.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED);
    }

}
