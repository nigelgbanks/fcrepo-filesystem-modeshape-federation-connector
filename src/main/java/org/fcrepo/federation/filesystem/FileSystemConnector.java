/**
 * Copyright 2013 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fcrepo.federation.filesystem;

import static org.modeshape.jcr.api.JcrConstants.JCR_CONTENT;
import static org.modeshape.jcr.api.JcrConstants.JCR_DATA;
import static org.modeshape.jcr.api.JcrConstants.NT_FOLDER;
import static org.modeshape.jcr.api.JcrConstants.NT_RESOURCE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;

import org.infinispan.schematic.document.Document;
//import org.modeshape.connector.filesystem.FileSystemConnector;
import org.modeshape.jcr.JcrI18n;
import org.modeshape.jcr.JcrLexicon;
import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.api.value.DateTime;
import org.modeshape.jcr.cache.DocumentStoreException;
import org.modeshape.jcr.federation.spi.ConnectorChangeSet;
import org.modeshape.jcr.federation.spi.DocumentChanges;
import org.modeshape.jcr.federation.spi.DocumentReader;
import org.modeshape.jcr.federation.spi.DocumentWriter;
import org.modeshape.jcr.value.BinaryValue;
import org.modeshape.jcr.value.Property;
import org.modeshape.jcr.value.PropertyFactory;
import org.modeshape.jcr.value.PropertyType;
import org.modeshape.jcr.value.ValueFactories;
import org.modeshape.jcr.value.basic.BasicPropertyFactory;

/**
 * Extends the ModeShape FileSystemConnector class and adds event support.
 */
public class FileSystemConnector extends org.modeshape.connector.filesystem.FileSystemConnector {

    /**
     * The string path for a {@link File} object that represents the top-level
     * directory accessed by this connector. This is set via reflection and is
     * required for this connector.
     */
    private String directoryPath;

    private ExecutorService threadPool;

    /**
     * Initialize the connector. This is called automatically by ModeShape once
     * for each Connector instance, and should not be called by the connector.
     * By the time this method is called, ModeShape will have already set the
     * {{ExecutionContext}}, {{Logger}}, connector name, repository name
     * {@link #context}, and any fields that match configuration properties for
     * the connector.
     *
     * By default this method does nothing, so it should be overridden by
     * implementations to do a one-time initialization of any internal
     * components. For example, connectors can use the supplied {{registry}} and
     * {{nodeTypeManager}} parameters to register custom namespaces and node
     * types used by the exposed nodes.
     *
     * This is also an excellent place for connector to validate the
     * connector-specific fields set by ModeShape via reflection during
     * instantiation.
     *
     * @param registry the namespace registry that can be used to register
     *        custom namespaces; never null
     * @param nodeTypeManager the node type manager that can be used to register
     *        custom node types; never null
     * @throws RepositoryException if operations on the
     *         {@link NamespaceRegistry} or {@link NodeTypeManager} fail
     * @throws IOException if any stream based operations fail
     *         (like importing cnd files)
     */
    /*
    @Override
    public void initialize(final NamespaceRegistry registry,
                           final NodeTypeManager nodeTypeManager)
        throws RepositoryException, IOException {
        super.initialize(registry, nodeTypeManager);
        getLogger().trace("Initializing at " + this.directoryPath + " ...");
        final BlockingQueue<Runnable> workQueue =
            new ArrayBlockingQueue<Runnable>(1);
        threadPool =
            new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, workQueue);
        getLogger().trace("Threadpool initialized.");
        threadPool.execute(new FileSystemMonitor(this));
        getLogger().trace("Monitor thread queued.");
        }*/

    /**
     * Returns the id of an external node located at the given external path
     * within the connector's exposed tree of content.
     *
     * @param externalPath a non-null string representing an external path, or
     *        "/" for the top-level node exposed by the connector
     * @return either the id of the document or null
     */
    /*public abstract String getDocumentId(final String externalPath) {
      }*/

    /**
     * Returns a Document instance representing the document with a given id.
     * The document should have a "proper" structure for it to be usable by
     * ModeShape.
     *
     * @param id a {@code non-null} string
     * @return either an {@link Document} instance or {@code null}
     */
    /*public abstract Document getDocumentById(final String id) {
      }*/

    /**
     * Return the path(s) of the external node with the given identifier.
     * The resulting paths are from the point of view of the connector. For
     * example, the "root" node exposed by the connector will have a path of
     *  "/".
     *
     * @param id a null-null string
     * @return the connector-specific path(s) of the node, or an empty document
     *         if there is no such document; never null
     */
    /*public abstract Collection<String> getDocumentPathsById(final String id) {
      }*/

    /**
     */
    public void setDirectoryPath(final String directoryPath) {
        this.directoryPath = directoryPath;
    }

    /**
     * Gets the directory path for this Connector.
     *
     * @return the directory path
     */
    public String getDirectoryPath() {
        return this.directoryPath;
    }
}
