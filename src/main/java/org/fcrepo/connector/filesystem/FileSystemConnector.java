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

package org.fcrepo.connector.filesystem;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;

import org.infinispan.schematic.document.Document;
import org.modeshape.common.util.FileUtil;
import org.modeshape.common.util.IoUtil;
import org.modeshape.common.util.SecureHash;
import org.modeshape.common.util.SecureHash.Algorithm;
import org.modeshape.common.util.StringUtil;
import org.modeshape.jcr.JcrI18n;
import org.modeshape.jcr.JcrLexicon;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.api.value.DateTime;
import org.modeshape.jcr.cache.DocumentStoreException;
import org.modeshape.jcr.federation.NoExtraPropertiesStorage;
import org.modeshape.jcr.federation.spi.ConnectorChangeSet;
import org.modeshape.jcr.federation.spi.DocumentChanges;
import org.modeshape.jcr.federation.spi.DocumentReader;
import org.modeshape.jcr.federation.spi.DocumentWriter;
import org.modeshape.jcr.federation.spi.PageKey;
import org.modeshape.jcr.federation.spi.Pageable;
import org.modeshape.jcr.federation.spi.WritableConnector;
import org.modeshape.jcr.value.BinaryKey;
import org.modeshape.jcr.value.BinaryValue;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.Property;
import org.modeshape.jcr.value.PropertyType;
import org.modeshape.jcr.value.basic.BasicPropertyFactory;
import org.modeshape.jcr.value.binary.ExternalBinaryValue;
import org.modeshape.jcr.value.binary.UrlBinaryValue;

/**
 * {@link Connector} implementation that exposes a single directory on the local
 * file system. This connector has several properties that must be configured
 * via the {@link RepositoryConfiguration}:
 * <ul>
 * <li><strong><code>directoryPath</code></strong> - The path to the file or
 * folder that is to be accessed by this connector.</li>
 * <li><strong><code>
 * readOnly</code></strong> - A boolean flag that specifies whether this source
 * can create/modify/remove files and directories on the file system to reflect
 * changes in the JCR content. By default, sources are not read-only.</li>
 * <li><strong><code>addMimeTypeMixin</code></strong> - A boolean flag that
 * specifies whether this connector should add the 'mix:mimeType' mixin to the
 * 'nt:resource' nodes to include the 'jcr:mimeType' property. If set to
 * <code>true</code>, the MIME type is computed immediately when the
 * 'nt:resource' node is accessed, which might be expensive for larger files.
 * This is <code>false</code> by default.</li>
 * <li><strong><code>extraPropertyStorage</code></strong> - An optional string
 * flag that specifies how this source handles "extra" properties that are not
 * stored via file system attributes. See {@link #extraPropertiesStorage} for
 * details. By default, extra properties are stored in the same Infinispan cache
 * that the repository uses.</li>
 * <li><strong><code>exclusionPattern</code></strong> - Optional property that
 * specifies a regular expression that is used to determine which files and
 * folders in the underlying file system are not exposed through this connector.
 * Files and folders with a name that matches the provided regular expression
 * will <i>not</i> be exposed by this source.</li>
 * <li><strong><code>
 * inclusionPattern</code></strong> - Optional property that specifies a regular
 * expression that is used to determine which files and folders in the
 * underlying file system are exposed through this connector. Files and folders
 * with a name that matches the provided regular expression will be exposed by
 * this source.</li>
 * </ul>
 * Inclusion and exclusion patterns can be used separately or in combination.
 * For example, consider these cases:
 * <table cellspacing="0" cellpadding="1" border="1">
 * <tr>
 * <th>Inclusion Pattern</th>
 * <th>Exclusion Pattern</th>
 * <th>Examples</th>
 * </tr>
 * <tr>
 * <td>(.+)\\.txt$</td>
 * <td></td>
 * <td>Includes only files and directories whose names end in "<code>.txt</code>
 * " (e.g., "<code>something.txt</code>" ), but does not include files and other
 * folders such as "<code>something.jar</code>" or "
 * <code>something.txt.zip</code>".</td>
 * </tr>
 * <tr>
 * <td>(.+)\\.txt$</td>
 * <td>my.txt</td>
 * <td>Includes only files and directories whose names end in "<code>.txt</code>
 * " (e.g., "<code>something.txt</code>" ) with the exception of "
 * <code>my.txt</code>", and does not include files and other folders such as "
 * <code>something.jar</code>" or " <code>something.txt.zip</code>".</td>
 * </tr>
 * <tr>
 * <td>my.txt</td>
 * <td>.+</td>
 * <td>Excludes all files and directories except any named "<code>my.txt</code>
 * ".</td>
 * </tr>
 * </table>
 */
public class FileSystemConnector extends WritableConnector implements Pageable {

    private static final String FILE_SEPARATOR = System
        .getProperty("file.separator");

    private static final String DELIMITER = "/";

    private static final String NT_FOLDER = "nt:folder";

    private static final String NT_FILE = "nt:file";

    private static final String NT_RESOURCE = "nt:resource";

    private static final String MIX_MIME_TYPE = "mix:mimeType";

    private static final String JCR_PRIMARY_TYPE = "jcr:primaryType";

    private static final String JCR_DATA = "jcr:data";

    private static final String JCR_MIME_TYPE = "jcr:mimeType";

    private static final String JCR_ENCODING = "jcr:encoding";

    private static final String JCR_CREATED = "jcr:created";

    private static final String JCR_CREATED_BY = "jcr:createdBy";

    private static final String JCR_LAST_MODIFIED = "jcr:lastModified";

    private static final String JCR_LAST_MODIFIED_BY = "jcr:lastModified";

    private static final String JCR_CONTENT = "jcr:content";

    private static final String JCR_CONTENT_SUFFIX = DELIMITER + JCR_CONTENT;

    private static final int JCR_CONTENT_SUFFIX_LENGTH = JCR_CONTENT_SUFFIX
        .length();

    private static final String EXTRA_PROPERTIES_JSON = "json";

    private static final String EXTRA_PROPERTIES_LEGACY = "legacy";

    private static final String EXTRA_PROPERTIES_NONE = "none";

    /**
     * The string path for a {@link File} object that represents the top-level
     * directory accessed by this connector. This is set via reflection and is
     * required for this connector.
     */
    private String directoryPath;

    private File directory;

    /**
     * A string that is created in the
     * {@link #initialize(NamespaceRegistry, NodeTypeManager)} method that
     * represents the absolute path to the {@link #directory}. This path is
     * removed from an absolute path of a file to obtain the ID of the node.
     */
    private String directoryAbsolutePath;

    private int directoryAbsolutePathLength;

    /**
     * A boolean flag that specifies whether this connector should add the 'mix:
     * mimeType' mixin to the 'nt:resource' nodes to include the 'jcr:mimeType'
     * property. If set to <code>true</code>, the MIME type is computed
     * immediately when the 'nt:resource' node is accessed, which might be
     * expensive for larger files. This is <code>false</code> by default.
     */
    private final boolean addMimeTypeMixin = false;

    /**
     * The regular expression that, if matched by a file or folder, indicates
     * that the file or folder should be included.
     */
    private String inclusionPattern;

    /**
     * The regular expression that, if matched by a file or folder, indicates
     * that the file or folder should be ignored.
     */
    private String exclusionPattern;

    /**
     * The maximum number of children a folder will expose at any given time.
     */
    private final int pageSize = 20;

    /**
     * The {@link FilenameFilter} implementation that is instantiated in the
     * {@link #initialize(NamespaceRegistry, NodeTypeManager)} method.
     */
    private InclusionExclusionFilenameFilter filenameFilter;

    /**
     * Thread pool which manages the file system monitors.
     */
    private ExecutorService threadPool;

    /**
     * A string that specifies how the "extra" properties are to be stored,
     * where an "extra" property is any JCR property that cannot be stored
     * natively on the file system as file attributes. This field is set via
     * reflection, and the value is expected to be one of these valid values:
     * <ul>
     * <li>"<code>store</code>" - Any extra properties are stored in the same
     * Infinispan cache where the content is stored. This is the default and is
     * used if the actual value doesn't match any of the other accepted values.</li>
     * <li>"<code>json</code>" - Any extra properties are stored in a JSON file
     * next to the file or directory.</li>
     * <li>"<code>legacy</code>" - Any extra properties are stored in a
     * ModeShape 2.x-compatible file next to the file or directory. This is
     * generally discouraged unless you were using ModeShape 2.x and have a
     * directory structure that already contains these files.</li>
     * <li>"<code>none</code>" - An extra properties that prevents the storage
     * of extra properties by throwing an exception when such extra properties
     * are defined.</li>
     * </ul>
     */
    private String extraPropertiesStorage;

    private NamespaceRegistry registry;

    @Override
    public void initialize(final NamespaceRegistry registry,
        final NodeTypeManager nodeTypeManager) throws RepositoryException,
        IOException {
        super.initialize(registry, nodeTypeManager);
        this.registry = registry;

        // Initialize the directory path field that has been set via reflection
        // when this method is called...
        checkFieldNotNull(directoryPath, "directoryPath");
        directory = new File(directoryPath);
        if (!directory.exists() || !directory.isDirectory()) {
            final String msg =
                JcrI18n.fileConnectorTopLevelDirectoryMissingOrCannotBeRead
                    .text(getSourceName(), "directoryPath");
            throw new RepositoryException(msg);
        }
        if (!directory.canRead() && !directory.setReadable(true)) {
            final String msg =
                JcrI18n.fileConnectorTopLevelDirectoryMissingOrCannotBeRead
                    .text(getSourceName(), "directoryPath");
            throw new RepositoryException(msg);
        }
        directoryAbsolutePath = directory.getAbsolutePath();
        if (!directoryAbsolutePath.endsWith(FILE_SEPARATOR)) {
            directoryAbsolutePath = directoryAbsolutePath + FILE_SEPARATOR;
        }
        // Does NOT include the separtor.
        directoryAbsolutePathLength =
            directoryAbsolutePath.length() - FILE_SEPARATOR.length();

        // Initialize the filename filter ...
        filenameFilter = new InclusionExclusionFilenameFilter();
        if (exclusionPattern != null) {
            filenameFilter.setExclusionPattern(exclusionPattern);
        }
        if (inclusionPattern != null) {
            filenameFilter.setInclusionPattern(inclusionPattern);
        }

        // Set up the extra properties storage ...
        if (EXTRA_PROPERTIES_JSON.equalsIgnoreCase(extraPropertiesStorage)) {
            final JsonSidecarExtraPropertyStore store =
                new JsonSidecarExtraPropertyStore(this, translator());
            setExtraPropertiesStore(store);
            filenameFilter.setExtraPropertiesExclusionPattern(store
                .getExclusionPattern());
        } else if (EXTRA_PROPERTIES_LEGACY
            .equalsIgnoreCase(extraPropertiesStorage)) {
            final LegacySidecarExtraPropertyStore store =
                new LegacySidecarExtraPropertyStore(this);
            setExtraPropertiesStore(store);
            filenameFilter.setExtraPropertiesExclusionPattern(store
                .getExclusionPattern());
        } else if (EXTRA_PROPERTIES_NONE
            .equalsIgnoreCase(extraPropertiesStorage)) {
            setExtraPropertiesStore(new NoExtraPropertiesStorage(this));
        }
        // otherwise use the default extra properties storage

        // Set up the file system monitor.
        final BlockingQueue<Runnable> workQueue =
            new ArrayBlockingQueue<Runnable>(1);
        threadPool =
            new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, workQueue);
        getLogger().trace("Threadpool initialized.");
        threadPool.execute(new FileSystemMonitor(this, directoryPath));
        getLogger().trace("Monitor thread queued.");
    }

    /**
     * Get the namespace registry.
     * 
     * @return the namespace registry; never null
     */
    NamespaceRegistry registry() {
        return registry;
    }

    /**
     * Utility method for determining if the supplied identifier is for the "
     * jcr:content" child node of a file. * Subclasses may override this method
     * to change the format of the identifiers, but in that case should also
     * override the {@link #fileFor(String)}, {@link #isRoot(String)}, and
     * {@link #idFor(File)} methods.
     * 
     * @param id the identifier; may not be null
     * @return true if the identifier signals the "jcr:content" child node of a
     *         file, or false otherwise
     * @see #isRoot(String)
     * @see #fileFor(String)
     * @see #idFor(File)
     */
    protected boolean isContentNode(final String id) {
        return id.endsWith(JCR_CONTENT_SUFFIX);
    }

    /**
     * Utility method for obtaining the {@link File} object that corresponds to
     * the supplied identifier. Subclasses may override this method to change
     * the format of the identifiers, but in that case should also override the
     * {@link #isRoot(String)}, {@link #isContentNode(String)}, and
     * {@link #idFor(File)} methods.
     * 
     * @param id the identifier; may not be null
     * @return the File object for the given identifier
     * @see #isRoot(String)
     * @see #isContentNode(String)
     * @see #idFor(File)
     */
    protected File fileFor(String id) {
        assert id.startsWith(DELIMITER);
        if (id.endsWith(DELIMITER)) {
            id = id.substring(0, id.length() - DELIMITER.length());
        }
        if (isContentNode(id)) {
            id = id.substring(0, id.length() - JCR_CONTENT_SUFFIX_LENGTH);
        }
        return new File(directory, id);
    }

    /**
     * Utility method for determining if the node identifier is the identifier
     * of the root node in this external source. Subclasses may override this
     * method to change the format of the identifiers, but in that case should
     * also override the {@link #fileFor(String)},
     * {@link #isContentNode(String)}, and {@link #idFor(File)} methods.
     * 
     * @param id the identifier; may not be null
     * @return true if the identifier is for the root of this source, or false
     *         otherwise
     * @see #isContentNode(String)
     * @see #fileFor(String)
     * @see #idFor(File)
     */
    protected boolean isRoot(final String id) {
        return DELIMITER.equals(id);
    }

    /**
     * Utility method for determining the node identifier for the supplied file.
     * Subclasses may override this method to change the format of the
     * identifiers, but in that case should also override the
     * {@link #fileFor(String)}, {@link #isContentNode(String)}, and
     * {@link #isRoot(String)} methods.
     * 
     * @param file the file; may not be null
     * @return the node identifier; never null
     * @see #isRoot(String)
     * @see #isContentNode(String)
     * @see #fileFor(String)
     */
    protected String idFor(final File file) {
        final String path = file.getAbsolutePath();
        if (!path.startsWith(directoryAbsolutePath)) {
            if (directory.getAbsolutePath().equals(path)) {
                // This is the root
                return DELIMITER;
            }
            final String msg =
                JcrI18n.fileConnectorNodeIdentifierIsNotWithinScopeOfConnector
                    .text(getSourceName(), directoryPath, path);
            throw new DocumentStoreException(path, msg);
        }
        String id = path.substring(directoryAbsolutePathLength);
        id = id.replaceAll(Pattern.quote(FILE_SEPARATOR), DELIMITER);
        assert id.startsWith(DELIMITER);
        return id;
    }

    /**
     * Utility method for creating a {@link BinaryValue} for the given
     * {@link File} object. Subclasses should rarely override this method.
     * 
     * @param file the file; may not be null
     * @return the BinaryValue; never null
     */
    protected ExternalBinaryValue binaryFor(final File file) {
        try {
            final byte[] sha1 = SecureHash.getHash(Algorithm.SHA_1, file);
            final BinaryKey key = new BinaryKey(sha1);
            return createBinaryValue(key, file);
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Utility method to create a {@link BinaryValue} object for the given file.
     * Subclasses should rarely override this method, since the
     * {@link UrlBinaryValue} will be applicable in most situations.
     * 
     * @param key the binary key; never null
     * @param file the file for which the {@link BinaryValue} is to be created;
     *        never null
     * @return the binary value; never null
     * @throws IOException if there is an error creating the value
     */
    protected ExternalBinaryValue createBinaryValue(final BinaryKey key,
        final File file) throws IOException {
        final URL content = createUrlForFile(file);
        return new UrlBinaryValue(key, getSourceName(), content, file.length(),
            file.getName(), getMimeTypeDetector());
    }

    /**
     * Construct a {@link URL} object for the given file, to be used within the
     * {@link Binary} value representing the "jcr:data" property of a
     * 'nt:resource' node.
     * <p>
     * Subclasses can override this method to transform the URL into something
     * different. For example, if the files are being served by a web server,
     * the overridden method might transform the file-based URL into the
     * corresponding HTTP-based URL.
     * </p>
     * 
     * @param file the file for which the URL is to be created; never null
     * @return the URL for the file; never null
     * @throws IOException if there is an error creating the URL
     */
    protected URL createUrlForFile(final File file) throws IOException {
        return file.toURI().toURL();
    }

    protected File createFileForUrl(final URL url) throws URISyntaxException {
        return new File(url.toURI());
    }

    /**
     * Utility method to determine if the file is excluded by the inclusion/
     * exclusion filter.
     * 
     * @param file the file
     * @return true if the file is excluded, or false if it is to be included
     */
    protected boolean isExcluded(final File file) {
        return !filenameFilter.accept(file.getParentFile(), file.getName());
    }

    /**
     * Utility method to ensure that the file is writable by this connector.
     * 
     * @param id the identifier of the node
     * @param file the file
     * @throws DocumentStoreException if the file is expected to be writable but
     *         is not or is excluded, or if the connector is readonly
     */
    protected void checkFileNotExcluded(final String id, final File file) {
        if (isExcluded(file)) {
            final String msg =
                JcrI18n.fileConnectorCannotStoreFileThatIsExcluded.text(
                    getSourceName(), id, file.getAbsolutePath());
            throw new DocumentStoreException(id, msg);
        }
    }

    /**
     * Checks if a document with the given id exists in the end-source.
     * 
     * @param id a {@code non-null} string.
     * @return {@code true} if such a document exists, {@code false} otherwise.
     */
    @Override
    public boolean hasDocument(final String id) {
        return fileFor(id).exists();
    }

    /**
     * Returns a {@link Document} instance representing the document with a
     * given id. The document should have a "proper" structure for it to be
     * usable by ModeShape.
     * 
     * @param id a {@code non-null} string
     * @return either an {@link Document} instance or {@code null}
     */
    @Override
    public Document getDocumentById(final String id) {
        final File file = fileFor(id);
        if (isExcluded(file) || !file.exists()) {
            return null;
        }
        final boolean isRoot = isRoot(id);
        final boolean isResource = isContentNode(id);
        DocumentWriter writer = null;
        File parentFile = file.getParentFile();
        if (isResource) {
            writer = newDocument(id);
            final BinaryValue binaryValue = binaryFor(file);
            writer.setPrimaryType(NT_RESOURCE);
            writer.addProperty(JCR_DATA, binaryValue);
            if (addMimeTypeMixin) {
                String mimeType = null;
                final String encoding = null; // We don't really know this
                try {
                    mimeType = binaryValue.getMimeType();
                } catch (final Throwable e) {
                    getLogger().error(e, JcrI18n.couldNotGetMimeType,
                        getSourceName(), id, e.getMessage());
                }
                writer.addProperty(JCR_ENCODING, encoding);
                writer.addProperty(JCR_MIME_TYPE, mimeType);
            }
            writer.addProperty(JCR_LAST_MODIFIED, factories().getDateFactory()
                .create(file.lastModified()));
            writer.addProperty(JCR_LAST_MODIFIED_BY, null); // ignored

            // make these binary not queryable. If we really want to query them,
            // we need to switch to external binaries
            writer.setNotQueryable();
            parentFile = file;
        } else if (file.isFile()) {
            writer = newDocument(id);
            writer.setPrimaryType(NT_FILE);
            writer.addProperty(JCR_CREATED, factories().getDateFactory()
                .create(file.lastModified()));
            writer.addProperty(JCR_CREATED_BY, null); // ignored
            final String childId =
                isRoot ? JCR_CONTENT_SUFFIX : id + JCR_CONTENT_SUFFIX;
            writer.addChild(childId, JCR_CONTENT);
        } else {
            writer = newFolderWriter(id, file, 0);
        }

        if (!isRoot) {
            // Set the reference to the parent ...
            final String parentId = idFor(parentFile);
            writer.setParents(parentId);
        }

        // Add the extra properties (if there are any), overwriting any
        // properties with the same names
        // (e.g., jcr:primaryType, jcr:mixinTypes, jcr:mimeType, etc.) ...
        writer.addProperties(extraPropertiesStore().getProperties(id));

        // Add the 'mix:mixinType' mixin; if other mixins are stored in the
        // extra properties, this will append ...
        if (addMimeTypeMixin) {
            writer.addMixinType(MIX_MIME_TYPE);
        }

        // Return the document ...
        return writer.document();
    }

    private DocumentWriter newFolderWriter(final String id, final File file,
        final int offset) {
        final boolean root = isRoot(id);
        final DocumentWriter writer = newDocument(id);
        writer.setPrimaryType(NT_FOLDER);
        writer.addProperty(JCR_CREATED, factories().getDateFactory().create(
            file.lastModified()));
        writer.addProperty(JCR_CREATED_BY, null); // ignored
        final File[] children = file.listFiles(filenameFilter);
        long totalChildren = 0;
        int nextOffset = 0;
        for (int i = 0; i < children.length; i++) {
            final File child = children[i];
            // Only include as a child if we can access and read the file.
            // Permissions might prevent us from reading the file, and the file
            // might not exist if it is a broken symlink
            // (see MODE-1768 for details).
            if (child.exists() && child.canRead() &&
                (child.isFile() || child.isDirectory())) {
                // we need to count the total accessible children
                totalChildren++;
                // only add a child if it's in the current page
                if (i >= offset && i < offset + pageSize) {
                    // We use identifiers that contain the file/directory name
                    final String childName = child.getName();
                    final String childId =
                        root ? DELIMITER + childName : id + DELIMITER +
                            childName;
                    writer.addChild(childId, childName);
                    nextOffset = i + 1;
                }
            }
        }
        // if there are still accessible children add the next page
        if (nextOffset < totalChildren) {
            writer.addPage(id, nextOffset, pageSize, totalChildren);
        }
        return writer;
    }

    /**
     * Returns the id of an external node located at the given path.
     * 
     * @param path a {@code non-null} string representing an exeternal path.
     * @return either the id of the document or {@code null}
     */
    @Override
    public String getDocumentId(final String path) {
        final String id = path; // this connector treats the ID as the path
        final File file = fileFor(id);
        return file.exists() ? id : null;
    }

    /**
     * Return the path(s) of the external node with the given identifier. The
     * resulting paths are from the point of view of the connector. For example,
     * the "root" node exposed by the connector wil have a path of "/".
     * 
     * @param id a {@code non-null} string
     * @return the connector-specific path(s) of the node, or an empty document
     *         if there is no such document; never null
     */
    @Override
    public Collection<String> getDocumentPathsById(final String id) {
        // this connector treats the ID as the path
        return Collections.singletonList(id);
    }

    /**
     * Returns a binary value which is connector specific and which is never
     * stored by ModeShape. Typically connectors who need this feature will have
     * their own subclasses of {@link ExternalBinaryValue}
     * 
     * @param id a {@code String} representing the id of the external binary
     *        which should have connector-specific meaning.
     * @return either a binary value implementation or {@code null} if there is
     *         no such value with the given id.
     */
    @Override
    public ExternalBinaryValue getBinaryValue(final String id) {
        try {
            final File f = createFileForUrl(new URL(id));
            return binaryFor(f);
        } catch (final IOException e) {
            throw new DocumentStoreException(id, e);
        } catch (final URISyntaxException e) {
            throw new DocumentStoreException(id, e);
        }
    }

    /**
     * Removes the document with the given id.
     * 
     * @param id a {@code non-null} string.
     * @return true if the document was removed, or false if there was no
     *         document with the given id
     */
    @Override
    public boolean removeDocument(final String id) {
        final File file = fileFor(id);
        checkFileNotExcluded(id, file);
        // Remove the extra properties at the old location ...
        extraPropertiesStore().removeProperties(id);
        // Now remove the file (if it is there) ...
        if (!file.exists()) {
            return false;
        }
        FileUtil.delete(file); // recursive delete
        return true;
    }

    /**
     * Stores the given document.
     * 
     * @param document a {@code non-null}
     *        {@link org.infinispan.schematic.document.Document} instance.
     * @throws DocumentStoreException if there is already a new document with
     *         the same identifier
     * @throws DocumentStoreException if one of the modified documents was
     *         removed by another session
     */
    @Override
    public void storeDocument(final Document document) {
        // Create a new directory or file described by the document ...
        final DocumentReader reader = readDocument(document);
        final String id = reader.getDocumentId();
        final File file = fileFor(id);
        checkFileNotExcluded(id, file);
        final File parent = file.getParentFile();
        if (!parent.exists()) {
            parent.mkdirs();
        }
        if (!parent.canWrite()) {
            final String parentPath = parent.getAbsolutePath();
            final String msg =
                JcrI18n.fileConnectorCannotWriteToDirectory.text(
                    getSourceName(), getClass(), parentPath);
            throw new DocumentStoreException(id, msg);
        }
        final String primaryType = reader.getPrimaryTypeName();
        final Map<Name, Property> properties = reader.getProperties();
        final ExtraProperties extraProperties = extraPropertiesFor(id, false);
        extraProperties.addAll(properties).except(JCR_PRIMARY_TYPE,
            JCR_CREATED, JCR_LAST_MODIFIED, JCR_DATA);
        try {
            if (NT_FILE.equals(primaryType)) {
                file.createNewFile();
            } else if (NT_FOLDER.equals(primaryType)) {
                file.mkdirs();
            } else if (isContentNode(id)) {
                final Property content = properties.get(JcrLexicon.DATA);
                final BinaryValue binary =
                    factories().getBinaryFactory().create(
                        content.getFirstValue());
                final OutputStream ostream =
                    new BufferedOutputStream(new FileOutputStream(file));
                IoUtil.write(binary.getStream(), ostream);
                if (!NT_RESOURCE.equals(primaryType)) {
                    // This is the "jcr:content" child, but the primary type is
                    // non-standard so record it as an extra property
                    extraProperties
                        .add(properties.get(JcrLexicon.PRIMARY_TYPE));
                }
            }
            extraProperties.save();
        } catch (final RepositoryException e) {
            throw new DocumentStoreException(id, e);
        } catch (final IOException e) {
            throw new DocumentStoreException(id, e);
        }
    }

    /**
     * Generates an identifier which will be assigned when a new document (aka.
     * child) is created under an existing document (aka.parent). This method
     * should be implemented only by connectors which support writing.
     * 
     * @param parentId a {@code non-null} {@link String} which represents the
     *        identifier of the parent under which the new document will be
     *        created.
     * @param newDocumentName a {@code non-null}
     *        {@link org.modeshape.jcr.value.Name} which represents the name
     *        that will be given to the child document
     * @param newDocumentPrimaryType a {@code non-null}
     *        {@link org.modeshape.jcr.value.Name} which represents the child
     *        document's primary type.
     * @return either a {@code non-null} {@link String} which will be assigned
     *         as the new identifier, or {@code null} which means that no
     *         "special" id format is required. In this last case, the
     *         repository will auto-generate a random id.
     * @throws org.modeshape.jcr.cache.DocumentStoreException if the connector
     *         is readonly.
     */
    @Override
    public String newDocumentId(final String parentId,
        final Name newDocumentName, final Name newDocumentPrimaryType) {
        final StringBuilder id = new StringBuilder(parentId);
        if (!parentId.endsWith(DELIMITER)) {
            id.append(DELIMITER);
        }

        // We're only using the name to check, which can be a bit dangerous if
        // users don't follow the JCR conventions. However, it matches what
        // "isContentNode(...)" does.
        final String childNameStr =
            getContext().getValueFactories().getStringFactory().create(
                newDocumentName);

        if (JCR_CONTENT.equals(childNameStr)) {
            // This is for the "jcr:content" node underneath a file node. Since
            // this doesn't actually result in a file or folder on the file
            // system (it's merged into the file for the parent 'nt:file' node),
            // we'll keep the "jcr" namespace prefix in the ID so that
            // 'isContentNode(...)' works properly ...
            id.append(childNameStr);
        } else {
            // File systems don't universally deal well with ':' in the names,
            // and when they do it can be a bit awkward. Since we don't often
            // expect the node NAMES to contain namespaces (at leat with this
            // connector), we'll just use the local part for the ID ...
            id.append(newDocumentName.getLocalName());
            if (!StringUtil.isBlank(newDocumentName.getNamespaceUri())) {
                // the FS connector does not support namespaces in names
                final String ns = newDocumentName.getNamespaceUri();
                getLogger().warn(JcrI18n.fileConnectorNamespaceIgnored,
                    getSourceName(), ns, id, childNameStr, parentId);
            }
        }
        return id.toString();
    }

    /**
     * Updates a document using the provided changes.
     * 
     * @param documentChanges a {@code non-null}
     *        {@link org.modeshape.jcr.federation.spi.DocumentChanges} object
     *        which contains granular information about all the changes.
     */
    @Override
    public void updateDocument(final DocumentChanges documentChanges) {
        String id = documentChanges.getDocumentId();

        final Document document = documentChanges.getDocument();
        final DocumentReader reader = readDocument(document);

        final File file = fileFor(id);

        // if we're dealing with the root of the connector, we can't process any
        // moves/removes because that would go "outside" the connector scope
        if (!isRoot(id)) {
            final String parentId = reader.getParentIds().get(0);
            File parent = file.getParentFile();
            final String newParentId = idFor(parent);
            if (!parentId.equals(newParentId)) {
                // The node has a new parent (via the 'update' method), meaning
                // it was moved ...
                final File newParent = fileFor(newParentId);
                final File newFile = new File(newParent, file.getName());
                file.renameTo(newFile);
                if (!parent.exists()) {
                    // In case they were removed since we created them ...
                    parent.mkdirs();
                }
                if (!parent.canWrite()) {
                    final String parentPath = newParent.getAbsolutePath();
                    final String msg =
                        JcrI18n.fileConnectorCannotWriteToDirectory.text(
                            getSourceName(), getClass(), parentPath);
                    throw new DocumentStoreException(id, msg);
                }
                parent = newParent;
                // Remove the extra properties at the old location ...
                extraPropertiesStore().removeProperties(id);
                // Set the id to the new location ...
                id = idFor(newFile);
            } else {
                // It is the same parent as before ...
                if (!parent.exists()) {
                    // In case they were removed since we created them ...
                    parent.mkdirs();
                }
                if (!parent.canWrite()) {
                    final String parentPath = parent.getAbsolutePath();
                    final String msg =
                        JcrI18n.fileConnectorCannotWriteToDirectory.text(
                            getSourceName(), getClass(), parentPath);
                    throw new DocumentStoreException(id, msg);
                }
            }
        }

        // Children renames have to be processed in the parent
        final DocumentChanges.ChildrenChanges childrenChanges =
            documentChanges.getChildrenChanges();
        final Map<String, Name> renamedChildren = childrenChanges.getRenamed();
        for (final String renamedChildId : renamedChildren.keySet()) {
            final File child = fileFor(renamedChildId);
            final Name newName = renamedChildren.get(renamedChildId);
            final String newNameStr =
                getContext().getValueFactories().getStringFactory().create(
                    newName);
            final File renamedChild = new File(file, newNameStr);
            if (!child.renameTo(renamedChild)) {
                getLogger().debug("Cannot rename {0} to {1}", child,
                    renamedChild);
            }
        }

        final String primaryType = reader.getPrimaryTypeName();
        final Map<Name, Property> properties = reader.getProperties();
        final ExtraProperties extraProperties = extraPropertiesFor(id, true);
        extraProperties.addAll(properties).except(JCR_PRIMARY_TYPE,
            JCR_CREATED, JCR_LAST_MODIFIED, JCR_DATA);
        try {
            if (NT_FILE.equals(primaryType)) {
                file.createNewFile();
            } else if (NT_FOLDER.equals(primaryType)) {
                file.mkdir();
            } else if (isContentNode(id)) {
                final Property content = reader.getProperty(JCR_DATA);
                final BinaryValue binary =
                    factories().getBinaryFactory().create(
                        content.getFirstValue());
                final OutputStream ostream =
                    new BufferedOutputStream(new FileOutputStream(file));
                IoUtil.write(binary.getStream(), ostream);
                if (!NT_RESOURCE.equals(primaryType)) {
                    // This is the "jcr:content" child, but the primary type is
                    // non-standard so record it as an extra property
                    extraProperties
                        .add(properties.get(JcrLexicon.PRIMARY_TYPE));
                }
            }
            extraProperties.save();
        } catch (final RepositoryException e) {
            throw new DocumentStoreException(id, e);
        } catch (final IOException e) {
            throw new DocumentStoreException(id, e);
        }
    }

    /**
     * Return a document which represents a page of children. The document for
     * the parent node should include as many children as desired, and then
     * include a reference to the next page of children with the {{PageWriter#
     * addPage(String, String, long, long)}} method. Each page returned by this
     * method should also include a reference to the next page.
     * 
     * @param pageKey a non-null {@link PageKey} instance, which offers
     *        information about the page that should be retrieved.
     * @return either a non-null page document or {@code null} indicating that
     *         such a page doesn't exist
     */
    @Override
    public Document getChildren(final PageKey pageKey) {
        final String parentId = pageKey.getParentId();
        final File folder = fileFor(parentId);
        assert folder.isDirectory();
        if (!folder.canRead()) {
            getLogger().debug("Cannot read the {0} folder",
                folder.getAbsolutePath());
            return null;
        }
        return newFolderWriter(parentId, folder, pageKey.getOffsetInt())
            .document();
    }

    /**
     * Gets the directory at which the external filesystem is rooted.
     */
    public String getDirectoryPath() {
        return directoryPath;
    }

    /**
     * Shutdown the connector by releasing all resources. This is called
     * automatically by ModeShape when this Connector instance is no longer
     * needed, and should never be called by the connector.
     */
    @Override
    public void shutdown() {
        threadPool.shutdown();
        getLogger().trace("FileSystemMonitor Threadpool shutdown.");
    }

    /**
     * Sends a change set with a new node event for the given file.
     * 
     * @param file the file; may not be null
     */
    public void fireCreateEvent(final File file) {

        final ConnectorChangeSet changes = newConnectorChangedSet();
        final String key = idFor(file);
        final Document doc = getDocumentById(key);
        final DocumentReader reader = readDocument(doc);
        getLogger().debug(
            "Firing create file event with\n\tkey {0}\n\tpathToNode {1}", key,
            key);
        changes.nodeCreated(key, "/", key, reader.getProperties());
        changes.publish(null);
    }

    /**
     * Sends a change set with a modify node event for the given file.
     * 
     * @param file the file; may not be null
     */
    public void fireModifyEvent(final File file) {
        final ConnectorChangeSet changes = newConnectorChangedSet();
        final String key = idFor(file);
        final Document doc = getDocumentById(key);
        final DocumentReader reader = readDocument(doc);
        // Not sure why we are using CREATED as the JCR value that has changed
        // LASTMODIFIED seems to make more sense?
        getLogger().debug(
            "Firing modify file event with\n\tkey {0}\n\tpathToNode {1}", key,
            key);
        final DateTime dt =
            this.factories().getDateFactory().create(
                System.currentTimeMillis() - 10000);

        final Property dtprop =
            new BasicPropertyFactory(factories()).create(JcrLexicon.CREATED,
                PropertyType.DATE, dt);

        changes.propertyChanged(key, key, reader.getProperty(JCR_CREATED),
            dtprop);
        changes.publish(null);
    }

    /**
     * Sends a change set with a delete node event for the given file.
     * 
     * @param file the file; may not be null
     */
    public void fireDeleteEvent(final File file) {
        final ConnectorChangeSet changes = newConnectorChangedSet();
        final String key = idFor(file);
        getLogger().debug(
            "Firing remove file event with\n\tkey {0}\n\tpathToNode {1}", key,
            key);
        changes.nodeRemoved(key, "/", key);
        changes.publish(null);
    }
}
