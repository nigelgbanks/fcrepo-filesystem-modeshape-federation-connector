/*
 * ModeShape (http://www.modeshape.org)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of
 * individual contributors.
 *
 * ModeShape is free software. Unless otherwise indicated, all code in ModeShape
 * is licensed to you under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * ModeShape is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.fcrepo.connector.filesystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import org.infinispan.schematic.Schematic;
import org.infinispan.schematic.document.Document;
import org.infinispan.schematic.document.EditableDocument;
import org.infinispan.schematic.document.Json;
import org.modeshape.jcr.cache.DocumentStoreException;
import org.modeshape.jcr.cache.document.DocumentTranslator;
import org.modeshape.jcr.federation.spi.ExtraPropertiesStore;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.Property;

/**
 * An {@link ExtraPropertiesStore} implementation that stores extra properties
 * in JSON sidecar files adjacent to the actual file or directory corresponding
 * to the external node.
 */
class JsonSidecarExtraPropertyStore implements ExtraPropertiesStore {

    public static final String DEFAULT_EXTENSION = ".modeshape.json";

    public static final String DEFAULT_RESOURCE_EXTENSION =
        ".content.modeshape.json";

    private final FileSystemConnector connector;

    private final DocumentTranslator translator;

    protected JsonSidecarExtraPropertyStore(
        final FileSystemConnector connector, final DocumentTranslator translator) {
        this.connector = connector;
        this.translator = translator;
    }

    protected String getExclusionPattern() {
        return "(.+)\\.(content\\.)?modeshape\\.json$";

    }

    @Override
    public Map<Name, Property> getProperties(final String id) {
        final File sidecarFile = sidecarFile(id);
        if (!sidecarFile.exists()) {
            return NO_PROPERTIES;
        }
        try {
            final Document document = read(new FileInputStream(sidecarFile));
            final Map<Name, Property> results = new HashMap<Name, Property>();
            translator.getProperties(document, results);
            return results;
        } catch (final IOException e) {
            throw new DocumentStoreException(id, e);
        }
    }

    @Override
    public void updateProperties(final String id,
        final Map<Name, Property> properties) {
        final File sidecarFile = sidecarFile(id);
        try {
            EditableDocument document = null;
            if (!sidecarFile.exists()) {
                if (properties.isEmpty()) {
                    return;
                }
                sidecarFile.createNewFile();
                document = Schematic.newDocument();
            } else {
                final Document existing =
                    read(new FileInputStream(sidecarFile));
                document = Schematic.newDocument(existing);
            }
            for (final Map.Entry<Name, Property> entry : properties.entrySet()) {
                final Property property = entry.getValue();
                if (property == null) {
                    translator.removeProperty(document, entry.getKey(), null);
                } else {
                    translator.setProperty(document, property, null);
                }
            }
            write(document, new FileOutputStream(sidecarFile));
        } catch (final IOException e) {
            throw new DocumentStoreException(id, e);
        }
    }

    @Override
    public void storeProperties(final String id,
        final Map<Name, Property> properties) {
        final File sidecarFile = sidecarFile(id);
        try {
            if (!sidecarFile.exists()) {
                if (properties.isEmpty()) {
                    return;
                }
                sidecarFile.createNewFile();
            }
            final EditableDocument document = Schematic.newDocument();
            for (final Property property : properties.values()) {
                if (property == null) {
                    continue;
                }
                translator.setProperty(document, property, null);
            }
            write(document, new FileOutputStream(sidecarFile));
        } catch (final IOException e) {
            throw new DocumentStoreException(id, e);
        }
    }

    protected Document read(final InputStream stream) throws IOException {
        return Json.read(stream);
    }

    protected void write(final Document document, final OutputStream stream)
        throws IOException {
        Json.write(document, stream);
    }

    protected String extension() {
        return DEFAULT_EXTENSION;
    }

    protected String resourceExtension() {
        return DEFAULT_EXTENSION;
    }

    @Override
    public boolean removeProperties(final String id) {
        final File file = sidecarFile(id);
        if (!file.exists()) {
            return false;
        }
        file.delete();
        return true;
    }

    protected File sidecarFile(final String id) {
        final File actualFile = connector.fileFor(id);
        String extension = extension();
        if (connector.isContentNode(id)) {
            extension = resourceExtension();
        }
        return new File(actualFile.getAbsolutePath() + extension);
    }
}
