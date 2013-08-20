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

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.Logger;

/**
 * Monitors a FileSystem for changes to its files/folders.
 */
public class FileSystemMonitor implements Runnable {

    private WatchService watchService;

    private List<WatchKey> watchKeys = new ArrayList<WatchKey>();

    private FileSystemConnector connector = null;

    private String directoryPath = null;

    private volatile boolean shutdown = false;

    private static final Logger logger = getLogger(FileSystemMonitor.class);

    public FileSystemMonitor(final FileSystemConnector connector, final String directoryPath) throws IOException {
        logger.debug("Initializing FileSystemMonitor on directory: {}",
                     directoryPath);
        this.connector = connector;
        this.directoryPath = directoryPath;
        try {
            this.watchService = FileSystems.getDefault().newWatchService();
        } catch (IOException e) {
            throw new Error("FileSystemMonitor failed to create watchService.", e);
        }
    }

    @Override
    public void run() {
        logger.debug("Now executing FileSystemMonitor.run()...");
        File directory = new File(directoryPath);
        recursivelyWatchDirectories(directory);
        while (!shutdown) {
            try {
                pollWatchService();
            } catch (final InterruptedException e) {
                logger.debug("FilesSystemMonitor.run() was interrupted.");
                shutdown = true;
            }
        }
    }

    public void shutdown() {
        logger.debug("Shutting down FileSystemMonitor on FileSystemConnector on directory: {}",
                     directoryPath);
        shutdown = true;
    }

    private void recursivelyWatchDirectories(final File directory) {
        try {
            watchDirectory(directory);
            for (final File file : directory.listFiles()) {
                if (file.isDirectory()) {
                    recursivelyWatchDirectories(file);
                }
                else {
                    connector.fireCreateEvent(file);
                }
            }
        } catch (IOException e) {
            throw new Error("FileSystemMonitor failed to monitor directory: " +
                            directory.getAbsolutePath(), e);
        }
    }

    private void watchDirectory(final File directory) throws IOException {
        Path path = Paths.get(directory.toURI());
        WatchKey watchKey = path.register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        watchKeys.add(watchKey);
        logger.info("FileSystemMonitor started watching a directory: " +
                    path.toAbsolutePath());
        connector.fireCreateEvent(directory);
    }

    private void broadcastEvent(final WatchEvent<?> event) {
        // Path parent = (Path) key.watchable();
        // Path path = (Path) event.context();
        // path = parent.resolve(path);

        // @SuppressWarnings("unchecked")
        // final Kind<Path> kind = (Kind<Path>) event.kind();
        // logger.debug("Received an event at context: {} of kind: {}",
        //              path.toAbsolutePath(), kind.name());
    }

    private void pollWatchService() throws InterruptedException {
        final WatchKey key = watchService.poll(2, SECONDS);
        if (key != null) {
            handleEvents(key, key.pollEvents());
        }
    }

    private void handleEvents(final WatchKey key, final List<WatchEvent<?>> events) {
        Path parent = (Path) key.watchable();
        logger.debug("An event occured in parent {}", parent.toAbsolutePath());
        for (final WatchEvent<?> event : events) {
            Path path = (Path) event.context();
            path = parent.resolve(path);
            File file = new File(path.toUri());

            @SuppressWarnings("unchecked")
            final Kind<Path> kind = (Kind<Path>) event.kind();
            logger.debug("Received an event at context: {} of kind: {}",
                         path.toAbsolutePath(), kind.name());
            handleEvent(kind, file);
        }
        key.reset();
    }

    private void handleEvent(final Kind<Path> kind, final File file) {
        if (kind == ENTRY_CREATE) {
            if (file.isDirectory()) {
                // Watch this directory and any sub-directories.
                recursivelyWatchDirectories(file);
            }
            else {
                connector.fireCreateEvent(file);
            }
        }
        else if (kind == ENTRY_MODIFY) {
            // We don't dispatch MODIFY events for directories as they don't
            // indicate what happened within the directory.
            if (file.isFile()) {
                connector.fireModifyEvent(file);
            }
        }
        else if (kind == ENTRY_DELETE) {
            // When we delete a top level directory we only send an delete event
            // for that directory. We don't generate events for any files or
            // sub-directories within it. The WatchKeys for sub-directories will
            // be correctly cancelled by the watch service so we don't need to
            // worry about that.
            connector.fireDeleteEvent(file);
        }
    }
}
