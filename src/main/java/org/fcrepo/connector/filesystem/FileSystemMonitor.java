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
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

import org.slf4j.Logger;

/**
 * Monitors a FileSystem for changes to its files/folders.
 */
public class FileSystemMonitor implements Runnable {

    private WatchService watchService;

    private FileSystemConnector connector = null;

    private String directoryPath = null;

    private final HashMap<Path, ArrayList<Path>> tracker =
        new HashMap<Path, ArrayList<Path>>();

    private volatile boolean shutdown = false;

    private static final Logger logger = getLogger(FileSystemMonitor.class);

    /**
     * Monitors the given directory and all sub-directories for changes.
     * 
     * @param connector
     * @param directoryPath
     * @throws IOException
     */
    public FileSystemMonitor(final FileSystemConnector connector,
        final String directoryPath) throws IOException {
        logger.debug("Initializing FileSystemMonitor on directory: {}",
            directoryPath);
        this.connector = connector;
        this.directoryPath = directoryPath;
        try {
            this.watchService = FileSystems.getDefault().newWatchService();
        } catch (final IOException e) {
            throw new Error("FileSystemMonitor failed to create watchService.",
                e);
        }
    }

    /**
     * Monitor the root directory and all sub-directories for changes.
     */
    @Override
    public void run() {
        logger.debug("Now executing FileSystemMonitor.run()...");
        final File directory = new File(directoryPath);
        // First set up watchers for each directory in the tree.
        watch(directory);
        while (!shutdown) {
            try {
                // Watch for any new files/directories, watch any new
                // directories that have been created.
                pollWatchService();
            } catch (final InterruptedException e) {
                logger.debug("FilesSystemMonitor.run() was interrupted.");
                shutdown = true;
            }
        }
        logger.debug("So the run function is exiting... I guess");
    }

    /**
     * Stop polling for changes and exit.
     */
    public void shutdown() {
        logger
            .debug(
                "Shutting down FileSystemMonitor on FileSystemConnector on directory: {}",
                directoryPath);
        shutdown = true;
    }

    /**
     * Registers a watcher for the given directory.
     * 
     * @param directory
     */
    private void registerToWatcher(final File directory) {
        // Ignore files as they can't be watched.
        if (directory.isFile()) {
            return;
        }
        logger.trace("registerToWatcher: {}", directory.getName());
        final Path path = directory.toPath();
        try {
            path.register(watchService, ENTRY_CREATE, ENTRY_DELETE,
                ENTRY_MODIFY);
            logger.info("FileSystemMonitor started watching a directory: " +
                path.toAbsolutePath());
        } catch (final IOException e) {
            throw new Error("FileSystemMonitor failed to monitor directory: " +
                directory.getAbsolutePath(), e);
        }
    }

    /**
     * Track's the file directory tree so we can send the correct delete events.
     * The Java watcher system doesn't emit delete events for sub-directories or
     * files when their parent directory is deleted.
     * 
     * @param file
     */
    private void track(final File file) {
        final Path path = file.toPath();
        final Path parent = file.getParentFile().toPath();
        ArrayList<Path> children = tracker.get(parent);
        if (children == null) {
            children = new ArrayList<Path>();
            tracker.put(parent, children);
        }
        children.add(path);
    }

    /**
     * Register the given file/directory and its sub-tree to the watcher.
     * 
     * @param directory
     * @throws IOException
     */
    private void watch(final File file) {
        track(file);
        registerToWatcher(file);
        connector.fireCreateEvent(file);
        if (file.isDirectory()) {
            for (final File child : file.listFiles()) {
                watch(child);
            }
        }
    }

    /**
     * Polls the watch service handling any events if they occur.
     * 
     * @throws InterruptedException
     */
    private void pollWatchService() throws InterruptedException {
        final WatchKey key = watchService.poll(2, SECONDS);
        if (key != null) {
            handleEvents(key, key.pollEvents());
        }
    }

    /**
     * Handles all events given events for the given watch key.
     * 
     * @param key
     * @param events
     */
    private void handleEvents(final WatchKey key,
        final List<WatchEvent<?>> events) {
        final Path parent = (Path) key.watchable();
        logger.debug("An event occured in parent {}", parent.toAbsolutePath());
        for (final WatchEvent<?> event : events) {
            Path path = (Path) event.context();
            path = parent.resolve(path);
            @SuppressWarnings("unchecked")
            final Kind<Path> kind = (Kind<Path>) event.kind();
            logger.debug("Received an event at context: {} of kind: {}", path
                .toAbsolutePath(), kind.name());
            handleEvent(kind, path.toFile());
        }
        key.reset();
    }

    /**
     * Handles the given event for the given file.
     * 
     * @param kind; either created/modified/deleted
     * @param file; the file that the event occurred on/to.
     */
    private void handleEvent(final Kind<Path> kind, final File file) {
        if (kind == ENTRY_CREATE) {
            // Watch this file/directory and any sub-directories. This will
            // generate created events for the given file and any children
            // of the given file.
            watch(file);
        } else if (kind == ENTRY_MODIFY) {
            // We don't dispatch MODIFY events for directories as they don't
            // indicate what happened within the directory.
            if (file.isFile()) {
                connector.fireModifyEvent(file);
            }
        } else if (kind == ENTRY_DELETE) {
            // When a directory is deleted the Java watch services does not
            // generate events for any files or sub-directories within
            // it, so we must do that ourselves. The WatchKeys for
            // sub-directories will be correctly cancelled by the watch service
            // so we don't need to worry about deallocating
            // them.
            recursivelyFireDeleteEvent(file);
        }
    }

    /**
     * Recursively fire delete events for the given file/directory's sub-tree.
     * 
     * @param file
     */
    private void recursivelyFireDeleteEvent(final File file) {
        connector.fireDeleteEvent(file);
        final ArrayList<Path> children = tracker.remove(file.toPath());
        if (children != null) {
            for (final Path child : children) {
                recursivelyFireDeleteEvent(child.toFile());
            }
        }
    }
}
