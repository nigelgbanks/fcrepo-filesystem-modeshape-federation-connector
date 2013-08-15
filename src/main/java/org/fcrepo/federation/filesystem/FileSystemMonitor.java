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

import org.slf4j.Logger;

/**
 * Monitors a FileSystem for changes to its files/folders.
 */
public class FileSystemMonitor implements Runnable {

    private WatchService watchService;

    private String rootDirectory = null;

    private volatile boolean shutdown;

    private static final Logger logger = getLogger(FileSystemMonitor.class);

    public FileSystemMonitor(final FileSystemConnector connector) throws IOException {
        logger.debug("Initializing FileSystemMonitor on directory: {}",
                     rootDirectory);
        rootDirectory = connector.getDirectoryPath();
        this.shutdown = false;
    }

    @Override
    public void run() {
        logger.debug("Now executing FileSystemMonitor.run()...");
        while (!this.shutdown) {
            try {
                Thread.sleep(200);
            }
            catch(final InterruptedException e) {
                logger.debug("FileSystemMonitor.run() interrupted.");
                this.shutdown = true;
            }
        }
    }

    public void shutdown() {
        logger.debug("Shutting down FileSystemMonitor on FileSystemConnector on directory: {}",
                     this.rootDirectory);
        this.shutdown = true;
    }
}
