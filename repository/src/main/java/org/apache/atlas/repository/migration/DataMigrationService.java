/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.migration;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.impexp.ImportTypeDefProcessor;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration.Configuration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.service.Service;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

import static org.apache.atlas.AtlasConstants.ATLAS_MIGRATION_MODE_FILENAME;

@Component
public class DataMigrationService implements Service {
    private static final Logger LOG = LoggerFactory.getLogger(DataMigrationService.class);

    private static String ATLAS_MIGRATION_DATA_NAME     = "atlas-migration-data.json";
    private static String ATLAS_MIGRATION_TYPESDEF_NAME = "atlas-migration-typesdef.json";

    private final Configuration configuration;
    private final Thread        thread;

    @Inject
    public DataMigrationService(AtlasTypeDefStore typeDefStore, Configuration configuration,
                                GraphBackedSearchIndexer indexer, AtlasTypeDefStoreInitializer storeInitializer,
                                AtlasTypeRegistry typeRegistry) {
        this.configuration = configuration;
        this.thread        = new Thread(new FileImporter(typeDefStore, typeRegistry, storeInitializer, getFileName(), indexer));
    }

    @Override
    public void start() {
        Runtime.getRuntime().addShutdownHook(thread);
        thread.start();
    }

    @Override
    public void stop() {
        try {
            thread.join();
        } catch (InterruptedException e) {
            LOG.error("Data Migration: Interrupted", e);
        }
    }

    public String getFileName() {
        return configuration.getString(ATLAS_MIGRATION_MODE_FILENAME, "");
    }

    public static class FileImporter implements Runnable {
        private final AtlasTypeDefStore            typeDefStore;
        private final String                       importDirectory;
        private final GraphBackedSearchIndexer     indexer;
        private final AtlasTypeRegistry            typeRegistry;
        private final AtlasTypeDefStoreInitializer storeInitializer;

        public FileImporter(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry,
                            AtlasTypeDefStoreInitializer storeInitializer,
                            String directoryName, GraphBackedSearchIndexer indexer) {
            this.typeDefStore     = typeDefStore;
            this.typeRegistry     = typeRegistry;
            this.storeInitializer = storeInitializer;
            this.importDirectory  = directoryName;
            this.indexer          = indexer;
        }

        public void performImport() throws AtlasBaseException {
            try {
                performInit();

                FileInputStream fs = new FileInputStream(getFileFromImportDirectory(importDirectory, ATLAS_MIGRATION_DATA_NAME));

                typeDefStore.loadLegacyData(RelationshipCacheGenerator.get(typeRegistry), fs);
            } catch (Exception ex) {
                LOG.error("Import failed!", ex);
                throw new AtlasBaseException(ex);
            }
        }

        private void performInit() throws AtlasBaseException, AtlasException {
            storeInitializer.init();
            processIncomingTypesDef(getFileFromImportDirectory(importDirectory, ATLAS_MIGRATION_TYPESDEF_NAME));
            indexer.instanceIsActive();
        }

        @VisibleForTesting
        void processIncomingTypesDef(File typesDefFile) throws AtlasBaseException {
            try {
                String jsonStr = FileUtils.readFileToString(typesDefFile);
                AtlasTypesDef typesDef = AtlasType.fromJson(jsonStr, AtlasTypesDef.class);
                ImportTypeDefProcessor processor = new ImportTypeDefProcessor(typeDefStore, typeRegistry);
                processor.processTypes(typesDef, new AtlasImportResult());
            } catch (IOException e) {
                LOG.error("processIncomingTypesDef: Could not process file: {}! Imported data may not be usable.", typesDefFile.getName());
            }
        }

        private File getFileFromImportDirectory(String importDirectory, String fileName) {
            return Paths.get(importDirectory, fileName).toFile();
        }

        @Override
        public void run() {
            try {
                performImport();
            } catch (AtlasBaseException e) {
                LOG.error("Data Migration:", e);
            }
        }
    }
}
