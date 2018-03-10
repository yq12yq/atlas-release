/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.hbase.util;


import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.hook.AtlasHookException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Pattern;


public class ImportHBaseEntities extends ImportHBaseEntitiesBase {
    private static final Logger LOG = LoggerFactory.getLogger(ImportHBaseEntities.class);
    private static final String NAMESPACE_FLAG = "-n";
    private static final String TABLE_FLAG     = "-t";
    private static final String NAMESPACE_FULL_FLAG = "--namespace";
    private static final String TABLE_FULL_FLAG     = "--tablename";
    private static String       namespaceToImport;
    private static String       tableToImport;

    public static void main(String[] args) throws AtlasHookException {
        try {
            if (checkArgs(args)) {
                ImportHBaseEntities importHBaseEntities = new ImportHBaseEntities(args);
                if (importNameSpace) {
                    namespaceToImport = cmd.getOptionValue("n");
                } else if (importTable) {
                    tableToImport = cmd.getOptionValue("t");
                }
                importHBaseEntities.execute();
            }
        } catch(Exception e) {
            throw new AtlasHookException("ImportHBaseEntities failed.", e);
        }
    }

    public ImportHBaseEntities(String[] args) throws Exception {
        super(args);
    }

    public boolean execute() throws Exception {
        boolean ret = false;
        if (hbaseAdmin != null) {
            if (StringUtils.isEmpty(namespaceToImport) && StringUtils.isEmpty(tableToImport)) {
                NamespaceDescriptor[] namespaceDescriptors = hbaseAdmin.listNamespaceDescriptors();
                if (!ArrayUtils.isEmpty(namespaceDescriptors)) {
                    for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
                        String namespace = namespaceDescriptor.getName();
                        importNameSpace(namespace);
                    }
                }
                TableDescriptor[] tableDescriptors = hbaseAdmin.listTables();
                if (!ArrayUtils.isEmpty(tableDescriptors)) {
                    for (TableDescriptor tableDescriptor : tableDescriptors) {
                        String tblName = tableDescriptor.getTableName().getNameAsString();
                        importTable(tblName);
                    }
                }
                ret = true;
            } else if (!StringUtils.isEmpty(namespaceToImport)){
                importNameSpace(namespaceToImport);
                ret = true;
            } else  if (!StringUtils.isEmpty(tableToImport)) {
                importTable(tableToImport);
                ret = true;
            }
        }
        return ret;
    }

    public String importNameSpace(final String nameSpace) throws Exception {
        NamespaceDescriptor namespaceDescriptor = hbaseAdmin.getNamespaceDescriptor(nameSpace);
        createOrUpdateNameSpace(namespaceDescriptor);
        return namespaceDescriptor.getName();
    }

    public String importTable(final String tableName) throws Exception {
        String ret = null;

        TableDescriptor tableDescriptor = null;
        String          tableNameStr    = null;
        List<TableDescriptor> tableDescriptors = hbaseAdmin.listTableDescriptors(Pattern.compile(tableName));
        if (!tableDescriptors.isEmpty()) {
            for (TableDescriptor tblDescriptor : tableDescriptors) {
                ret = tableNameStr = tblDescriptor.getTableName().getNameWithNamespaceInclAsString();
                if (tableName.equals(tableNameStr)) {
                    tableDescriptor = tblDescriptor;
                    break;
                }
            }
            String namespace =  tableDescriptor.getTableName().getNamespaceAsString();
            NamespaceDescriptor nsDescriptor = hbaseAdmin.getNamespaceDescriptor(namespace);
            AtlasEntity nsEntity = createOrUpdateNameSpace(nsDescriptor);
            ColumnFamilyDescriptor[] columnFamilyDescriptors = tableDescriptor.getColumnFamilies();
            createOrUpdateTable(namespace, tableNameStr, nsEntity, tableDescriptor, columnFamilyDescriptors);
        }

        return ret;
    }

    private static boolean checkArgs(String[] args) throws Exception {
        boolean ret = true;
        String option = args.length > 0 ? args[0] : null;
        String value  = args.length > 1 ? args[1] : null;

        if (option != null && value == null) {
            if (option.equalsIgnoreCase(NAMESPACE_FLAG) || option.equalsIgnoreCase(NAMESPACE_FULL_FLAG) ||
                    option.equalsIgnoreCase(TABLE_FLAG) || option.equalsIgnoreCase(TABLE_FULL_FLAG)) {
                System.out.println("Usage: import-hive.sh [-n <namespace> OR --namespace <namespace>] [-t <table> OR --table <table>]");
                ret = false;
                throw new Exception("Missing arguments..");
            }
        }
        return ret;
    }
}
