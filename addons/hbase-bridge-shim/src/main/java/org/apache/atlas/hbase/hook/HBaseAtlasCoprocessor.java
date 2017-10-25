/**
 *
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
package org.apache.atlas.hbase.hook;

import java.io.IOException;
import java.util.List;

import org.apache.atlas.plugin.classloader.AtlasPluginClassLoader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.util.Pair;
import java.util.Set;


public class HBaseAtlasCoprocessor implements MasterObserver, RegionObserver, RegionServerObserver, BulkLoadObserver {
    public static final Log LOG = LogFactory.getLog(HBaseAtlasCoprocessor.class);

    private static final String ATLAS_PLUGIN_TYPE               = "hbase";
    private static final String ATLAS_HBASE_HOOK_IMPL_CLASSNAME = "org.apache.atlas.hbase.hook.HBaseAtlasCoprocessor";

    private AtlasPluginClassLoader atlasPluginClassLoader = null;
    private Object                 impl                     = null;
    private MasterObserver         implMasterObserver       = null;
    private RegionObserver         implRegionObserver       = null;
    private RegionServerObserver   implRegionServerObserver = null;
    private BulkLoadObserver       implBulkLoadObserver     = null;

    public HBaseAtlasCoprocessor() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.HBaseAtlasCoprocessor()");
        }

        this.init();

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.HBaseAtlasCoprocessor()");
        }
    }

    private void init(){
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.init()");
        }

        try {
            atlasPluginClassLoader = AtlasPluginClassLoader.getInstance(ATLAS_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<?> cls = Class.forName(ATLAS_HBASE_HOOK_IMPL_CLASSNAME, true, atlasPluginClassLoader);

            activatePluginClassLoader();

            impl                     = cls.newInstance();
            implMasterObserver       = (MasterObserver)impl;
            implRegionObserver       = (RegionObserver)impl;
            implRegionServerObserver = (RegionServerObserver)impl;
            implBulkLoadObserver     = (BulkLoadObserver)impl;

        } catch (Exception e) {
            // check what need to be done
            LOG.error("Error Enabling RangerHbasePlugin", e);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.init()");
        }
    }



    @Override
    public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postScannerClose()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.postScannerClose(c, s);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postScannerClose()");
        }
    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, RegionScanner s) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postScannerOpen()");
        }

        final RegionScanner ret;

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.postScannerOpen(c, scan, s);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postScannerOpen()");
        }

        return ret;
    }

    @Override
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postStartMaster()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postStartMaster(ctx);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postStartMaster()");
        }

    }

    @Override
    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preAppend()");
        }

        final Result ret;

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.preAppend(c, append);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preAppend()");
        }

        return ret;
    }

    @Override
    public void preAssign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preAssign()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preAssign(c, regionInfo);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preAssign()");
        }
    }

    @Override
    public void preBalance(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preBalance()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preBalance(c);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preBalance()");
        }
    }

    @Override
    public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c, boolean newValue) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preBalanceSwitch()");
        }

        final boolean ret;

        try {
            activatePluginClassLoader();
            ret = implMasterObserver.preBalanceSwitch(c, newValue);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preBalanceSwitch()");
        }

        return ret;
    }

    @Override
    public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> familyPaths) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preBulkLoadHFile()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.preBulkLoadHFile(ctx, familyPaths);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preBulkLoadHFile()");
        }

    }

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preClose()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.preClose(e, abortRequested);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preClose()");
        }
    }

    @Override
    public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preDeleteTable()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preDeleteTable(c, tableName);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preDeleteTable()");
        }
    }

    @Override
    public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preDisableTable()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preDisableTable(c, tableName);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preDisableTable()");
        }
    }

    @Override
    public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preEnableTable()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preEnableTable(c, tableName);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preEnableTable()");
        }
    }

    @Override
    public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preExists()");
        }

        final boolean ret;

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.preExists(c, get, exists);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preExists()");
        }

        return ret;
    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preFlush()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.preFlush(e);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preFlush()");
        }
    }

    @Override
    public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment) throws IOException {
        final Result ret;

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preIncrement()");
        }

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.preIncrement(c, increment);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preIncrement()");
        }

        return ret;
    }

    @Override
    public void preMove(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo region, ServerName srcServer, ServerName destServer) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preMove()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preMove(c, region, srcServer, destServer);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preMove()");
        }
    }

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preOpen()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.preOpen(e);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preOpen()");
        }
    }

    @Override
    public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preScannerClose()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.preScannerClose(c, s);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preScannerClose()");
        }
    }

    @Override
    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preScannerNext()");
        }

        final boolean ret;

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.preScannerNext(c, s, result, limit, hasNext);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preScannerNext()");
        }

        return ret;
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, RegionScanner s) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preScannerOpen()");
        }

        final RegionScanner ret;

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.preScannerOpen(c, scan, s);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preScannerOpen()");
        }

        return ret;
    }

    @Override
    public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preShutdown()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preShutdown(c);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preShutdown()");
        }
    }

    @Override
    public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preStopMaster()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preStopMaster(c);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preStopMaster()");
        }
    }

    @Override
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preStopRegionServer()");
        }

        try {
            activatePluginClassLoader();
            implRegionServerObserver.preStopRegionServer(env);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preStopRegionServer()");
        }
    }

    @Override
    public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo, boolean force) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preUnassign()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preUnassign(c, regionInfo, force);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preUnassign()");
        }
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.start()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.start(env);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.start()");
        }
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> rEnv, Get get, List<Cell> result) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preGetOp()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.preGetOp(rEnv, get, result);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preGetOp()");
        }
    }

    @Override
    public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preRegionOffline()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preRegionOffline(c, regionInfo);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preRegionOffline()");
        }
    }

    @Override
    public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preCreateNamespace()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preCreateNamespace(ctx, ns);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preCreateNamespace()");
        }
    }

    @Override
    public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preDeleteNamespace()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preDeleteNamespace(ctx, namespace);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preDeleteNamespace()");
        }
    }

    @Override
    public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preModifyNamespace()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preModifyNamespace(ctx, ns);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preModifyNamespace()");
        }
    }

    @Override
    public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx, Region regionA, Region regionB) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preMerge()");
        }

        try {
            activatePluginClassLoader();
            implRegionServerObserver.preMerge(ctx, regionA, regionB);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preMerge()");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.stop()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.stop(env);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.stop()");
        }
    }

    @Override
    public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c, Region regionA, Region regionB, Region mergedRegion) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postMerge()");
        }

        try {
            activatePluginClassLoader();
            implRegionServerObserver.postMerge(c, regionA, regionB, mergedRegion);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postMerge()");
        }
    }

    @Override
    public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx, Region regionA, Region regionB, List<Mutation> metaEntries) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preMergeCommit()");
        }

        try {
            activatePluginClassLoader();
            implRegionServerObserver.preMergeCommit(ctx ,regionA, regionB, metaEntries);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preMergeCommit()");
        }
    }

    @Override
    public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx, Region regionA, Region regionB, Region mergedRegion) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postMergeCommit()");
        }

        try {
            activatePluginClassLoader();
            implRegionServerObserver.postMergeCommit(ctx ,regionA, regionB, mergedRegion);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postMergeCommit()");
        }
    }

    @Override
    public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx, Region regionA, Region regionB) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preRollBackMerge()");
        }

        try {
            activatePluginClassLoader();
            implRegionServerObserver.preRollBackMerge(ctx, regionA, regionB);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preRollBackMerge()");
        }
    }

    @Override
    public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx, Region regionA, Region regionB) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postRollBackMerge()");
        }

        try {
            activatePluginClassLoader();
            implRegionServerObserver.postRollBackMerge(ctx, regionA, regionB);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postRollBackMerge()");
        }
    }

    @Override
    public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preRollWALWriterRequest()");
        }

        try {
            activatePluginClassLoader();
            implRegionServerObserver.preRollWALWriterRequest(ctx);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preRollWALWriterRequest()");
        }
    }

    @Override
    public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postRollWALWriterRequest()");
        }

        try {
            activatePluginClassLoader();
            implRegionServerObserver.postRollWALWriterRequest(ctx);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postRollWALWriterRequest()");
        }
    }

    @Override
    public ReplicationEndpoint postCreateReplicationEndPoint(ObserverContext<RegionServerCoprocessorEnvironment> ctx, ReplicationEndpoint endpoint) {

        final ReplicationEndpoint ret;

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postCreateReplicationEndPoint()");
        }

        try {
            activatePluginClassLoader();
            ret = implRegionServerObserver.postCreateReplicationEndPoint(ctx, endpoint);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postCreateReplicationEndPoint()");
        }

        return ret;
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postOpen()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.postOpen(c);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postOpen()");
        }
    }

    @Override
    public void postLogReplay(ObserverContext<RegionCoprocessorEnvironment> c) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postLogReplay()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.postLogReplay(c);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postLogReplay()");
        }
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner) throws IOException {

        final InternalScanner ret;

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preFlush()");
        }

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.preFlush(c, store, scanner);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preFlush()");
        }

        return ret;
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postFlush()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.postFlush(c);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postFlush()");
        }
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postFlush()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.postFlush(c, store, resultFile);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postFlush()");
        }
    }

    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postClose()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.postClose(c, abortRequested);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postClose()");
        }
    }

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postGetOp()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.postGetOp(c, get, result);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postGetOp()");
        }
    }

    @Override
    public boolean postExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postExists()");
        }

        final boolean ret;

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.postExists(c, get, exists);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postExists()");
        }

        return ret;
    }

    @Override
    public void prePrepareTimeStampForDeleteVersion(ObserverContext<RegionCoprocessorEnvironment> c, Mutation mutation, Cell cell, byte[] byteNow, Get get) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.prePrepareTimeStampForDeleteVersion()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.prePrepareTimeStampForDeleteVersion(c, mutation, cell, byteNow, get);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.prePrepareTimeStampForDeleteVersion()");
        }
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preBatchMutate()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.preBatchMutate(c, miniBatchOp);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preBatchMutate()");
        }
    }

    @Override
    public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postBatchMutate()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.postBatchMutate(c, miniBatchOp);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postBatchMutate()");
        }
    }

    @Override
    public void postStartRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx, Operation operation) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postStartRegionOperation()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.postStartRegionOperation(ctx, operation);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postStartRegionOperation()");
        }
    }

    @Override
    public void postCloseRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx, Operation operation) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postCloseRegionOperation()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.postCloseRegionOperation(ctx, operation);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postCloseRegionOperation()");
        }
    }

    @Override
    public void postBatchMutateIndispensably(ObserverContext<RegionCoprocessorEnvironment> ctx, MiniBatchOperationInProgress<Mutation> miniBatchOp, boolean success) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postBatchMutateIndispensably()");
        }

        try {
            activatePluginClassLoader();
            implRegionObserver.postBatchMutateIndispensably(ctx, miniBatchOp, success);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postBatchMutateIndispensably()");
        }
    }

    @Override
    public Result preAppendAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c, Append append) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preAppendAfterRowLock()");
        }

        final Result ret;

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.preAppendAfterRowLock(c, append);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preAppendAfterRowLock()");
        }

        return ret;
    }

    @Override
    public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append, Result result) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postAppend()");
        }

        final Result ret;

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.postAppend(c, append, result);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postAppend()");
        }

        return ret;
    }

    @Override
    public Result preIncrementAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preIncrementAfterRowLock()");
        }

        final Result ret;

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.preIncrementAfterRowLock(c, increment);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preIncrementAfterRowLock()");
        }

        return ret;
    }

    @Override
    public Result postIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment, Result result) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postIncrement()");
        }

        final Result ret;

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.postIncrement(c, increment, result );
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postIncrement()");
        }

        return ret;
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postScannerNext()");
        }

        final boolean ret;

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.postScannerNext(c, s, result, limit, hasNext);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postScannerNext()");
        }

        return ret;
    }

    @Override
    public Cell postMutationBeforeWAL(ObserverContext<RegionCoprocessorEnvironment> ctx, MutationType opType, Mutation mutation, Cell oldCell, Cell newCell) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postMutationBeforeWAL()");
        }

        final Cell ret;

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.postMutationBeforeWAL(ctx, opType, mutation, oldCell, newCell);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postMutationBeforeWAL()");
        }

        return ret;
    }

    @Override
    public DeleteTracker postInstantiateDeleteTracker(ObserverContext<RegionCoprocessorEnvironment> ctx, DeleteTracker delTracker) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postInstantiateDeleteTracker()");
        }

        final DeleteTracker ret;

        try {
            activatePluginClassLoader();
            ret = implRegionObserver.postInstantiateDeleteTracker(ctx, delTracker);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postInstantiateDeleteTracker()");
        }

        return ret;
    }

    @Override
    public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postDeleteTable()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postDeleteTable(ctx, tableName);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postDeleteTable()");
        }
    }

    @Override
    public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preTruncateTable()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preTruncateTable(ctx, tableName);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preTruncateTable()");
        }
    }

    @Override
    public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postTruncateTable()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postTruncateTable(ctx, tableName);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postTruncateTable()");
        }
    }

    @Override
    public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postEnableTable()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postEnableTable(ctx, tableName);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postEnableTable()");
        }
    }

    @Override
    public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postDisableTable()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postDisableTable(ctx, tableName);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postDisableTable()");
        }
    }

    @Override
    public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region, ServerName srcServer, ServerName destServer) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postMove()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postMove(ctx, region, srcServer, destServer);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postMove()");
        }
    }

    @Override
    public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postAssign()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postAssign(ctx, regionInfo);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postAssign()");
        }
    }

    @Override
    public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo, boolean force) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postUnassign()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postUnassign(ctx, regionInfo, force);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postUnassign()");
        }
    }

    @Override
    public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postRegionOffline()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postRegionOffline(ctx, regionInfo);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postRegionOffline()");
        }
    }

    @Override
    public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx, List<RegionPlan> plans) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postBalance()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postBalance(ctx, plans);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postBalance()");
        }
    }

    @Override
    public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx, boolean oldValue, boolean newValue) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postBalanceSwitch()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postBalanceSwitch(ctx, oldValue, newValue);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postBalanceSwitch()");
        }
    }

    @Override
    public void preMasterInitialization(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preMasterInitialization()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preMasterInitialization(ctx);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preMasterInitialization()");
        }
    }

    @Override
    public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postCreateNamespace()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postCreateNamespace(ctx, ns);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postCreateNamespace()");
        }
    }

    @Override
    public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postDeleteNamespace()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postDeleteNamespace(ctx, namespace);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postDeleteNamespace()");
        }
    }

    @Override
    public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postModifyNamespace()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postModifyNamespace(ctx, ns);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postModifyNamespace()");
        }
    }

    @Override
    public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preGetNamespaceDescriptor()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preGetNamespaceDescriptor(ctx, namespace);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preGetNamespaceDescriptor()");
        }
    }

    @Override
    public void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postGetNamespaceDescriptor()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postGetNamespaceDescriptor(ctx, ns);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postGetNamespaceDescriptor()");
        }
    }

    @Override
    public void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<NamespaceDescriptor> descriptors) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preListNamespaceDescriptors()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preListNamespaceDescriptors(ctx, descriptors);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preListNamespaceDescriptors()");
        }
    }

    @Override
    public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<NamespaceDescriptor> descriptors) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postListNamespaceDescriptors()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postListNamespaceDescriptors(ctx, descriptors);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postListNamespaceDescriptors()");
        }
    }

    @Override
    public void preTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.preTableFlush()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.preTableFlush(ctx, tableName);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.preTableFlush()");
        }
    }

    @Override
    public void postTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postTableFlush()");
        }

        try {
            activatePluginClassLoader();
            implMasterObserver.postTableFlush(ctx, tableName);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postTableFlush()");
        }
    }

    private void activatePluginClassLoader() {
        if(atlasPluginClassLoader != null) {
            atlasPluginClassLoader.activate();
        }
    }

    private void deactivatePluginClassLoader() {
        if(atlasPluginClassLoader != null) {
            atlasPluginClassLoader.deactivate();
        }
    }



    // TODO : need override annotations for all of the following methods
    public void preMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx, Set<TableName> tables, String targetGroup) throws IOException {}
    public void postMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx, Set<TableName> tables, String targetGroup) throws IOException {}
    public void preRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String name) throws IOException {}
    public void postRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String name) throws IOException {}
    public void preBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String groupName) throws IOException {}
    public void postBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String groupName, boolean balancerRan) throws IOException {}
    public void preAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name) throws IOException {}
    public void postAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name) throws IOException {}
}
