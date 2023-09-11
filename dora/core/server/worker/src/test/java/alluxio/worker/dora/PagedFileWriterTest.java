/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.dora;

import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.membership.MembershipManager;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit test for {@link PagedFileWriter}.
 */
public class PagedFileWriterTest {
  private PagedDoraWorker mWorker;
  public PagedFileWriter mPagedFileWriter;
  private CacheManager mCacheManager;
  private MembershipManager mMembershipManager;
  private final InstancedConfiguration mConf = Configuration.copyGlobal();
  private final EmbeddedChannel mEmbeddedChannel = new EmbeddedChannel();
  private TemporaryFolder mTestFolder = new TemporaryFolder();
  @Before
  public void before() throws IOException {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.MEM);
    mConf.set(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_DIR, mTestFolder.newFolder("rocks"));
    CacheManagerOptions cacheManagerOptions = CacheManagerOptions.createForWorker(mConf);
    PageMetaStore pageMetaStore = PageMetaStore.create(cacheManagerOptions);
    mCacheManager = CacheManager.Factory.create(mConf, cacheManagerOptions, pageMetaStore);
    mMembershipManager = MembershipManager.Factory.create(mConf);
    mWorker = new PagedDoraWorker(
        new AtomicReference<>(1L), mConf, mCacheManager, mMembershipManager);
    String localUfsRoot = mTestFolder.getRoot().getAbsolutePath();
    Path path = Paths.get(localUfsRoot, "testFile" + UUID.randomUUID());

//    mPagedFileWriter = new PagedFileWriter(mWorker, path.toString(), mCacheManager,
//        ???, mConf.get(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE));
  }
}
