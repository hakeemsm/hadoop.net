using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Lang.Time;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestCacheDirectives
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestCacheDirectives));

		private static readonly UserGroupInformation unprivilegedUser = UserGroupInformation
			.CreateRemoteUser("unprivilegedUser");

		private static Configuration conf;

		private static MiniDFSCluster cluster;

		private static DistributedFileSystem dfs;

		private static NamenodeProtocols proto;

		private static NameNode namenode;

		private static NativeIO.POSIX.CacheManipulator prevCacheManipulator;

		static TestCacheDirectives()
		{
			NativeIO.POSIX.SetCacheManipulator(new NativeIO.POSIX.NoMlockCacheManipulator());
		}

		private const long BlockSize = 4096;

		private const int NumDatanodes = 4;

		private const long CacheCapacity = 64 * 1024 / NumDatanodes;

		// Most Linux installs will allow non-root users to lock 64KB.
		// In this test though, we stub out mlock so this doesn't matter.
		private static HdfsConfiguration CreateCachingConf()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetLong(DFSConfigKeys.DfsDatanodeMaxLockedMemoryKey, CacheCapacity);
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetLong(DFSConfigKeys.DfsCachereportIntervalMsecKey, 1000);
			conf.SetLong(DFSConfigKeys.DfsNamenodePathBasedCacheRefreshIntervalMs, 1000);
			// set low limits here for testing purposes
			conf.SetInt(DFSConfigKeys.DfsNamenodeListCachePoolsNumResponses, 2);
			conf.SetInt(DFSConfigKeys.DfsNamenodeListCacheDirectivesNumResponses, 2);
			return conf;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = CreateCachingConf();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDatanodes).Build();
			cluster.WaitActive();
			dfs = cluster.GetFileSystem();
			proto = cluster.GetNameNodeRpc();
			namenode = cluster.GetNameNode();
			prevCacheManipulator = NativeIO.POSIX.GetCacheManipulator();
			NativeIO.POSIX.SetCacheManipulator(new NativeIO.POSIX.NoMlockCacheManipulator());
			BlockReaderTestUtil.EnableHdfsCachingTracing();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Teardown()
		{
			// Remove cache directives left behind by tests so that we release mmaps.
			RemoteIterator<CacheDirectiveEntry> iter = dfs.ListCacheDirectives(null);
			while (iter.HasNext())
			{
				dfs.RemoveCacheDirective(iter.Next().GetInfo().GetId());
			}
			WaitForCachedBlocks(namenode, 0, 0, "teardown");
			if (cluster != null)
			{
				cluster.Shutdown();
			}
			// Restore the original CacheManipulator
			NativeIO.POSIX.SetCacheManipulator(prevCacheManipulator);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBasicPoolOperations()
		{
			string poolName = "pool1";
			CachePoolInfo info = new CachePoolInfo(poolName).SetOwnerName("bob").SetGroupName
				("bobgroup").SetMode(new FsPermission((short)0x1ed)).SetLimit(150l);
			// Add a pool
			dfs.AddCachePool(info);
			// Do some bad addCachePools
			try
			{
				dfs.AddCachePool(info);
				NUnit.Framework.Assert.Fail("added the pool with the same name twice");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("pool1 already exists", ioe);
			}
			try
			{
				dfs.AddCachePool(new CachePoolInfo(string.Empty));
				NUnit.Framework.Assert.Fail("added empty pool");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("invalid empty cache pool name", ioe);
			}
			try
			{
				dfs.AddCachePool(null);
				NUnit.Framework.Assert.Fail("added null pool");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("CachePoolInfo is null", ioe);
			}
			try
			{
				proto.AddCachePool(new CachePoolInfo(string.Empty));
				NUnit.Framework.Assert.Fail("added empty pool");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("invalid empty cache pool name", ioe);
			}
			try
			{
				proto.AddCachePool(null);
				NUnit.Framework.Assert.Fail("added null pool");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("CachePoolInfo is null", ioe);
			}
			// Modify the pool
			info.SetOwnerName("jane").SetGroupName("janegroup").SetMode(new FsPermission((short
				)0x1c0)).SetLimit(314l);
			dfs.ModifyCachePool(info);
			// Do some invalid modify pools
			try
			{
				dfs.ModifyCachePool(new CachePoolInfo("fool"));
				NUnit.Framework.Assert.Fail("modified non-existent cache pool");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("fool does not exist", ioe);
			}
			try
			{
				dfs.ModifyCachePool(new CachePoolInfo(string.Empty));
				NUnit.Framework.Assert.Fail("modified empty pool");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("invalid empty cache pool name", ioe);
			}
			try
			{
				dfs.ModifyCachePool(null);
				NUnit.Framework.Assert.Fail("modified null pool");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("CachePoolInfo is null", ioe);
			}
			try
			{
				proto.ModifyCachePool(new CachePoolInfo(string.Empty));
				NUnit.Framework.Assert.Fail("modified empty pool");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("invalid empty cache pool name", ioe);
			}
			try
			{
				proto.ModifyCachePool(null);
				NUnit.Framework.Assert.Fail("modified null pool");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("CachePoolInfo is null", ioe);
			}
			// Remove the pool
			dfs.RemoveCachePool(poolName);
			// Do some bad removePools
			try
			{
				dfs.RemoveCachePool("pool99");
				NUnit.Framework.Assert.Fail("expected to get an exception when " + "removing a non-existent pool."
					);
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Cannot remove " + "non-existent cache pool"
					, ioe);
			}
			try
			{
				dfs.RemoveCachePool(poolName);
				NUnit.Framework.Assert.Fail("expected to get an exception when " + "removing a non-existent pool."
					);
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Cannot remove " + "non-existent cache pool"
					, ioe);
			}
			try
			{
				dfs.RemoveCachePool(string.Empty);
				NUnit.Framework.Assert.Fail("removed empty pool");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("invalid empty cache pool name", ioe);
			}
			try
			{
				dfs.RemoveCachePool(null);
				NUnit.Framework.Assert.Fail("removed null pool");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("invalid empty cache pool name", ioe);
			}
			try
			{
				proto.RemoveCachePool(string.Empty);
				NUnit.Framework.Assert.Fail("removed empty pool");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("invalid empty cache pool name", ioe);
			}
			try
			{
				proto.RemoveCachePool(null);
				NUnit.Framework.Assert.Fail("removed null pool");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("invalid empty cache pool name", ioe);
			}
			info = new CachePoolInfo("pool2");
			dfs.AddCachePool(info);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreateAndModifyPools()
		{
			string poolName = "pool1";
			string ownerName = "abc";
			string groupName = "123";
			FsPermission mode = new FsPermission((short)0x1ed);
			long limit = 150;
			dfs.AddCachePool(new CachePoolInfo(poolName).SetOwnerName(ownerName).SetGroupName
				(groupName).SetMode(mode).SetLimit(limit));
			RemoteIterator<CachePoolEntry> iter = dfs.ListCachePools();
			CachePoolInfo info = iter.Next().GetInfo();
			NUnit.Framework.Assert.AreEqual(poolName, info.GetPoolName());
			NUnit.Framework.Assert.AreEqual(ownerName, info.GetOwnerName());
			NUnit.Framework.Assert.AreEqual(groupName, info.GetGroupName());
			ownerName = "def";
			groupName = "456";
			mode = new FsPermission((short)0x1c0);
			limit = 151;
			dfs.ModifyCachePool(new CachePoolInfo(poolName).SetOwnerName(ownerName).SetGroupName
				(groupName).SetMode(mode).SetLimit(limit));
			iter = dfs.ListCachePools();
			info = iter.Next().GetInfo();
			NUnit.Framework.Assert.AreEqual(poolName, info.GetPoolName());
			NUnit.Framework.Assert.AreEqual(ownerName, info.GetOwnerName());
			NUnit.Framework.Assert.AreEqual(groupName, info.GetGroupName());
			NUnit.Framework.Assert.AreEqual(mode, info.GetMode());
			NUnit.Framework.Assert.AreEqual(limit, (long)info.GetLimit());
			dfs.RemoveCachePool(poolName);
			iter = dfs.ListCachePools();
			NUnit.Framework.Assert.IsFalse("expected no cache pools after deleting pool", iter
				.HasNext());
			proto.ListCachePools(null);
			try
			{
				proto.RemoveCachePool("pool99");
				NUnit.Framework.Assert.Fail("expected to get an exception when " + "removing a non-existent pool."
					);
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Cannot remove non-existent", ioe);
			}
			try
			{
				proto.RemoveCachePool(poolName);
				NUnit.Framework.Assert.Fail("expected to get an exception when " + "removing a non-existent pool."
					);
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Cannot remove non-existent", ioe);
			}
			iter = dfs.ListCachePools();
			NUnit.Framework.Assert.IsFalse("expected no cache pools after deleting pool", iter
				.HasNext());
		}

		/// <exception cref="System.Exception"/>
		private static void ValidateListAll(RemoteIterator<CacheDirectiveEntry> iter, params 
			long[] ids)
		{
			foreach (long id in ids)
			{
				NUnit.Framework.Assert.IsTrue("Unexpectedly few elements", iter.HasNext());
				NUnit.Framework.Assert.AreEqual("Unexpected directive ID", id, iter.Next().GetInfo
					().GetId());
			}
			NUnit.Framework.Assert.IsFalse("Unexpectedly many list elements", iter.HasNext());
		}

		/// <exception cref="System.Exception"/>
		private static long AddAsUnprivileged(CacheDirectiveInfo directive)
		{
			return unprivilegedUser.DoAs(new _PrivilegedExceptionAction_375(directive));
		}

		private sealed class _PrivilegedExceptionAction_375 : PrivilegedExceptionAction<long
			>
		{
			public _PrivilegedExceptionAction_375(CacheDirectiveInfo directive)
			{
				this.directive = directive;
			}

			/// <exception cref="System.IO.IOException"/>
			public long Run()
			{
				DistributedFileSystem myDfs = (DistributedFileSystem)FileSystem.Get(TestCacheDirectives
					.conf);
				return myDfs.AddCacheDirective(directive);
			}

			private readonly CacheDirectiveInfo directive;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddRemoveDirectives()
		{
			proto.AddCachePool(new CachePoolInfo("pool1").SetMode(new FsPermission((short)0x1ff
				)));
			proto.AddCachePool(new CachePoolInfo("pool2").SetMode(new FsPermission((short)0x1ff
				)));
			proto.AddCachePool(new CachePoolInfo("pool3").SetMode(new FsPermission((short)0x1ff
				)));
			proto.AddCachePool(new CachePoolInfo("pool4").SetMode(new FsPermission((short)0))
				);
			CacheDirectiveInfo alpha = new CacheDirectiveInfo.Builder().SetPath(new Path("/alpha"
				)).SetPool("pool1").Build();
			CacheDirectiveInfo beta = new CacheDirectiveInfo.Builder().SetPath(new Path("/beta"
				)).SetPool("pool2").Build();
			CacheDirectiveInfo delta = new CacheDirectiveInfo.Builder().SetPath(new Path("/delta"
				)).SetPool("pool1").Build();
			long alphaId = AddAsUnprivileged(alpha);
			long alphaId2 = AddAsUnprivileged(alpha);
			NUnit.Framework.Assert.IsFalse("Expected to get unique directives when re-adding an "
				 + "existing CacheDirectiveInfo", alphaId == alphaId2);
			long betaId = AddAsUnprivileged(beta);
			try
			{
				AddAsUnprivileged(new CacheDirectiveInfo.Builder().SetPath(new Path("/unicorn")).
					SetPool("no_such_pool").Build());
				NUnit.Framework.Assert.Fail("expected an error when adding to a non-existent pool."
					);
			}
			catch (InvalidRequestException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Unknown pool", ioe);
			}
			try
			{
				AddAsUnprivileged(new CacheDirectiveInfo.Builder().SetPath(new Path("/blackhole")
					).SetPool("pool4").Build());
				NUnit.Framework.Assert.Fail("expected an error when adding to a pool with " + "mode 0 (no permissions for anyone)."
					);
			}
			catch (AccessControlException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied while accessing pool"
					, e);
			}
			try
			{
				AddAsUnprivileged(new CacheDirectiveInfo.Builder().SetPath(new Path("/illegal:path/"
					)).SetPool("pool1").Build());
				NUnit.Framework.Assert.Fail("expected an error when adding a malformed path " + "to the cache directives."
					);
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("is not a valid DFS filename", e);
			}
			try
			{
				AddAsUnprivileged(new CacheDirectiveInfo.Builder().SetPath(new Path("/emptypoolname"
					)).SetReplication((short)1).SetPool(string.Empty).Build());
				NUnit.Framework.Assert.Fail("expected an error when adding a cache " + "directive with an empty pool name."
					);
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("Invalid empty pool name", e);
			}
			long deltaId = AddAsUnprivileged(delta);
			// We expect the following to succeed, because DistributedFileSystem
			// qualifies the path.
			long relativeId = AddAsUnprivileged(new CacheDirectiveInfo.Builder().SetPath(new 
				Path("relative")).SetPool("pool1").Build());
			RemoteIterator<CacheDirectiveEntry> iter;
			iter = dfs.ListCacheDirectives(null);
			ValidateListAll(iter, alphaId, alphaId2, betaId, deltaId, relativeId);
			iter = dfs.ListCacheDirectives(new CacheDirectiveInfo.Builder().SetPool("pool3").
				Build());
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			iter = dfs.ListCacheDirectives(new CacheDirectiveInfo.Builder().SetPool("pool1").
				Build());
			ValidateListAll(iter, alphaId, alphaId2, deltaId, relativeId);
			iter = dfs.ListCacheDirectives(new CacheDirectiveInfo.Builder().SetPool("pool2").
				Build());
			ValidateListAll(iter, betaId);
			iter = dfs.ListCacheDirectives(new CacheDirectiveInfo.Builder().SetId(alphaId2).Build
				());
			ValidateListAll(iter, alphaId2);
			iter = dfs.ListCacheDirectives(new CacheDirectiveInfo.Builder().SetId(relativeId)
				.Build());
			ValidateListAll(iter, relativeId);
			dfs.RemoveCacheDirective(betaId);
			iter = dfs.ListCacheDirectives(new CacheDirectiveInfo.Builder().SetPool("pool2").
				Build());
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			try
			{
				dfs.RemoveCacheDirective(betaId);
				NUnit.Framework.Assert.Fail("expected an error when removing a non-existent ID");
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("No directive with ID", e);
			}
			try
			{
				proto.RemoveCacheDirective(-42l);
				NUnit.Framework.Assert.Fail("expected an error when removing a negative ID");
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("Invalid negative ID", e);
			}
			try
			{
				proto.RemoveCacheDirective(43l);
				NUnit.Framework.Assert.Fail("expected an error when removing a non-existent ID");
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("No directive with ID", e);
			}
			dfs.RemoveCacheDirective(alphaId);
			dfs.RemoveCacheDirective(alphaId2);
			dfs.RemoveCacheDirective(deltaId);
			dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder().SetId(relativeId).SetReplication
				((short)555).Build());
			iter = dfs.ListCacheDirectives(null);
			NUnit.Framework.Assert.IsTrue(iter.HasNext());
			CacheDirectiveInfo modified = iter.Next().GetInfo();
			NUnit.Framework.Assert.AreEqual(relativeId, modified.GetId());
			NUnit.Framework.Assert.AreEqual((short)555, modified.GetReplication());
			dfs.RemoveCacheDirective(relativeId);
			iter = dfs.ListCacheDirectives(null);
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			// Verify that PBCDs with path "." work correctly
			CacheDirectiveInfo directive = new CacheDirectiveInfo.Builder().SetPath(new Path(
				".")).SetPool("pool1").Build();
			long id = dfs.AddCacheDirective(directive);
			dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder(directive).SetId(id).SetReplication
				((short)2).Build());
			dfs.RemoveCacheDirective(id);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCacheManagerRestart()
		{
			SecondaryNameNode secondary = null;
			try
			{
				// Start a secondary namenode
				conf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, "0.0.0.0:0");
				secondary = new SecondaryNameNode(conf);
				// Create and validate a pool
				string pool = "poolparty";
				string groupName = "partygroup";
				FsPermission mode = new FsPermission((short)0x1ff);
				long limit = 747;
				dfs.AddCachePool(new CachePoolInfo(pool).SetGroupName(groupName).SetMode(mode).SetLimit
					(limit));
				RemoteIterator<CachePoolEntry> pit = dfs.ListCachePools();
				NUnit.Framework.Assert.IsTrue("No cache pools found", pit.HasNext());
				CachePoolInfo info = pit.Next().GetInfo();
				NUnit.Framework.Assert.AreEqual(pool, info.GetPoolName());
				NUnit.Framework.Assert.AreEqual(groupName, info.GetGroupName());
				NUnit.Framework.Assert.AreEqual(mode, info.GetMode());
				NUnit.Framework.Assert.AreEqual(limit, (long)info.GetLimit());
				NUnit.Framework.Assert.IsFalse("Unexpected # of cache pools found", pit.HasNext()
					);
				// Create some cache entries
				int numEntries = 10;
				string entryPrefix = "/party-";
				long prevId = -1;
				DateTime expiry = new DateTime();
				for (int i = 0; i < numEntries; i++)
				{
					prevId = dfs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPath(new Path(
						entryPrefix + i)).SetPool(pool).SetExpiration(CacheDirectiveInfo.Expiration.NewAbsolute
						(expiry.GetTime())).Build());
				}
				RemoteIterator<CacheDirectiveEntry> dit = dfs.ListCacheDirectives(null);
				for (int i_1 = 0; i_1 < numEntries; i_1++)
				{
					NUnit.Framework.Assert.IsTrue("Unexpected # of cache entries: " + i_1, dit.HasNext
						());
					CacheDirectiveInfo cd = dit.Next().GetInfo();
					NUnit.Framework.Assert.AreEqual(i_1 + 1, cd.GetId());
					NUnit.Framework.Assert.AreEqual(entryPrefix + i_1, cd.GetPath().ToUri().GetPath()
						);
					NUnit.Framework.Assert.AreEqual(pool, cd.GetPool());
				}
				NUnit.Framework.Assert.IsFalse("Unexpected # of cache directives found", dit.HasNext
					());
				// Checkpoint once to set some cache pools and directives on 2NN side
				secondary.DoCheckpoint();
				// Add some more CacheManager state
				string imagePool = "imagePool";
				dfs.AddCachePool(new CachePoolInfo(imagePool));
				prevId = dfs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPath(new Path(
					"/image")).SetPool(imagePool).Build());
				// Save a new image to force a fresh fsimage download
				dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				dfs.SaveNamespace();
				dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
				// Checkpoint again forcing a reload of FSN state
				bool fetchImage = secondary.DoCheckpoint();
				NUnit.Framework.Assert.IsTrue("Secondary should have fetched a new fsimage from NameNode"
					, fetchImage);
				// Remove temp pool and directive
				dfs.RemoveCachePool(imagePool);
				// Restart namenode
				cluster.RestartNameNode();
				// Check that state came back up
				pit = dfs.ListCachePools();
				NUnit.Framework.Assert.IsTrue("No cache pools found", pit.HasNext());
				info = pit.Next().GetInfo();
				NUnit.Framework.Assert.AreEqual(pool, info.GetPoolName());
				NUnit.Framework.Assert.AreEqual(pool, info.GetPoolName());
				NUnit.Framework.Assert.AreEqual(groupName, info.GetGroupName());
				NUnit.Framework.Assert.AreEqual(mode, info.GetMode());
				NUnit.Framework.Assert.AreEqual(limit, (long)info.GetLimit());
				NUnit.Framework.Assert.IsFalse("Unexpected # of cache pools found", pit.HasNext()
					);
				dit = dfs.ListCacheDirectives(null);
				for (int i_2 = 0; i_2 < numEntries; i_2++)
				{
					NUnit.Framework.Assert.IsTrue("Unexpected # of cache entries: " + i_2, dit.HasNext
						());
					CacheDirectiveInfo cd = dit.Next().GetInfo();
					NUnit.Framework.Assert.AreEqual(i_2 + 1, cd.GetId());
					NUnit.Framework.Assert.AreEqual(entryPrefix + i_2, cd.GetPath().ToUri().GetPath()
						);
					NUnit.Framework.Assert.AreEqual(pool, cd.GetPool());
					NUnit.Framework.Assert.AreEqual(expiry.GetTime(), cd.GetExpiration().GetMillis());
				}
				NUnit.Framework.Assert.IsFalse("Unexpected # of cache directives found", dit.HasNext
					());
				long nextId = dfs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPath(new 
					Path("/foobar")).SetPool(pool).Build());
				NUnit.Framework.Assert.AreEqual(prevId + 1, nextId);
			}
			finally
			{
				if (secondary != null)
				{
					secondary.Shutdown();
				}
			}
		}

		/// <summary>
		/// Wait for the NameNode to have an expected number of cached blocks
		/// and replicas.
		/// </summary>
		/// <param name="nn">NameNode</param>
		/// <param name="expectedCachedBlocks">if -1, treat as wildcard</param>
		/// <param name="expectedCachedReplicas">if -1, treat as wildcard</param>
		/// <exception cref="System.Exception"/>
		private static void WaitForCachedBlocks(NameNode nn, int expectedCachedBlocks, int
			 expectedCachedReplicas, string logString)
		{
			FSNamesystem namesystem = nn.GetNamesystem();
			CacheManager cacheManager = namesystem.GetCacheManager();
			Log.Info("Waiting for " + expectedCachedBlocks + " blocks with " + expectedCachedReplicas
				 + " replicas.");
			GenericTestUtils.WaitFor(new _Supplier_667(namesystem, cacheManager, logString, expectedCachedBlocks
				, expectedCachedReplicas), 500, 60000);
		}

		private sealed class _Supplier_667 : Supplier<bool>
		{
			public _Supplier_667(FSNamesystem namesystem, CacheManager cacheManager, string logString
				, int expectedCachedBlocks, int expectedCachedReplicas)
			{
				this.namesystem = namesystem;
				this.cacheManager = cacheManager;
				this.logString = logString;
				this.expectedCachedBlocks = expectedCachedBlocks;
				this.expectedCachedReplicas = expectedCachedReplicas;
			}

			public bool Get()
			{
				int numCachedBlocks = 0;
				int numCachedReplicas = 0;
				namesystem.ReadLock();
				try
				{
					GSet<CachedBlock, CachedBlock> cachedBlocks = cacheManager.GetCachedBlocks();
					if (cachedBlocks != null)
					{
						for (IEnumerator<CachedBlock> iter = cachedBlocks.GetEnumerator(); iter.HasNext()
							; )
						{
							CachedBlock cachedBlock = iter.Next();
							numCachedBlocks++;
							numCachedReplicas += cachedBlock.GetDatanodes(DatanodeDescriptor.CachedBlocksList.Type
								.Cached).Count;
						}
					}
				}
				finally
				{
					namesystem.ReadUnlock();
				}
				TestCacheDirectives.Log.Info(logString + " cached blocks: have " + numCachedBlocks
					 + " / " + expectedCachedBlocks + ".  " + "cached replicas: have " + numCachedReplicas
					 + " / " + expectedCachedReplicas);
				if (expectedCachedBlocks == -1 || numCachedBlocks == expectedCachedBlocks)
				{
					if (expectedCachedReplicas == -1 || numCachedReplicas == expectedCachedReplicas)
					{
						return true;
					}
				}
				return false;
			}

			private readonly FSNamesystem namesystem;

			private readonly CacheManager cacheManager;

			private readonly string logString;

			private readonly int expectedCachedBlocks;

			private readonly int expectedCachedReplicas;
		}

		/// <exception cref="System.Exception"/>
		private static void WaitForCacheDirectiveStats(DistributedFileSystem dfs, long targetBytesNeeded
			, long targetBytesCached, long targetFilesNeeded, long targetFilesCached, CacheDirectiveInfo
			 filter, string infoString)
		{
			Log.Info("Polling listCacheDirectives " + ((filter == null) ? "ALL" : filter.ToString
				()) + " for " + targetBytesNeeded + " targetBytesNeeded, " + targetBytesCached +
				 " targetBytesCached, " + targetFilesNeeded + " targetFilesNeeded, " + targetFilesCached
				 + " targetFilesCached");
			GenericTestUtils.WaitFor(new _Supplier_715(dfs, filter, targetBytesNeeded, targetBytesCached
				, targetFilesNeeded, targetFilesCached, infoString), 500, 60000);
		}

		private sealed class _Supplier_715 : Supplier<bool>
		{
			public _Supplier_715(DistributedFileSystem dfs, CacheDirectiveInfo filter, long targetBytesNeeded
				, long targetBytesCached, long targetFilesNeeded, long targetFilesCached, string
				 infoString)
			{
				this.dfs = dfs;
				this.filter = filter;
				this.targetBytesNeeded = targetBytesNeeded;
				this.targetBytesCached = targetBytesCached;
				this.targetFilesNeeded = targetFilesNeeded;
				this.targetFilesCached = targetFilesCached;
				this.infoString = infoString;
			}

			public bool Get()
			{
				RemoteIterator<CacheDirectiveEntry> iter = null;
				CacheDirectiveEntry entry = null;
				try
				{
					iter = dfs.ListCacheDirectives(filter);
					entry = iter.Next();
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.Fail("got IOException while calling " + "listCacheDirectives: "
						 + e.Message);
				}
				NUnit.Framework.Assert.IsNotNull(entry);
				CacheDirectiveStats stats = entry.GetStats();
				if ((targetBytesNeeded == stats.GetBytesNeeded()) && (targetBytesCached == stats.
					GetBytesCached()) && (targetFilesNeeded == stats.GetFilesNeeded()) && (targetFilesCached
					 == stats.GetFilesCached()))
				{
					return true;
				}
				else
				{
					TestCacheDirectives.Log.Info(infoString + ": " + "filesNeeded: " + stats.GetFilesNeeded
						() + "/" + targetFilesNeeded + ", filesCached: " + stats.GetFilesCached() + "/" 
						+ targetFilesCached + ", bytesNeeded: " + stats.GetBytesNeeded() + "/" + targetBytesNeeded
						 + ", bytesCached: " + stats.GetBytesCached() + "/" + targetBytesCached);
					return false;
				}
			}

			private readonly DistributedFileSystem dfs;

			private readonly CacheDirectiveInfo filter;

			private readonly long targetBytesNeeded;

			private readonly long targetBytesCached;

			private readonly long targetFilesNeeded;

			private readonly long targetFilesCached;

			private readonly string infoString;
		}

		/// <exception cref="System.Exception"/>
		private static void WaitForCachePoolStats(DistributedFileSystem dfs, long targetBytesNeeded
			, long targetBytesCached, long targetFilesNeeded, long targetFilesCached, CachePoolInfo
			 pool, string infoString)
		{
			Log.Info("Polling listCachePools " + pool.ToString() + " for " + targetBytesNeeded
				 + " targetBytesNeeded, " + targetBytesCached + " targetBytesCached, " + targetFilesNeeded
				 + " targetFilesNeeded, " + targetFilesCached + " targetFilesCached");
			GenericTestUtils.WaitFor(new _Supplier_760(dfs, pool, targetBytesNeeded, targetBytesCached
				, targetFilesNeeded, targetFilesCached, infoString), 500, 60000);
		}

		private sealed class _Supplier_760 : Supplier<bool>
		{
			public _Supplier_760(DistributedFileSystem dfs, CachePoolInfo pool, long targetBytesNeeded
				, long targetBytesCached, long targetFilesNeeded, long targetFilesCached, string
				 infoString)
			{
				this.dfs = dfs;
				this.pool = pool;
				this.targetBytesNeeded = targetBytesNeeded;
				this.targetBytesCached = targetBytesCached;
				this.targetFilesNeeded = targetFilesNeeded;
				this.targetFilesCached = targetFilesCached;
				this.infoString = infoString;
			}

			public bool Get()
			{
				RemoteIterator<CachePoolEntry> iter = null;
				try
				{
					iter = dfs.ListCachePools();
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.Fail("got IOException while calling " + "listCachePools: "
						 + e.Message);
				}
				while (true)
				{
					CachePoolEntry entry = null;
					try
					{
						if (!iter.HasNext())
						{
							break;
						}
						entry = iter.Next();
					}
					catch (IOException e)
					{
						NUnit.Framework.Assert.Fail("got IOException while iterating through " + "listCachePools: "
							 + e.Message);
					}
					if (entry == null)
					{
						break;
					}
					if (!entry.GetInfo().GetPoolName().Equals(pool.GetPoolName()))
					{
						continue;
					}
					CachePoolStats stats = entry.GetStats();
					if ((targetBytesNeeded == stats.GetBytesNeeded()) && (targetBytesCached == stats.
						GetBytesCached()) && (targetFilesNeeded == stats.GetFilesNeeded()) && (targetFilesCached
						 == stats.GetFilesCached()))
					{
						return true;
					}
					else
					{
						TestCacheDirectives.Log.Info(infoString + ": " + "filesNeeded: " + stats.GetFilesNeeded
							() + "/" + targetFilesNeeded + ", filesCached: " + stats.GetFilesCached() + "/" 
							+ targetFilesCached + ", bytesNeeded: " + stats.GetBytesNeeded() + "/" + targetBytesNeeded
							 + ", bytesCached: " + stats.GetBytesCached() + "/" + targetBytesCached);
						return false;
					}
				}
				return false;
			}

			private readonly DistributedFileSystem dfs;

			private readonly CachePoolInfo pool;

			private readonly long targetBytesNeeded;

			private readonly long targetBytesCached;

			private readonly long targetFilesNeeded;

			private readonly long targetFilesCached;

			private readonly string infoString;
		}

		/// <exception cref="System.Exception"/>
		private static void CheckNumCachedReplicas(DistributedFileSystem dfs, IList<Path>
			 paths, int expectedBlocks, int expectedReplicas)
		{
			int numCachedBlocks = 0;
			int numCachedReplicas = 0;
			foreach (Path p in paths)
			{
				FileStatus f = dfs.GetFileStatus(p);
				long len = f.GetLen();
				long blockSize = f.GetBlockSize();
				// round it up to full blocks
				long numBlocks = (len + blockSize - 1) / blockSize;
				BlockLocation[] locs = dfs.GetFileBlockLocations(p, 0, len);
				NUnit.Framework.Assert.AreEqual("Unexpected number of block locations for path " 
					+ p, numBlocks, locs.Length);
				foreach (BlockLocation l in locs)
				{
					if (l.GetCachedHosts().Length > 0)
					{
						numCachedBlocks++;
					}
					numCachedReplicas += l.GetCachedHosts().Length;
				}
			}
			Log.Info("Found " + numCachedBlocks + " of " + expectedBlocks + " blocks");
			Log.Info("Found " + numCachedReplicas + " of " + expectedReplicas + " replicas");
			NUnit.Framework.Assert.AreEqual("Unexpected number of cached blocks", expectedBlocks
				, numCachedBlocks);
			NUnit.Framework.Assert.AreEqual("Unexpected number of cached replicas", expectedReplicas
				, numCachedReplicas);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWaitForCachedReplicas()
		{
			FileSystemTestHelper helper = new FileSystemTestHelper();
			GenericTestUtils.WaitFor(new _Supplier_845(), 500, 60000);
			// Send a cache report referring to a bogus block.  It is important that
			// the NameNode be robust against this.
			NamenodeProtocols nnRpc = namenode.GetRpcServer();
			DataNode dn0 = cluster.GetDataNodes()[0];
			string bpid = cluster.GetNamesystem().GetBlockPoolId();
			List<long> bogusBlockIds = new List<long>();
			bogusBlockIds.AddItem(999999L);
			nnRpc.CacheReport(dn0.GetDNRegistrationForBP(bpid), bpid, bogusBlockIds);
			Path rootDir = helper.GetDefaultWorkingDirectory(dfs);
			// Create the pool
			string pool = "friendlyPool";
			nnRpc.AddCachePool(new CachePoolInfo("friendlyPool"));
			// Create some test files
			int numFiles = 2;
			int numBlocksPerFile = 2;
			IList<string> paths = new AList<string>(numFiles);
			for (int i = 0; i < numFiles; i++)
			{
				Path p = new Path(rootDir, "testCachePaths-" + i);
				FileSystemTestHelper.CreateFile(dfs, p, numBlocksPerFile, (int)BlockSize);
				paths.AddItem(p.ToUri().GetPath());
			}
			// Check the initial statistics at the namenode
			WaitForCachedBlocks(namenode, 0, 0, "testWaitForCachedReplicas:0");
			// Cache and check each path in sequence
			int expected = 0;
			for (int i_1 = 0; i_1 < numFiles; i_1++)
			{
				CacheDirectiveInfo directive = new CacheDirectiveInfo.Builder().SetPath(new Path(
					paths[i_1])).SetPool(pool).Build();
				nnRpc.AddCacheDirective(directive, EnumSet.NoneOf<CacheFlag>());
				expected += numBlocksPerFile;
				WaitForCachedBlocks(namenode, expected, expected, "testWaitForCachedReplicas:1");
			}
			// Check that the datanodes have the right cache values
			DatanodeInfo[] live = dfs.GetDataNodeStats(HdfsConstants.DatanodeReportType.Live);
			NUnit.Framework.Assert.AreEqual("Unexpected number of live nodes", NumDatanodes, 
				live.Length);
			long totalUsed = 0;
			foreach (DatanodeInfo dn in live)
			{
				long cacheCapacity = dn.GetCacheCapacity();
				long cacheUsed = dn.GetCacheUsed();
				long cacheRemaining = dn.GetCacheRemaining();
				NUnit.Framework.Assert.AreEqual("Unexpected cache capacity", CacheCapacity, cacheCapacity
					);
				NUnit.Framework.Assert.AreEqual("Capacity not equal to used + remaining", cacheCapacity
					, cacheUsed + cacheRemaining);
				NUnit.Framework.Assert.AreEqual("Remaining not equal to capacity - used", cacheCapacity
					 - cacheUsed, cacheRemaining);
				totalUsed += cacheUsed;
			}
			NUnit.Framework.Assert.AreEqual(expected * BlockSize, totalUsed);
			// Uncache and check each path in sequence
			RemoteIterator<CacheDirectiveEntry> entries = new CacheDirectiveIterator(nnRpc, null
				, Sampler.Never);
			for (int i_2 = 0; i_2 < numFiles; i_2++)
			{
				CacheDirectiveEntry entry = entries.Next();
				nnRpc.RemoveCacheDirective(entry.GetInfo().GetId());
				expected -= numBlocksPerFile;
				WaitForCachedBlocks(namenode, expected, expected, "testWaitForCachedReplicas:2");
			}
		}

		private sealed class _Supplier_845 : Supplier<bool>
		{
			public _Supplier_845()
			{
			}

			public bool Get()
			{
				return ((TestCacheDirectives.namenode.GetNamesystem().GetCacheCapacity() == (TestCacheDirectives
					.NumDatanodes * TestCacheDirectives.CacheCapacity)) && (TestCacheDirectives.namenode
					.GetNamesystem().GetCacheUsed() == 0));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWaitForCachedReplicasInDirectory()
		{
			// Create the pool
			string pool = "friendlyPool";
			CachePoolInfo poolInfo = new CachePoolInfo(pool);
			dfs.AddCachePool(poolInfo);
			// Create some test files
			IList<Path> paths = new List<Path>();
			paths.AddItem(new Path("/foo/bar"));
			paths.AddItem(new Path("/foo/baz"));
			paths.AddItem(new Path("/foo2/bar2"));
			paths.AddItem(new Path("/foo2/baz2"));
			dfs.Mkdir(new Path("/foo"), FsPermission.GetDirDefault());
			dfs.Mkdir(new Path("/foo2"), FsPermission.GetDirDefault());
			int numBlocksPerFile = 2;
			foreach (Path path in paths)
			{
				FileSystemTestHelper.CreateFile(dfs, path, numBlocksPerFile, (int)BlockSize, (short
					)3, false);
			}
			WaitForCachedBlocks(namenode, 0, 0, "testWaitForCachedReplicasInDirectory:0");
			// cache entire directory
			long id = dfs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPath(new Path
				("/foo")).SetReplication((short)2).SetPool(pool).Build());
			WaitForCachedBlocks(namenode, 4, 8, "testWaitForCachedReplicasInDirectory:1:blocks"
				);
			// Verify that listDirectives gives the stats we want.
			WaitForCacheDirectiveStats(dfs, 4 * numBlocksPerFile * BlockSize, 4 * numBlocksPerFile
				 * BlockSize, 2, 2, new CacheDirectiveInfo.Builder().SetPath(new Path("/foo")).Build
				(), "testWaitForCachedReplicasInDirectory:1:directive");
			WaitForCachePoolStats(dfs, 4 * numBlocksPerFile * BlockSize, 4 * numBlocksPerFile
				 * BlockSize, 2, 2, poolInfo, "testWaitForCachedReplicasInDirectory:1:pool");
			long id2 = dfs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPath(new Path
				("/foo/bar")).SetReplication((short)4).SetPool(pool).Build());
			// wait for an additional 2 cached replicas to come up
			WaitForCachedBlocks(namenode, 4, 10, "testWaitForCachedReplicasInDirectory:2:blocks"
				);
			// the directory directive's stats are unchanged
			WaitForCacheDirectiveStats(dfs, 4 * numBlocksPerFile * BlockSize, 4 * numBlocksPerFile
				 * BlockSize, 2, 2, new CacheDirectiveInfo.Builder().SetPath(new Path("/foo")).Build
				(), "testWaitForCachedReplicasInDirectory:2:directive-1");
			// verify /foo/bar's stats
			WaitForCacheDirectiveStats(dfs, 4 * numBlocksPerFile * BlockSize, 3 * numBlocksPerFile
				 * BlockSize, 1, 0, new CacheDirectiveInfo.Builder().SetPath(new Path("/foo/bar"
				)).Build(), "testWaitForCachedReplicasInDirectory:2:directive-2");
			// only 3 because the file only has 3 replicas, not 4 as requested.
			// only 0 because the file can't be fully cached
			WaitForCachePoolStats(dfs, (4 + 4) * numBlocksPerFile * BlockSize, (4 + 3) * numBlocksPerFile
				 * BlockSize, 3, 2, poolInfo, "testWaitForCachedReplicasInDirectory:2:pool");
			// remove and watch numCached go to 0
			dfs.RemoveCacheDirective(id);
			dfs.RemoveCacheDirective(id2);
			WaitForCachedBlocks(namenode, 0, 0, "testWaitForCachedReplicasInDirectory:3:blocks"
				);
			WaitForCachePoolStats(dfs, 0, 0, 0, 0, poolInfo, "testWaitForCachedReplicasInDirectory:3:pool"
				);
		}

		/// <summary>
		/// Tests stepping the cache replication factor up and down, checking the
		/// number of cached replicas and blocks as well as the advertised locations.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestReplicationFactor()
		{
			// Create the pool
			string pool = "friendlyPool";
			dfs.AddCachePool(new CachePoolInfo(pool));
			// Create some test files
			IList<Path> paths = new List<Path>();
			paths.AddItem(new Path("/foo/bar"));
			paths.AddItem(new Path("/foo/baz"));
			paths.AddItem(new Path("/foo2/bar2"));
			paths.AddItem(new Path("/foo2/baz2"));
			dfs.Mkdir(new Path("/foo"), FsPermission.GetDirDefault());
			dfs.Mkdir(new Path("/foo2"), FsPermission.GetDirDefault());
			int numBlocksPerFile = 2;
			foreach (Path path in paths)
			{
				FileSystemTestHelper.CreateFile(dfs, path, numBlocksPerFile, (int)BlockSize, (short
					)3, false);
			}
			WaitForCachedBlocks(namenode, 0, 0, "testReplicationFactor:0");
			CheckNumCachedReplicas(dfs, paths, 0, 0);
			// cache directory
			long id = dfs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPath(new Path
				("/foo")).SetReplication((short)1).SetPool(pool).Build());
			WaitForCachedBlocks(namenode, 4, 4, "testReplicationFactor:1");
			CheckNumCachedReplicas(dfs, paths, 4, 4);
			// step up the replication factor
			for (int i = 2; i <= 3; i++)
			{
				dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder().SetId(id).SetReplication
					((short)i).Build());
				WaitForCachedBlocks(namenode, 4, 4 * i, "testReplicationFactor:2");
				CheckNumCachedReplicas(dfs, paths, 4, 4 * i);
			}
			// step it down
			for (int i_1 = 2; i_1 >= 1; i_1--)
			{
				dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder().SetId(id).SetReplication
					((short)i_1).Build());
				WaitForCachedBlocks(namenode, 4, 4 * i_1, "testReplicationFactor:3");
				CheckNumCachedReplicas(dfs, paths, 4, 4 * i_1);
			}
			// remove and watch numCached go to 0
			dfs.RemoveCacheDirective(id);
			WaitForCachedBlocks(namenode, 0, 0, "testReplicationFactor:4");
			CheckNumCachedReplicas(dfs, paths, 0, 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestListCachePoolPermissions()
		{
			UserGroupInformation myUser = UserGroupInformation.CreateRemoteUser("myuser");
			DistributedFileSystem myDfs = (DistributedFileSystem)DFSTestUtil.GetFileSystemAs(
				myUser, conf);
			string poolName = "poolparty";
			dfs.AddCachePool(new CachePoolInfo(poolName).SetMode(new FsPermission((short)0x1c0
				)));
			// Should only see partial info
			RemoteIterator<CachePoolEntry> it = myDfs.ListCachePools();
			CachePoolInfo info = it.Next().GetInfo();
			NUnit.Framework.Assert.IsFalse(it.HasNext());
			NUnit.Framework.Assert.AreEqual("Expected pool name", poolName, info.GetPoolName(
				));
			NUnit.Framework.Assert.IsNull("Unexpected owner name", info.GetOwnerName());
			NUnit.Framework.Assert.IsNull("Unexpected group name", info.GetGroupName());
			NUnit.Framework.Assert.IsNull("Unexpected mode", info.GetMode());
			NUnit.Framework.Assert.IsNull("Unexpected limit", info.GetLimit());
			// Modify the pool so myuser is now the owner
			long limit = 99;
			dfs.ModifyCachePool(new CachePoolInfo(poolName).SetOwnerName(myUser.GetShortUserName
				()).SetLimit(limit));
			// Should see full info
			it = myDfs.ListCachePools();
			info = it.Next().GetInfo();
			NUnit.Framework.Assert.IsFalse(it.HasNext());
			NUnit.Framework.Assert.AreEqual("Expected pool name", poolName, info.GetPoolName(
				));
			NUnit.Framework.Assert.AreEqual("Mismatched owner name", myUser.GetShortUserName(
				), info.GetOwnerName());
			NUnit.Framework.Assert.IsNotNull("Expected group name", info.GetGroupName());
			NUnit.Framework.Assert.AreEqual("Mismatched mode", (short)0x1c0, info.GetMode().ToShort
				());
			NUnit.Framework.Assert.AreEqual("Mismatched limit", limit, (long)info.GetLimit());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestExpiry()
		{
			string pool = "pool1";
			dfs.AddCachePool(new CachePoolInfo(pool));
			Path p = new Path("/mypath");
			DFSTestUtil.CreateFile(dfs, p, BlockSize * 2, (short)2, unchecked((int)(0x999)));
			// Expire after test timeout
			DateTime start = new DateTime();
			DateTime expiry = DateUtils.AddSeconds(start, 120);
			long id = dfs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPath(p).SetPool
				(pool).SetExpiration(CacheDirectiveInfo.Expiration.NewAbsolute(expiry)).SetReplication
				((short)2).Build());
			WaitForCachedBlocks(cluster.GetNameNode(), 2, 4, "testExpiry:1");
			// Change it to expire sooner
			dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder().SetId(id).SetExpiration
				(CacheDirectiveInfo.Expiration.NewRelative(0)).Build());
			WaitForCachedBlocks(cluster.GetNameNode(), 0, 0, "testExpiry:2");
			RemoteIterator<CacheDirectiveEntry> it = dfs.ListCacheDirectives(null);
			CacheDirectiveEntry ent = it.Next();
			NUnit.Framework.Assert.IsFalse(it.HasNext());
			DateTime entryExpiry = Sharpen.Extensions.CreateDate(ent.GetInfo().GetExpiration(
				).GetMillis());
			NUnit.Framework.Assert.IsTrue("Directive should have expired", entryExpiry.Before
				(new DateTime()));
			// Change it back to expire later
			dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder().SetId(id).SetExpiration
				(CacheDirectiveInfo.Expiration.NewRelative(120000)).Build());
			WaitForCachedBlocks(cluster.GetNameNode(), 2, 4, "testExpiry:3");
			it = dfs.ListCacheDirectives(null);
			ent = it.Next();
			NUnit.Framework.Assert.IsFalse(it.HasNext());
			entryExpiry = Sharpen.Extensions.CreateDate(ent.GetInfo().GetExpiration().GetMillis
				());
			NUnit.Framework.Assert.IsTrue("Directive should not have expired", entryExpiry.After
				(new DateTime()));
			// Verify that setting a negative TTL throws an error
			try
			{
				dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder().SetId(id).SetExpiration
					(CacheDirectiveInfo.Expiration.NewRelative(-1)).Build());
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot set a negative expiration", e);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLimit()
		{
			try
			{
				dfs.AddCachePool(new CachePoolInfo("poolofnegativity").SetLimit(-99l));
				NUnit.Framework.Assert.Fail("Should not be able to set a negative limit");
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("negative", e);
			}
			string destiny = "poolofdestiny";
			Path path1 = new Path("/destiny");
			DFSTestUtil.CreateFile(dfs, path1, 2 * BlockSize, (short)1, unchecked((int)(0x9494
				)));
			// Start off with a limit that is too small
			CachePoolInfo poolInfo = new CachePoolInfo(destiny).SetLimit(2 * BlockSize - 1);
			dfs.AddCachePool(poolInfo);
			CacheDirectiveInfo info1 = new CacheDirectiveInfo.Builder().SetPool(destiny).SetPath
				(path1).Build();
			try
			{
				dfs.AddCacheDirective(info1);
				NUnit.Framework.Assert.Fail("Should not be able to cache when there is no more limit"
					);
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("remaining capacity", e);
			}
			// Raise the limit up to fit and it should work this time
			poolInfo.SetLimit(2 * BlockSize);
			dfs.ModifyCachePool(poolInfo);
			long id1 = dfs.AddCacheDirective(info1);
			WaitForCachePoolStats(dfs, 2 * BlockSize, 2 * BlockSize, 1, 1, poolInfo, "testLimit:1"
				);
			// Adding another file, it shouldn't be cached
			Path path2 = new Path("/failure");
			DFSTestUtil.CreateFile(dfs, path2, BlockSize, (short)1, unchecked((int)(0x9495)));
			try
			{
				dfs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPool(destiny).SetPath(path2
					).Build(), EnumSet.NoneOf<CacheFlag>());
				NUnit.Framework.Assert.Fail("Should not be able to add another cached file");
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("remaining capacity", e);
			}
			// Bring the limit down, the first file should get uncached
			poolInfo.SetLimit(BlockSize);
			dfs.ModifyCachePool(poolInfo);
			WaitForCachePoolStats(dfs, 2 * BlockSize, 0, 1, 0, poolInfo, "testLimit:2");
			RemoteIterator<CachePoolEntry> it = dfs.ListCachePools();
			NUnit.Framework.Assert.IsTrue("Expected a cache pool", it.HasNext());
			CachePoolStats stats = it.Next().GetStats();
			NUnit.Framework.Assert.AreEqual("Overlimit bytes should be difference of needed and limit"
				, BlockSize, stats.GetBytesOverlimit());
			// Moving a directive to a pool without enough limit should fail
			CachePoolInfo inadequate = new CachePoolInfo("poolofinadequacy").SetLimit(BlockSize
				);
			dfs.AddCachePool(inadequate);
			try
			{
				dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder(info1).SetId(id1).SetPool
					(inadequate.GetPoolName()).Build(), EnumSet.NoneOf<CacheFlag>());
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("remaining capacity", e);
			}
			// Succeeds when force=true
			dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder(info1).SetId(id1).SetPool
				(inadequate.GetPoolName()).Build(), EnumSet.Of(CacheFlag.Force));
			// Also can add with force=true
			dfs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPool(inadequate.GetPoolName
				()).SetPath(path1).Build(), EnumSet.Of(CacheFlag.Force));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMaxRelativeExpiry()
		{
			// Test that negative and really big max expirations can't be set during add
			try
			{
				dfs.AddCachePool(new CachePoolInfo("failpool").SetMaxRelativeExpiryMs(-1l));
				NUnit.Framework.Assert.Fail("Added a pool with a negative max expiry.");
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("negative", e);
			}
			try
			{
				dfs.AddCachePool(new CachePoolInfo("failpool").SetMaxRelativeExpiryMs(long.MaxValue
					 - 1));
				NUnit.Framework.Assert.Fail("Added a pool with too big of a max expiry.");
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("too big", e);
			}
			// Test that setting a max relative expiry on a pool works
			CachePoolInfo coolPool = new CachePoolInfo("coolPool");
			long poolExpiration = 1000 * 60 * 10l;
			dfs.AddCachePool(coolPool.SetMaxRelativeExpiryMs(poolExpiration));
			RemoteIterator<CachePoolEntry> poolIt = dfs.ListCachePools();
			CachePoolInfo listPool = poolIt.Next().GetInfo();
			NUnit.Framework.Assert.IsFalse("Should only be one pool", poolIt.HasNext());
			NUnit.Framework.Assert.AreEqual("Expected max relative expiry to match set value"
				, poolExpiration, listPool.GetMaxRelativeExpiryMs());
			// Test that negative and really big max expirations can't be modified
			try
			{
				dfs.AddCachePool(coolPool.SetMaxRelativeExpiryMs(-1l));
				NUnit.Framework.Assert.Fail("Added a pool with a negative max expiry.");
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("negative", e);
			}
			try
			{
				dfs.ModifyCachePool(coolPool.SetMaxRelativeExpiryMs(CachePoolInfo.RelativeExpiryNever
					 + 1));
				NUnit.Framework.Assert.Fail("Added a pool with too big of a max expiry.");
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("too big", e);
			}
			// Test that adding a directives without an expiration uses the pool's max
			CacheDirectiveInfo defaultExpiry = new CacheDirectiveInfo.Builder().SetPath(new Path
				("/blah")).SetPool(coolPool.GetPoolName()).Build();
			dfs.AddCacheDirective(defaultExpiry);
			RemoteIterator<CacheDirectiveEntry> dirIt = dfs.ListCacheDirectives(defaultExpiry
				);
			CacheDirectiveInfo listInfo = dirIt.Next().GetInfo();
			NUnit.Framework.Assert.IsFalse("Should only have one entry in listing", dirIt.HasNext
				());
			long listExpiration = listInfo.GetExpiration().GetAbsoluteMillis() - new DateTime
				().GetTime();
			NUnit.Framework.Assert.IsTrue("Directive expiry should be approximately the pool's max expiry"
				, Math.Abs(listExpiration - poolExpiration) < 10 * 1000);
			// Test that the max is enforced on add for relative and absolute
			CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder().SetPath(new 
				Path("/lolcat")).SetPool(coolPool.GetPoolName());
			try
			{
				dfs.AddCacheDirective(builder.SetExpiration(CacheDirectiveInfo.Expiration.NewRelative
					(poolExpiration + 1)).Build());
				NUnit.Framework.Assert.Fail("Added a directive that exceeds pool's max relative expiration"
					);
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("exceeds the max relative expiration", e
					);
			}
			try
			{
				dfs.AddCacheDirective(builder.SetExpiration(CacheDirectiveInfo.Expiration.NewAbsolute
					(new DateTime().GetTime() + poolExpiration + (10 * 1000))).Build());
				NUnit.Framework.Assert.Fail("Added a directive that exceeds pool's max relative expiration"
					);
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("exceeds the max relative expiration", e
					);
			}
			// Test that max is enforced on modify for relative and absolute Expirations
			try
			{
				dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder(defaultExpiry).SetId(listInfo
					.GetId()).SetExpiration(CacheDirectiveInfo.Expiration.NewRelative(poolExpiration
					 + 1)).Build());
				NUnit.Framework.Assert.Fail("Modified a directive to exceed pool's max relative expiration"
					);
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("exceeds the max relative expiration", e
					);
			}
			try
			{
				dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder(defaultExpiry).SetId(listInfo
					.GetId()).SetExpiration(CacheDirectiveInfo.Expiration.NewAbsolute(new DateTime()
					.GetTime() + poolExpiration + (10 * 1000))).Build());
				NUnit.Framework.Assert.Fail("Modified a directive to exceed pool's max relative expiration"
					);
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("exceeds the max relative expiration", e
					);
			}
			// Test some giant limit values with add
			try
			{
				dfs.AddCacheDirective(builder.SetExpiration(CacheDirectiveInfo.Expiration.NewRelative
					(long.MaxValue)).Build());
				NUnit.Framework.Assert.Fail("Added a directive with a gigantic max value");
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("is too far in the future", e);
			}
			try
			{
				dfs.AddCacheDirective(builder.SetExpiration(CacheDirectiveInfo.Expiration.NewAbsolute
					(long.MaxValue)).Build());
				NUnit.Framework.Assert.Fail("Added a directive with a gigantic max value");
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("is too far in the future", e);
			}
			// Test some giant limit values with modify
			try
			{
				dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder(defaultExpiry).SetId(listInfo
					.GetId()).SetExpiration(CacheDirectiveInfo.Expiration.Never).Build());
				NUnit.Framework.Assert.Fail("Modified a directive to exceed pool's max relative expiration"
					);
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("exceeds the max relative expiration", e
					);
			}
			try
			{
				dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder(defaultExpiry).SetId(listInfo
					.GetId()).SetExpiration(CacheDirectiveInfo.Expiration.NewAbsolute(long.MaxValue)
					).Build());
				NUnit.Framework.Assert.Fail("Modified a directive to exceed pool's max relative expiration"
					);
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("is too far in the future", e);
			}
			// Test that the max is enforced on modify correctly when changing pools
			CachePoolInfo destPool = new CachePoolInfo("destPool");
			dfs.AddCachePool(destPool.SetMaxRelativeExpiryMs(poolExpiration / 2));
			try
			{
				dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder(defaultExpiry).SetId(listInfo
					.GetId()).SetPool(destPool.GetPoolName()).Build());
				NUnit.Framework.Assert.Fail("Modified a directive to a pool with a lower max expiration"
					);
			}
			catch (InvalidRequestException e)
			{
				GenericTestUtils.AssertExceptionContains("exceeds the max relative expiration", e
					);
			}
			dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder(defaultExpiry).SetId(listInfo
				.GetId()).SetPool(destPool.GetPoolName()).SetExpiration(CacheDirectiveInfo.Expiration
				.NewRelative(poolExpiration / 2)).Build());
			dirIt = dfs.ListCacheDirectives(new CacheDirectiveInfo.Builder().SetPool(destPool
				.GetPoolName()).Build());
			listInfo = dirIt.Next().GetInfo();
			listExpiration = listInfo.GetExpiration().GetAbsoluteMillis() - new DateTime().GetTime
				();
			NUnit.Framework.Assert.IsTrue("Unexpected relative expiry " + listExpiration + " expected approximately "
				 + poolExpiration / 2, Math.Abs(poolExpiration / 2 - listExpiration) < 10 * 1000
				);
			// Test that cache pool and directive expiry can be modified back to never
			dfs.ModifyCachePool(destPool.SetMaxRelativeExpiryMs(CachePoolInfo.RelativeExpiryNever
				));
			poolIt = dfs.ListCachePools();
			listPool = poolIt.Next().GetInfo();
			while (!listPool.GetPoolName().Equals(destPool.GetPoolName()))
			{
				listPool = poolIt.Next().GetInfo();
			}
			NUnit.Framework.Assert.AreEqual("Expected max relative expiry to match set value"
				, CachePoolInfo.RelativeExpiryNever, listPool.GetMaxRelativeExpiryMs());
			dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder().SetId(listInfo.GetId())
				.SetExpiration(CacheDirectiveInfo.Expiration.NewRelative(CachePoolInfo.RelativeExpiryNever
				)).Build());
			// Test modifying close to the limit
			dfs.ModifyCacheDirective(new CacheDirectiveInfo.Builder().SetId(listInfo.GetId())
				.SetExpiration(CacheDirectiveInfo.Expiration.NewRelative(CachePoolInfo.RelativeExpiryNever
				 - 1)).Build());
		}

		/// <summary>Check that the NameNode is not attempting to cache anything.</summary>
		/// <exception cref="System.Exception"/>
		private void CheckPendingCachedEmpty(MiniDFSCluster cluster)
		{
			cluster.GetNamesystem().ReadLock();
			try
			{
				DatanodeManager datanodeManager = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
					();
				foreach (DataNode dn in cluster.GetDataNodes())
				{
					DatanodeDescriptor descriptor = datanodeManager.GetDatanode(dn.GetDatanodeId());
					NUnit.Framework.Assert.IsTrue("Pending cached list of " + descriptor + " is not empty, "
						 + Arrays.ToString(Sharpen.Collections.ToArray(descriptor.GetPendingCached())), 
						descriptor.GetPendingCached().IsEmpty());
				}
			}
			finally
			{
				cluster.GetNamesystem().ReadUnlock();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestExceedsCapacity()
		{
			// Create a giant file
			Path fileName = new Path("/exceeds");
			long fileLen = CacheCapacity * (NumDatanodes * 2);
			int numCachedReplicas = (int)((CacheCapacity * NumDatanodes) / BlockSize);
			DFSTestUtil.CreateFile(dfs, fileName, fileLen, (short)NumDatanodes, unchecked((int
				)(0xFADED)));
			dfs.AddCachePool(new CachePoolInfo("pool"));
			dfs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPool("pool").SetPath(fileName
				).SetReplication((short)1).Build());
			WaitForCachedBlocks(namenode, -1, numCachedReplicas, "testExceeds:1");
			CheckPendingCachedEmpty(cluster);
			Sharpen.Thread.Sleep(1000);
			CheckPendingCachedEmpty(cluster);
			// Try creating a file with giant-sized blocks that exceed cache capacity
			dfs.Delete(fileName, false);
			DFSTestUtil.CreateFile(dfs, fileName, 4096, fileLen, CacheCapacity * 2, (short)1, 
				unchecked((int)(0xFADED)));
			CheckPendingCachedEmpty(cluster);
			Sharpen.Thread.Sleep(1000);
			CheckPendingCachedEmpty(cluster);
		}
	}
}
