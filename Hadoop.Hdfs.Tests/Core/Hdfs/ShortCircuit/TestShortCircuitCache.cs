using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Lang.Mutable;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Hamcrest;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Shortcircuit
{
	public class TestShortCircuitCache
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestShortCircuitCache
			));

		private class TestFileDescriptorPair
		{
			internal readonly TemporarySocketDirectory dir = new TemporarySocketDirectory();

			internal readonly FileInputStream[] fis;

			/// <exception cref="System.IO.IOException"/>
			public TestFileDescriptorPair()
			{
				fis = new FileInputStream[2];
				for (int i = 0; i < 2; i++)
				{
					string name = dir.GetDir() + "/file" + i;
					FileOutputStream fos = new FileOutputStream(name);
					if (i == 0)
					{
						// write 'data' file
						fos.Write(1);
					}
					else
					{
						// write 'metadata' file
						BlockMetadataHeader header = new BlockMetadataHeader((short)1, DataChecksum.NewDataChecksum
							(DataChecksum.Type.Null, 4));
						DataOutputStream dos = new DataOutputStream(fos);
						BlockMetadataHeader.WriteHeader(dos, header);
						dos.Close();
					}
					fos.Close();
					fis[i] = new FileInputStream(name);
				}
			}

			public virtual FileInputStream[] GetFileInputStreams()
			{
				return fis;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				IOUtils.Cleanup(Log, fis);
				dir.Close();
			}

			public virtual bool CompareWith(FileInputStream data, FileInputStream meta)
			{
				return ((data == fis[0]) && (meta == fis[1]));
			}
		}

		private class SimpleReplicaCreator : ShortCircuitCache.ShortCircuitReplicaCreator
		{
			private readonly int blockId;

			private readonly ShortCircuitCache cache;

			private readonly TestShortCircuitCache.TestFileDescriptorPair pair;

			internal SimpleReplicaCreator(int blockId, ShortCircuitCache cache, TestShortCircuitCache.TestFileDescriptorPair
				 pair)
			{
				this.blockId = blockId;
				this.cache = cache;
				this.pair = pair;
			}

			public virtual ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo()
			{
				try
				{
					ExtendedBlockId key = new ExtendedBlockId(blockId, "test_bp1");
					return new ShortCircuitReplicaInfo(new ShortCircuitReplica(key, pair.GetFileInputStreams
						()[0], pair.GetFileInputStreams()[1], cache, Time.MonotonicNow(), null));
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreateAndDestroy()
		{
			ShortCircuitCache cache = new ShortCircuitCache(10, 1, 10, 1, 1, 10000, 0);
			cache.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddAndRetrieve()
		{
			ShortCircuitCache cache = new ShortCircuitCache(10, 10000000, 10, 10000000, 1, 10000
				, 0);
			TestShortCircuitCache.TestFileDescriptorPair pair = new TestShortCircuitCache.TestFileDescriptorPair
				();
			ShortCircuitReplicaInfo replicaInfo1 = cache.FetchOrCreate(new ExtendedBlockId(123
				, "test_bp1"), new TestShortCircuitCache.SimpleReplicaCreator(123, cache, pair));
			Preconditions.CheckNotNull(replicaInfo1.GetReplica());
			Preconditions.CheckState(replicaInfo1.GetInvalidTokenException() == null);
			pair.CompareWith(replicaInfo1.GetReplica().GetDataStream(), replicaInfo1.GetReplica
				().GetMetaStream());
			ShortCircuitReplicaInfo replicaInfo2 = cache.FetchOrCreate(new ExtendedBlockId(123
				, "test_bp1"), new _ShortCircuitReplicaCreator_175());
			Preconditions.CheckNotNull(replicaInfo2.GetReplica());
			Preconditions.CheckState(replicaInfo2.GetInvalidTokenException() == null);
			Preconditions.CheckState(replicaInfo1 == replicaInfo2);
			pair.CompareWith(replicaInfo2.GetReplica().GetDataStream(), replicaInfo2.GetReplica
				().GetMetaStream());
			replicaInfo1.GetReplica().Unref();
			replicaInfo2.GetReplica().Unref();
			// Even after the reference count falls to 0, we still keep the replica
			// around for a while (we have configured the expiry period to be really,
			// really long here)
			ShortCircuitReplicaInfo replicaInfo3 = cache.FetchOrCreate(new ExtendedBlockId(123
				, "test_bp1"), new _ShortCircuitReplicaCreator_195());
			Preconditions.CheckNotNull(replicaInfo3.GetReplica());
			Preconditions.CheckState(replicaInfo3.GetInvalidTokenException() == null);
			replicaInfo3.GetReplica().Unref();
			pair.Close();
			cache.Close();
		}

		private sealed class _ShortCircuitReplicaCreator_175 : ShortCircuitCache.ShortCircuitReplicaCreator
		{
			public _ShortCircuitReplicaCreator_175()
			{
			}

			public ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo()
			{
				NUnit.Framework.Assert.Fail("expected to use existing entry.");
				return null;
			}
		}

		private sealed class _ShortCircuitReplicaCreator_195 : ShortCircuitCache.ShortCircuitReplicaCreator
		{
			public _ShortCircuitReplicaCreator_195()
			{
			}

			public ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo()
			{
				NUnit.Framework.Assert.Fail("expected to use existing entry.");
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestExpiry()
		{
			ShortCircuitCache cache = new ShortCircuitCache(2, 1, 1, 10000000, 1, 10000000, 0
				);
			TestShortCircuitCache.TestFileDescriptorPair pair = new TestShortCircuitCache.TestFileDescriptorPair
				();
			ShortCircuitReplicaInfo replicaInfo1 = cache.FetchOrCreate(new ExtendedBlockId(123
				, "test_bp1"), new TestShortCircuitCache.SimpleReplicaCreator(123, cache, pair));
			Preconditions.CheckNotNull(replicaInfo1.GetReplica());
			Preconditions.CheckState(replicaInfo1.GetInvalidTokenException() == null);
			pair.CompareWith(replicaInfo1.GetReplica().GetDataStream(), replicaInfo1.GetReplica
				().GetMetaStream());
			replicaInfo1.GetReplica().Unref();
			MutableBoolean triedToCreate = new MutableBoolean(false);
			do
			{
				Sharpen.Thread.Sleep(10);
				ShortCircuitReplicaInfo replicaInfo2 = cache.FetchOrCreate(new ExtendedBlockId(123
					, "test_bp1"), new _ShortCircuitReplicaCreator_229(triedToCreate));
				if ((replicaInfo2 != null) && (replicaInfo2.GetReplica() != null))
				{
					replicaInfo2.GetReplica().Unref();
				}
			}
			while (triedToCreate.IsFalse());
			cache.Close();
		}

		private sealed class _ShortCircuitReplicaCreator_229 : ShortCircuitCache.ShortCircuitReplicaCreator
		{
			public _ShortCircuitReplicaCreator_229(MutableBoolean triedToCreate)
			{
				this.triedToCreate = triedToCreate;
			}

			public ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo()
			{
				triedToCreate.SetValue(true);
				return null;
			}

			private readonly MutableBoolean triedToCreate;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEviction()
		{
			ShortCircuitCache cache = new ShortCircuitCache(2, 10000000, 1, 10000000, 1, 10000
				, 0);
			TestShortCircuitCache.TestFileDescriptorPair[] pairs = new TestShortCircuitCache.TestFileDescriptorPair
				[] { new TestShortCircuitCache.TestFileDescriptorPair(), new TestShortCircuitCache.TestFileDescriptorPair
				(), new TestShortCircuitCache.TestFileDescriptorPair() };
			ShortCircuitReplicaInfo[] replicaInfos = new ShortCircuitReplicaInfo[] { null, null
				, null };
			for (int i = 0; i < pairs.Length; i++)
			{
				replicaInfos[i] = cache.FetchOrCreate(new ExtendedBlockId(i, "test_bp1"), new TestShortCircuitCache.SimpleReplicaCreator
					(i, cache, pairs[i]));
				Preconditions.CheckNotNull(replicaInfos[i].GetReplica());
				Preconditions.CheckState(replicaInfos[i].GetInvalidTokenException() == null);
				pairs[i].CompareWith(replicaInfos[i].GetReplica().GetDataStream(), replicaInfos[i
					].GetReplica().GetMetaStream());
			}
			// At this point, we have 3 replicas in use.
			// Let's close them all.
			for (int i_1 = 0; i_1 < pairs.Length; i_1++)
			{
				replicaInfos[i_1].GetReplica().Unref();
			}
			// The last two replicas should still be cached.
			for (int i_2 = 1; i_2 < pairs.Length; i_2++)
			{
				int iVal = i_2;
				replicaInfos[i_2] = cache.FetchOrCreate(new ExtendedBlockId(i_2, "test_bp1"), new 
					_ShortCircuitReplicaCreator_277(iVal));
				Preconditions.CheckNotNull(replicaInfos[i_2].GetReplica());
				Preconditions.CheckState(replicaInfos[i_2].GetInvalidTokenException() == null);
				pairs[i_2].CompareWith(replicaInfos[i_2].GetReplica().GetDataStream(), replicaInfos
					[i_2].GetReplica().GetMetaStream());
			}
			// The first (oldest) replica should not be cached.
			MutableBoolean calledCreate = new MutableBoolean(false);
			replicaInfos[0] = cache.FetchOrCreate(new ExtendedBlockId(0, "test_bp1"), new _ShortCircuitReplicaCreator_293
				(calledCreate));
			Preconditions.CheckState(replicaInfos[0].GetReplica() == null);
			NUnit.Framework.Assert.IsTrue(calledCreate.IsTrue());
			// Clean up
			for (int i_3 = 1; i_3 < pairs.Length; i_3++)
			{
				replicaInfos[i_3].GetReplica().Unref();
			}
			for (int i_4 = 0; i_4 < pairs.Length; i_4++)
			{
				pairs[i_4].Close();
			}
			cache.Close();
		}

		private sealed class _ShortCircuitReplicaCreator_277 : ShortCircuitCache.ShortCircuitReplicaCreator
		{
			public _ShortCircuitReplicaCreator_277(int iVal)
			{
				this.iVal = iVal;
			}

			public ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo()
			{
				NUnit.Framework.Assert.Fail("expected to use existing entry for " + iVal);
				return null;
			}

			private readonly int iVal;
		}

		private sealed class _ShortCircuitReplicaCreator_293 : ShortCircuitCache.ShortCircuitReplicaCreator
		{
			public _ShortCircuitReplicaCreator_293(MutableBoolean calledCreate)
			{
				this.calledCreate = calledCreate;
			}

			public ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo()
			{
				calledCreate.SetValue(true);
				return null;
			}

			private readonly MutableBoolean calledCreate;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTimeBasedStaleness()
		{
			// Set up the cache with a short staleness time.
			ShortCircuitCache cache = new ShortCircuitCache(2, 10000000, 1, 10000000, 1, 10, 
				0);
			TestShortCircuitCache.TestFileDescriptorPair[] pairs = new TestShortCircuitCache.TestFileDescriptorPair
				[] { new TestShortCircuitCache.TestFileDescriptorPair(), new TestShortCircuitCache.TestFileDescriptorPair
				() };
			ShortCircuitReplicaInfo[] replicaInfos = new ShortCircuitReplicaInfo[] { null, null
				 };
			long HourInMs = 60 * 60 * 1000;
			for (int i = 0; i < pairs.Length; i++)
			{
				int iVal = i;
				ExtendedBlockId key = new ExtendedBlockId(i, "test_bp1");
				replicaInfos[i] = cache.FetchOrCreate(key, new _ShortCircuitReplicaCreator_330(key
					, pairs, iVal, cache, HourInMs));
				Preconditions.CheckNotNull(replicaInfos[i].GetReplica());
				Preconditions.CheckState(replicaInfos[i].GetInvalidTokenException() == null);
				pairs[i].CompareWith(replicaInfos[i].GetReplica().GetDataStream(), replicaInfos[i
					].GetReplica().GetMetaStream());
			}
			// Keep trying to getOrCreate block 0 until it goes stale (and we must re-create.)
			GenericTestUtils.WaitFor(new _Supplier_351(cache), 500, 60000);
			// Make sure that second replica did not go stale.
			ShortCircuitReplicaInfo info = cache.FetchOrCreate(new ExtendedBlockId(1, "test_bp1"
				), new _ShortCircuitReplicaCreator_371());
			info.GetReplica().Unref();
			// Clean up
			for (int i_1 = 1; i_1 < pairs.Length; i_1++)
			{
				replicaInfos[i_1].GetReplica().Unref();
			}
			cache.Close();
		}

		private sealed class _ShortCircuitReplicaCreator_330 : ShortCircuitCache.ShortCircuitReplicaCreator
		{
			public _ShortCircuitReplicaCreator_330(ExtendedBlockId key, TestShortCircuitCache.TestFileDescriptorPair
				[] pairs, int iVal, ShortCircuitCache cache, long HourInMs)
			{
				this.key = key;
				this.pairs = pairs;
				this.iVal = iVal;
				this.cache = cache;
				this.HourInMs = HourInMs;
			}

			public ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo()
			{
				try
				{
					return new ShortCircuitReplicaInfo(new ShortCircuitReplica(key, pairs[iVal].GetFileInputStreams
						()[0], pairs[iVal].GetFileInputStreams()[1], cache, Time.MonotonicNow() + (iVal 
						* HourInMs), null));
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}

			private readonly ExtendedBlockId key;

			private readonly TestShortCircuitCache.TestFileDescriptorPair[] pairs;

			private readonly int iVal;

			private readonly ShortCircuitCache cache;

			private readonly long HourInMs;
		}

		private sealed class _Supplier_351 : Supplier<bool>
		{
			public _Supplier_351(ShortCircuitCache cache)
			{
				this.cache = cache;
			}

			public bool Get()
			{
				ShortCircuitReplicaInfo info = cache.FetchOrCreate(new ExtendedBlockId(0, "test_bp1"
					), new _ShortCircuitReplicaCreator_355());
				if (info.GetReplica() != null)
				{
					info.GetReplica().Unref();
					return false;
				}
				return true;
			}

			private sealed class _ShortCircuitReplicaCreator_355 : ShortCircuitCache.ShortCircuitReplicaCreator
			{
				public _ShortCircuitReplicaCreator_355()
				{
				}

				public ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo()
				{
					return null;
				}
			}

			private readonly ShortCircuitCache cache;
		}

		private sealed class _ShortCircuitReplicaCreator_371 : ShortCircuitCache.ShortCircuitReplicaCreator
		{
			public _ShortCircuitReplicaCreator_371()
			{
			}

			public ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo()
			{
				NUnit.Framework.Assert.Fail("second replica went stale, despite 1 " + "hour staleness time."
					);
				return null;
			}
		}

		private static Configuration CreateShortCircuitConf(string testName, TemporarySocketDirectory
			 sockDir)
		{
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsClientContext, testName);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 4096);
			conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, new FilePath(sockDir.GetDir(), testName
				).GetAbsolutePath());
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, false);
			conf.SetBoolean(DFSConfigKeys.DfsClientDomainSocketDataTraffic, false);
			DFSInputStream.tcpReadsDisabledForTesting = true;
			DomainSocket.DisableBindPathValidation();
			Assume.AssumeThat(DomainSocket.GetLoadingFailureReason(), CoreMatchers.EqualTo(null
				));
			return conf;
		}

		/// <exception cref="System.IO.IOException"/>
		private static DomainPeer GetDomainPeerToDn(Configuration conf)
		{
			DomainSocket sock = DomainSocket.Connect(conf.Get(DFSConfigKeys.DfsDomainSocketPathKey
				));
			return new DomainPeer(sock);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAllocShm()
		{
			BlockReaderTestUtil.EnableShortCircuitShmTracing();
			TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
			Configuration conf = CreateShortCircuitConf("testAllocShm", sockDir);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			DistributedFileSystem fs = cluster.GetFileSystem();
			ShortCircuitCache cache = fs.GetClient().GetClientContext().GetShortCircuitCache(
				);
			cache.GetDfsClientShmManager().Visit(new _Visitor_423());
			// The ClientShmManager starts off empty
			DomainPeer peer = GetDomainPeerToDn(conf);
			MutableBoolean usedPeer = new MutableBoolean(false);
			ExtendedBlockId blockId = new ExtendedBlockId(123, "xyz");
			DatanodeInfo datanode = new DatanodeInfo(cluster.GetDataNodes()[0].GetDatanodeId(
				));
			// Allocating the first shm slot requires using up a peer.
			ShortCircuitShm.Slot slot = cache.AllocShmSlot(datanode, peer, usedPeer, blockId, 
				"testAllocShm_client");
			NUnit.Framework.Assert.IsNotNull(slot);
			NUnit.Framework.Assert.IsTrue(usedPeer.BooleanValue());
			cache.GetDfsClientShmManager().Visit(new _Visitor_441(datanode));
			// The ClientShmManager starts off empty
			cache.ScheduleSlotReleaser(slot);
			// Wait for the slot to be released, and the shared memory area to be
			// closed.  Since we didn't register this shared memory segment on the
			// server, it will also be a test of how well the server deals with
			// bogus client behavior.
			GenericTestUtils.WaitFor(new _Supplier_458(cache, datanode), 10, 60000);
			cluster.Shutdown();
			sockDir.Close();
		}

		private sealed class _Visitor_423 : DfsClientShmManager.Visitor
		{
			public _Visitor_423()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public void Visit(Dictionary<DatanodeInfo, DfsClientShmManager.PerDatanodeVisitorInfo
				> info)
			{
				NUnit.Framework.Assert.AreEqual(0, info.Count);
			}
		}

		private sealed class _Visitor_441 : DfsClientShmManager.Visitor
		{
			public _Visitor_441(DatanodeInfo datanode)
			{
				this.datanode = datanode;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Visit(Dictionary<DatanodeInfo, DfsClientShmManager.PerDatanodeVisitorInfo
				> info)
			{
				NUnit.Framework.Assert.AreEqual(1, info.Count);
				DfsClientShmManager.PerDatanodeVisitorInfo vinfo = info[datanode];
				NUnit.Framework.Assert.IsFalse(vinfo.disabled);
				NUnit.Framework.Assert.AreEqual(0, vinfo.full.Count);
				NUnit.Framework.Assert.AreEqual(1, vinfo.notFull.Count);
			}

			private readonly DatanodeInfo datanode;
		}

		private sealed class _Supplier_458 : Supplier<bool>
		{
			public _Supplier_458(ShortCircuitCache cache, DatanodeInfo datanode)
			{
				this.cache = cache;
				this.datanode = datanode;
			}

			public bool Get()
			{
				MutableBoolean done = new MutableBoolean(false);
				try
				{
					cache.GetDfsClientShmManager().Visit(new _Visitor_463(done, datanode));
				}
				catch (IOException e)
				{
					TestShortCircuitCache.Log.Error("error running visitor", e);
				}
				return done.BooleanValue();
			}

			private sealed class _Visitor_463 : DfsClientShmManager.Visitor
			{
				public _Visitor_463(MutableBoolean done, DatanodeInfo datanode)
				{
					this.done = done;
					this.datanode = datanode;
				}

				/// <exception cref="System.IO.IOException"/>
				public void Visit(Dictionary<DatanodeInfo, DfsClientShmManager.PerDatanodeVisitorInfo
					> info)
				{
					done.SetValue(info[datanode].full.IsEmpty() && info[datanode].notFull.IsEmpty());
				}

				private readonly MutableBoolean done;

				private readonly DatanodeInfo datanode;
			}

			private readonly ShortCircuitCache cache;

			private readonly DatanodeInfo datanode;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestShmBasedStaleness()
		{
			BlockReaderTestUtil.EnableShortCircuitShmTracing();
			TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
			Configuration conf = CreateShortCircuitConf("testShmBasedStaleness", sockDir);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			DistributedFileSystem fs = cluster.GetFileSystem();
			ShortCircuitCache cache = fs.GetClient().GetClientContext().GetShortCircuitCache(
				);
			string TestFile = "/test_file";
			int TestFileLen = 8193;
			int Seed = unchecked((int)(0xFADED));
			DFSTestUtil.CreateFile(fs, new Path(TestFile), TestFileLen, (short)1, Seed);
			FSDataInputStream fis = fs.Open(new Path(TestFile));
			int first = fis.Read();
			ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, new Path(TestFile));
			NUnit.Framework.Assert.IsTrue(first != -1);
			cache.Accept(new _CacheVisitor_502(block));
			// Stop the Namenode.  This will close the socket keeping the client's
			// shared memory segment alive, and make it stale.
			cluster.GetDataNodes()[0].Shutdown();
			cache.Accept(new _CacheVisitor_518(block));
			cluster.Shutdown();
			sockDir.Close();
		}

		private sealed class _CacheVisitor_502 : ShortCircuitCache.CacheVisitor
		{
			public _CacheVisitor_502(ExtendedBlock block)
			{
				this.block = block;
			}

			public void Visit(int numOutstandingMmaps, IDictionary<ExtendedBlockId, ShortCircuitReplica
				> replicas, IDictionary<ExtendedBlockId, SecretManager.InvalidToken> failedLoads
				, IDictionary<long, ShortCircuitReplica> evictable, IDictionary<long, ShortCircuitReplica
				> evictableMmapped)
			{
				ShortCircuitReplica replica = replicas[ExtendedBlockId.FromExtendedBlock(block)];
				NUnit.Framework.Assert.IsNotNull(replica);
				NUnit.Framework.Assert.IsTrue(replica.GetSlot().IsValid());
			}

			private readonly ExtendedBlock block;
		}

		private sealed class _CacheVisitor_518 : ShortCircuitCache.CacheVisitor
		{
			public _CacheVisitor_518(ExtendedBlock block)
			{
				this.block = block;
			}

			public void Visit(int numOutstandingMmaps, IDictionary<ExtendedBlockId, ShortCircuitReplica
				> replicas, IDictionary<ExtendedBlockId, SecretManager.InvalidToken> failedLoads
				, IDictionary<long, ShortCircuitReplica> evictable, IDictionary<long, ShortCircuitReplica
				> evictableMmapped)
			{
				ShortCircuitReplica replica = replicas[ExtendedBlockId.FromExtendedBlock(block)];
				NUnit.Framework.Assert.IsNotNull(replica);
				NUnit.Framework.Assert.IsFalse(replica.GetSlot().IsValid());
			}

			private readonly ExtendedBlock block;
		}

		/// <summary>Test unlinking a file whose blocks we are caching in the DFSClient.</summary>
		/// <remarks>
		/// Test unlinking a file whose blocks we are caching in the DFSClient.
		/// The DataNode will notify the DFSClient that the replica is stale via the
		/// ShortCircuitShm.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestUnlinkingReplicasInFileDescriptorCache()
		{
			BlockReaderTestUtil.EnableShortCircuitShmTracing();
			TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
			Configuration conf = CreateShortCircuitConf("testUnlinkingReplicasInFileDescriptorCache"
				, sockDir);
			// We don't want the CacheCleaner to time out short-circuit shared memory
			// segments during the test, so set the timeout really high.
			conf.SetLong(DFSConfigKeys.DfsClientReadShortcircuitStreamsCacheExpiryMsKey, 1000000000L
				);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			DistributedFileSystem fs = cluster.GetFileSystem();
			ShortCircuitCache cache = fs.GetClient().GetClientContext().GetShortCircuitCache(
				);
			cache.GetDfsClientShmManager().Visit(new _Visitor_556());
			// The ClientShmManager starts off empty.
			Path TestPath = new Path("/test_file");
			int TestFileLen = 8193;
			int Seed = unchecked((int)(0xFADE0));
			DFSTestUtil.CreateFile(fs, TestPath, TestFileLen, (short)1, Seed);
			byte[] contents = DFSTestUtil.ReadFileBuffer(fs, TestPath);
			byte[] expected = DFSTestUtil.CalculateFileContentsFromSeed(Seed, TestFileLen);
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(contents, expected));
			// Loading this file brought the ShortCircuitReplica into our local
			// replica cache.
			DatanodeInfo datanode = new DatanodeInfo(cluster.GetDataNodes()[0].GetDatanodeId(
				));
			cache.GetDfsClientShmManager().Visit(new _Visitor_577(datanode));
			// Remove the file whose blocks we just read.
			fs.Delete(TestPath, false);
			// Wait for the replica to be purged from the DFSClient's cache.
			GenericTestUtils.WaitFor(new _Supplier_593(this, cache, datanode), 10, 60000);
			// Check that all slots have been invalidated.
			cluster.Shutdown();
			sockDir.Close();
		}

		private sealed class _Visitor_556 : DfsClientShmManager.Visitor
		{
			public _Visitor_556()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public void Visit(Dictionary<DatanodeInfo, DfsClientShmManager.PerDatanodeVisitorInfo
				> info)
			{
				NUnit.Framework.Assert.AreEqual(0, info.Count);
			}
		}

		private sealed class _Visitor_577 : DfsClientShmManager.Visitor
		{
			public _Visitor_577(DatanodeInfo datanode)
			{
				this.datanode = datanode;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Visit(Dictionary<DatanodeInfo, DfsClientShmManager.PerDatanodeVisitorInfo
				> info)
			{
				NUnit.Framework.Assert.IsTrue(info[datanode].full.IsEmpty());
				NUnit.Framework.Assert.IsFalse(info[datanode].disabled);
				NUnit.Framework.Assert.AreEqual(1, info[datanode].notFull.Values.Count);
				DfsClientShm shm = info[datanode].notFull.Values.GetEnumerator().Next();
				NUnit.Framework.Assert.IsFalse(shm.IsDisconnected());
			}

			private readonly DatanodeInfo datanode;
		}

		private sealed class _Supplier_593 : Supplier<bool>
		{
			public _Supplier_593(TestShortCircuitCache _enclosing, ShortCircuitCache cache, DatanodeInfo
				 datanode)
			{
				this._enclosing = _enclosing;
				this.cache = cache;
				this.datanode = datanode;
				this.done = new MutableBoolean(true);
			}

			internal MutableBoolean done;

			public bool Get()
			{
				try
				{
					this.done.SetValue(true);
					cache.GetDfsClientShmManager().Visit(new _Visitor_599(this, datanode));
				}
				catch (IOException e)
				{
					TestShortCircuitCache.Log.Error("error running visitor", e);
				}
				return this.done.BooleanValue();
			}

			private sealed class _Visitor_599 : DfsClientShmManager.Visitor
			{
				public _Visitor_599(_Supplier_593 _enclosing, DatanodeInfo datanode)
				{
					this._enclosing = _enclosing;
					this.datanode = datanode;
				}

				/// <exception cref="System.IO.IOException"/>
				public void Visit(Dictionary<DatanodeInfo, DfsClientShmManager.PerDatanodeVisitorInfo
					> info)
				{
					NUnit.Framework.Assert.IsTrue(info[datanode].full.IsEmpty());
					NUnit.Framework.Assert.IsFalse(info[datanode].disabled);
					NUnit.Framework.Assert.AreEqual(1, info[datanode].notFull.Values.Count);
					DfsClientShm shm = info[datanode].notFull.Values.GetEnumerator().Next();
					for (IEnumerator<ShortCircuitShm.Slot> iter = shm.SlotIterator(); iter.HasNext(); )
					{
						ShortCircuitShm.Slot slot = iter.Next();
						if (slot.IsValid())
						{
							this._enclosing.done.SetValue(false);
						}
					}
				}

				private readonly _Supplier_593 _enclosing;

				private readonly DatanodeInfo datanode;
			}

			private readonly TestShortCircuitCache _enclosing;

			private readonly ShortCircuitCache cache;

			private readonly DatanodeInfo datanode;
		}

		private static void CheckNumberOfSegmentsAndSlots(int expectedSegments, int expectedSlots
			, ShortCircuitRegistry registry)
		{
			registry.Visit(new _Visitor_631(expectedSegments, expectedSlots));
		}

		private sealed class _Visitor_631 : ShortCircuitRegistry.Visitor
		{
			public _Visitor_631(int expectedSegments, int expectedSlots)
			{
				this.expectedSegments = expectedSegments;
				this.expectedSlots = expectedSlots;
			}

			public void Accept(Dictionary<ShortCircuitShm.ShmId, ShortCircuitRegistry.RegisteredShm
				> segments, HashMultimap<ExtendedBlockId, ShortCircuitShm.Slot> slots)
			{
				NUnit.Framework.Assert.AreEqual(expectedSegments, segments.Count);
				NUnit.Framework.Assert.AreEqual(expectedSlots, slots.Size());
			}

			private readonly int expectedSegments;

			private readonly int expectedSlots;
		}

		public class TestCleanupFailureInjector : BlockReaderFactory.FailureInjector
		{
			/// <exception cref="System.IO.IOException"/>
			public override void InjectRequestFileDescriptorsFailure()
			{
				throw new IOException("injected I/O error");
			}
		}

		// Regression test for HDFS-7915
		/// <exception cref="System.Exception"/>
		public virtual void TestDataXceiverCleansUpSlotsOnFailure()
		{
			BlockReaderTestUtil.EnableShortCircuitShmTracing();
			TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
			Configuration conf = CreateShortCircuitConf("testDataXceiverCleansUpSlotsOnFailure"
				, sockDir);
			conf.SetLong(DFSConfigKeys.DfsClientReadShortcircuitStreamsCacheExpiryMsKey, 1000000000L
				);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			DistributedFileSystem fs = cluster.GetFileSystem();
			Path TestPath1 = new Path("/test_file1");
			Path TestPath2 = new Path("/test_file2");
			int TestFileLen = 4096;
			int Seed = unchecked((int)(0xFADE1));
			DFSTestUtil.CreateFile(fs, TestPath1, TestFileLen, (short)1, Seed);
			DFSTestUtil.CreateFile(fs, TestPath2, TestFileLen, (short)1, Seed);
			// The first read should allocate one shared memory segment and slot.
			DFSTestUtil.ReadFileBuffer(fs, TestPath1);
			// The second read should fail, and we should only have 1 segment and 1 slot
			// left.
			fs.GetClient().GetConf().brfFailureInjector = new TestShortCircuitCache.TestCleanupFailureInjector
				();
			try
			{
				DFSTestUtil.ReadFileBuffer(fs, TestPath2);
			}
			catch (Exception t)
			{
				GenericTestUtils.AssertExceptionContains("TCP reads were disabled for " + "testing, but we failed to do a non-TCP read."
					, t);
			}
			CheckNumberOfSegmentsAndSlots(1, 1, cluster.GetDataNodes()[0].GetShortCircuitRegistry
				());
			cluster.Shutdown();
			sockDir.Close();
		}

		// Regression test for HADOOP-11802
		/// <exception cref="System.Exception"/>
		public virtual void TestDataXceiverHandlesRequestShortCircuitShmFailure()
		{
			BlockReaderTestUtil.EnableShortCircuitShmTracing();
			TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
			Configuration conf = CreateShortCircuitConf("testDataXceiverHandlesRequestShortCircuitShmFailure"
				, sockDir);
			conf.SetLong(DFSConfigKeys.DfsClientReadShortcircuitStreamsCacheExpiryMsKey, 1000000000L
				);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			DistributedFileSystem fs = cluster.GetFileSystem();
			Path TestPath1 = new Path("/test_file1");
			DFSTestUtil.CreateFile(fs, TestPath1, 4096, (short)1, unchecked((int)(0xFADE1)));
			Log.Info("Setting failure injector and performing a read which " + "should fail..."
				);
			DataNodeFaultInjector failureInjector = Org.Mockito.Mockito.Mock<DataNodeFaultInjector
				>();
			Org.Mockito.Mockito.DoAnswer(new _Answer_710()).When(failureInjector).SendShortCircuitShmResponse
				();
			DataNodeFaultInjector prevInjector = DataNodeFaultInjector.instance;
			DataNodeFaultInjector.instance = failureInjector;
			try
			{
				// The first read will try to allocate a shared memory segment and slot.
				// The shared memory segment allocation will fail because of the failure
				// injector.
				DFSTestUtil.ReadFileBuffer(fs, TestPath1);
				NUnit.Framework.Assert.Fail("expected readFileBuffer to fail, but it succeeded.");
			}
			catch (Exception t)
			{
				GenericTestUtils.AssertExceptionContains("TCP reads were disabled for " + "testing, but we failed to do a non-TCP read."
					, t);
			}
			CheckNumberOfSegmentsAndSlots(0, 0, cluster.GetDataNodes()[0].GetShortCircuitRegistry
				());
			Log.Info("Clearing failure injector and performing another read...");
			DataNodeFaultInjector.instance = prevInjector;
			fs.GetClient().GetClientContext().GetDomainSocketFactory().ClearPathMap();
			// The second read should succeed.
			DFSTestUtil.ReadFileBuffer(fs, TestPath1);
			// We should have added a new short-circuit shared memory segment and slot.
			CheckNumberOfSegmentsAndSlots(1, 1, cluster.GetDataNodes()[0].GetShortCircuitRegistry
				());
			cluster.Shutdown();
			sockDir.Close();
		}

		private sealed class _Answer_710 : Answer<Void>
		{
			public _Answer_710()
			{
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				throw new IOException("injected error into sendShmResponse");
			}
		}

		public class TestPreReceiptVerificationFailureInjector : BlockReaderFactory.FailureInjector
		{
			public override bool GetSupportsReceiptVerification()
			{
				return false;
			}
		}

		// Regression test for HDFS-8070
		/// <exception cref="System.Exception"/>
		public virtual void TestPreReceiptVerificationDfsClientCanDoScr()
		{
			BlockReaderTestUtil.EnableShortCircuitShmTracing();
			TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
			Configuration conf = CreateShortCircuitConf("testPreReceiptVerificationDfsClientCanDoScr"
				, sockDir);
			conf.SetLong(DFSConfigKeys.DfsClientReadShortcircuitStreamsCacheExpiryMsKey, 1000000000L
				);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			DistributedFileSystem fs = cluster.GetFileSystem();
			fs.GetClient().GetConf().brfFailureInjector = new TestShortCircuitCache.TestPreReceiptVerificationFailureInjector
				();
			Path TestPath1 = new Path("/test_file1");
			DFSTestUtil.CreateFile(fs, TestPath1, 4096, (short)1, unchecked((int)(0xFADE2)));
			Path TestPath2 = new Path("/test_file2");
			DFSTestUtil.CreateFile(fs, TestPath2, 4096, (short)1, unchecked((int)(0xFADE2)));
			DFSTestUtil.ReadFileBuffer(fs, TestPath1);
			DFSTestUtil.ReadFileBuffer(fs, TestPath2);
			ShortCircuitRegistry registry = cluster.GetDataNodes()[0].GetShortCircuitRegistry
				();
			registry.Visit(new _Visitor_780());
			cluster.Shutdown();
			sockDir.Close();
		}

		private sealed class _Visitor_780 : ShortCircuitRegistry.Visitor
		{
			public _Visitor_780()
			{
			}

			public void Accept(Dictionary<ShortCircuitShm.ShmId, ShortCircuitRegistry.RegisteredShm
				> segments, HashMultimap<ExtendedBlockId, ShortCircuitShm.Slot> slots)
			{
				NUnit.Framework.Assert.AreEqual(1, segments.Count);
				NUnit.Framework.Assert.AreEqual(2, slots.Size());
			}
		}
	}
}
