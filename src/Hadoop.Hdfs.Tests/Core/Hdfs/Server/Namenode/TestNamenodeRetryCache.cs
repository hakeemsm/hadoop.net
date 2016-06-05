using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Tests for ensuring the namenode retry cache works correctly for
	/// non-idempotent requests.
	/// </summary>
	/// <remarks>
	/// Tests for ensuring the namenode retry cache works correctly for
	/// non-idempotent requests.
	/// Retry cache works based on tracking previously received request based on the
	/// ClientId and CallId received in RPC requests and storing the response. The
	/// response is replayed on retry when the same request is received again.
	/// The test works by manipulating the Rpc
	/// <see cref="Org.Apache.Hadoop.Ipc.Server"/>
	/// current RPC call. For
	/// testing retried requests, an Rpc callId is generated only once using
	/// <see cref="NewCall()"/>
	/// and reused for many method calls. For testing non-retried
	/// request, a new callId is generated using
	/// <see cref="NewCall()"/>
	/// .
	/// </remarks>
	public class TestNamenodeRetryCache
	{
		private static readonly byte[] ClientId = ClientId.GetClientId();

		private static MiniDFSCluster cluster;

		private static NamenodeProtocols nnRpc;

		private static readonly FsPermission perm = FsPermission.GetDefault();

		private static DistributedFileSystem filesystem;

		private static int callId = 100;

		private static Configuration conf;

		private const int BlockSize = 512;

		/// <summary>Start a cluster</summary>
		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeEnableRetryCacheKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			cluster = new MiniDFSCluster.Builder(conf).Build();
			cluster.WaitActive();
			nnRpc = cluster.GetNameNode().GetRpcServer();
			filesystem = cluster.GetFileSystem();
		}

		/// <summary>Cleanup after the test</summary>
		/// <exception cref="System.IO.IOException"></exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"></exception>
		/// <exception cref="SafeModeException"></exception>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"></exception>
		[TearDown]
		public virtual void Cleanup()
		{
			cluster.Shutdown();
		}

		/// <summary>Set the current Server RPC call</summary>
		public static void NewCall()
		{
			Server.Call call = new Server.Call(++callId, 1, null, null, RPC.RpcKind.RpcProtocolBuffer
				, ClientId);
			Org.Apache.Hadoop.Ipc.Server.GetCurCall().Set(call);
		}

		public static void ResetCall()
		{
			Server.Call call = new Server.Call(RpcConstants.InvalidCallId, 1, null, null, RPC.RpcKind
				.RpcProtocolBuffer, RpcConstants.DummyClientId);
			Org.Apache.Hadoop.Ipc.Server.GetCurCall().Set(call);
		}

		/// <exception cref="System.Exception"/>
		private void ConcatSetup(string file1, string file2)
		{
			DFSTestUtil.CreateFile(filesystem, new Path(file1), BlockSize, (short)1, 0L);
			DFSTestUtil.CreateFile(filesystem, new Path(file2), BlockSize, (short)1, 0L);
		}

		/// <summary>Tests for concat call</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConcat()
		{
			ResetCall();
			string file1 = "/testNamenodeRetryCache/testConcat/file1";
			string file2 = "/testNamenodeRetryCache/testConcat/file2";
			// Two retried concat calls succeed
			ConcatSetup(file1, file2);
			NewCall();
			nnRpc.Concat(file1, new string[] { file2 });
			nnRpc.Concat(file1, new string[] { file2 });
			nnRpc.Concat(file1, new string[] { file2 });
			// A non-retried concat request fails
			NewCall();
			try
			{
				// Second non-retry call should fail with an exception
				nnRpc.Concat(file1, new string[] { file2 });
				NUnit.Framework.Assert.Fail("testConcat - expected exception is not thrown");
			}
			catch (IOException)
			{
			}
		}

		// Expected
		/// <summary>Tests for delete call</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelete()
		{
			string dir = "/testNamenodeRetryCache/testDelete";
			// Two retried calls to create a non existent file
			NewCall();
			nnRpc.Mkdirs(dir, perm, true);
			NewCall();
			NUnit.Framework.Assert.IsTrue(nnRpc.Delete(dir, false));
			NUnit.Framework.Assert.IsTrue(nnRpc.Delete(dir, false));
			NUnit.Framework.Assert.IsTrue(nnRpc.Delete(dir, false));
			// non-retried call fails and gets false as return
			NewCall();
			NUnit.Framework.Assert.IsFalse(nnRpc.Delete(dir, false));
		}

		/// <summary>Test for createSymlink</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateSymlink()
		{
			string target = "/testNamenodeRetryCache/testCreateSymlink/target";
			// Two retried symlink calls succeed
			NewCall();
			nnRpc.CreateSymlink(target, "/a/b", perm, true);
			nnRpc.CreateSymlink(target, "/a/b", perm, true);
			nnRpc.CreateSymlink(target, "/a/b", perm, true);
			// non-retried call fails
			NewCall();
			try
			{
				// Second non-retry call should fail with an exception
				nnRpc.CreateSymlink(target, "/a/b", perm, true);
				NUnit.Framework.Assert.Fail("testCreateSymlink - expected exception is not thrown"
					);
			}
			catch (IOException)
			{
			}
		}

		// Expected
		/// <summary>Test for create file</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreate()
		{
			string src = "/testNamenodeRetryCache/testCreate/file";
			// Two retried calls succeed
			NewCall();
			HdfsFileStatus status = nnRpc.Create(src, perm, "holder", new EnumSetWritable<CreateFlag
				>(EnumSet.Of(CreateFlag.Create)), true, (short)1, BlockSize, null);
			NUnit.Framework.Assert.AreEqual(status, nnRpc.Create(src, perm, "holder", new EnumSetWritable
				<CreateFlag>(EnumSet.Of(CreateFlag.Create)), true, (short)1, BlockSize, null));
			NUnit.Framework.Assert.AreEqual(status, nnRpc.Create(src, perm, "holder", new EnumSetWritable
				<CreateFlag>(EnumSet.Of(CreateFlag.Create)), true, (short)1, BlockSize, null));
			// A non-retried call fails
			NewCall();
			try
			{
				nnRpc.Create(src, perm, "holder", new EnumSetWritable<CreateFlag>(EnumSet.Of(CreateFlag
					.Create)), true, (short)1, BlockSize, null);
				NUnit.Framework.Assert.Fail("testCreate - expected exception is not thrown");
			}
			catch (IOException)
			{
			}
		}

		// expected
		/// <summary>Test for rename1</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppend()
		{
			string src = "/testNamenodeRetryCache/testAppend/src";
			ResetCall();
			// Create a file with partial block
			DFSTestUtil.CreateFile(filesystem, new Path(src), 128, (short)1, 0L);
			// Retried append requests succeed
			NewCall();
			LastBlockWithStatus b = nnRpc.Append(src, "holder", new EnumSetWritable<CreateFlag
				>(EnumSet.Of(CreateFlag.Append)));
			NUnit.Framework.Assert.AreEqual(b, nnRpc.Append(src, "holder", new EnumSetWritable
				<CreateFlag>(EnumSet.Of(CreateFlag.Append))));
			NUnit.Framework.Assert.AreEqual(b, nnRpc.Append(src, "holder", new EnumSetWritable
				<CreateFlag>(EnumSet.Of(CreateFlag.Append))));
			// non-retried call fails
			NewCall();
			try
			{
				nnRpc.Append(src, "holder", new EnumSetWritable<CreateFlag>(EnumSet.Of(CreateFlag
					.Append)));
				NUnit.Framework.Assert.Fail("testAppend - expected exception is not thrown");
			}
			catch (Exception)
			{
			}
		}

		// Expected
		/// <summary>Test for rename1</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRename1()
		{
			string src = "/testNamenodeRetryCache/testRename1/src";
			string target = "/testNamenodeRetryCache/testRename1/target";
			ResetCall();
			nnRpc.Mkdirs(src, perm, true);
			// Retried renames succeed
			NewCall();
			NUnit.Framework.Assert.IsTrue(nnRpc.Rename(src, target));
			NUnit.Framework.Assert.IsTrue(nnRpc.Rename(src, target));
			NUnit.Framework.Assert.IsTrue(nnRpc.Rename(src, target));
			// A non-retried request fails
			NewCall();
			NUnit.Framework.Assert.IsFalse(nnRpc.Rename(src, target));
		}

		/// <summary>Test for rename2</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRename2()
		{
			string src = "/testNamenodeRetryCache/testRename2/src";
			string target = "/testNamenodeRetryCache/testRename2/target";
			ResetCall();
			nnRpc.Mkdirs(src, perm, true);
			// Retried renames succeed
			NewCall();
			nnRpc.Rename2(src, target, Options.Rename.None);
			nnRpc.Rename2(src, target, Options.Rename.None);
			nnRpc.Rename2(src, target, Options.Rename.None);
			// A non-retried request fails
			NewCall();
			try
			{
				nnRpc.Rename2(src, target, Options.Rename.None);
				NUnit.Framework.Assert.Fail("testRename 2 expected exception is not thrown");
			}
			catch (IOException)
			{
			}
		}

		// expected
		/// <summary>
		/// Make sure a retry call does not hang because of the exception thrown in the
		/// first call.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestUpdatePipelineWithFailOver()
		{
			cluster.Shutdown();
			nnRpc = null;
			filesystem = null;
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(1).Build();
			cluster.WaitActive();
			NamenodeProtocols ns0 = cluster.GetNameNodeRpc(0);
			ExtendedBlock oldBlock = new ExtendedBlock();
			ExtendedBlock newBlock = new ExtendedBlock();
			DatanodeID[] newNodes = new DatanodeID[2];
			string[] newStorages = new string[2];
			NewCall();
			try
			{
				ns0.UpdatePipeline("testClient", oldBlock, newBlock, newNodes, newStorages);
				NUnit.Framework.Assert.Fail("Expect StandbyException from the updatePipeline call"
					);
			}
			catch (StandbyException e)
			{
				// expected, since in the beginning both nn are in standby state
				GenericTestUtils.AssertExceptionContains(HAServiceProtocol.HAServiceState.Standby
					.ToString(), e);
			}
			cluster.TransitionToActive(0);
			try
			{
				ns0.UpdatePipeline("testClient", oldBlock, newBlock, newNodes, newStorages);
			}
			catch (IOException)
			{
			}
		}

		// ignore call should not hang.
		/// <summary>Test for crateSnapshot</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSnapshotMethods()
		{
			string dir = "/testNamenodeRetryCache/testCreateSnapshot/src";
			ResetCall();
			nnRpc.Mkdirs(dir, perm, true);
			nnRpc.AllowSnapshot(dir);
			// Test retry of create snapshot
			NewCall();
			string name = nnRpc.CreateSnapshot(dir, "snap1");
			NUnit.Framework.Assert.AreEqual(name, nnRpc.CreateSnapshot(dir, "snap1"));
			NUnit.Framework.Assert.AreEqual(name, nnRpc.CreateSnapshot(dir, "snap1"));
			NUnit.Framework.Assert.AreEqual(name, nnRpc.CreateSnapshot(dir, "snap1"));
			// Non retried calls should fail
			NewCall();
			try
			{
				nnRpc.CreateSnapshot(dir, "snap1");
				NUnit.Framework.Assert.Fail("testSnapshotMethods expected exception is not thrown"
					);
			}
			catch (IOException)
			{
			}
			// exptected
			// Test retry of rename snapshot
			NewCall();
			nnRpc.RenameSnapshot(dir, "snap1", "snap2");
			nnRpc.RenameSnapshot(dir, "snap1", "snap2");
			nnRpc.RenameSnapshot(dir, "snap1", "snap2");
			// Non retried calls should fail
			NewCall();
			try
			{
				nnRpc.RenameSnapshot(dir, "snap1", "snap2");
				NUnit.Framework.Assert.Fail("testSnapshotMethods expected exception is not thrown"
					);
			}
			catch (IOException)
			{
			}
			// expected
			// Test retry of delete snapshot
			NewCall();
			nnRpc.DeleteSnapshot(dir, "snap2");
			nnRpc.DeleteSnapshot(dir, "snap2");
			nnRpc.DeleteSnapshot(dir, "snap2");
			// Non retried calls should fail
			NewCall();
			try
			{
				nnRpc.DeleteSnapshot(dir, "snap2");
				NUnit.Framework.Assert.Fail("testSnapshotMethods expected exception is not thrown"
					);
			}
			catch (IOException)
			{
			}
		}

		// expected
		[NUnit.Framework.Test]
		public virtual void TestRetryCacheConfig()
		{
			// By default retry configuration should be enabled
			Configuration conf = new HdfsConfiguration();
			NUnit.Framework.Assert.IsNotNull(FSNamesystem.InitRetryCache(conf));
			// If retry cache is disabled, it should not be created
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeEnableRetryCacheKey, false);
			NUnit.Framework.Assert.IsNull(FSNamesystem.InitRetryCache(conf));
		}

		/// <summary>
		/// After run a set of operations, restart NN and check if the retry cache has
		/// been rebuilt based on the editlog.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRetryCacheRebuild()
		{
			DFSTestUtil.RunOperations(cluster, filesystem, conf, BlockSize, 0);
			FSNamesystem namesystem = cluster.GetNamesystem();
			LightWeightCache<RetryCache.CacheEntry, RetryCache.CacheEntry> cacheSet = (LightWeightCache
				<RetryCache.CacheEntry, RetryCache.CacheEntry>)namesystem.GetRetryCache().GetCacheSet
				();
			NUnit.Framework.Assert.AreEqual(25, cacheSet.Size());
			IDictionary<RetryCache.CacheEntry, RetryCache.CacheEntry> oldEntries = new Dictionary
				<RetryCache.CacheEntry, RetryCache.CacheEntry>();
			IEnumerator<RetryCache.CacheEntry> iter = cacheSet.GetEnumerator();
			while (iter.HasNext())
			{
				RetryCache.CacheEntry entry = iter.Next();
				oldEntries[entry] = entry;
			}
			// restart NameNode
			cluster.RestartNameNode();
			cluster.WaitActive();
			namesystem = cluster.GetNamesystem();
			// check retry cache
			NUnit.Framework.Assert.IsTrue(namesystem.HasRetryCache());
			cacheSet = (LightWeightCache<RetryCache.CacheEntry, RetryCache.CacheEntry>)namesystem
				.GetRetryCache().GetCacheSet();
			NUnit.Framework.Assert.AreEqual(25, cacheSet.Size());
			iter = cacheSet.GetEnumerator();
			while (iter.HasNext())
			{
				RetryCache.CacheEntry entry = iter.Next();
				NUnit.Framework.Assert.IsTrue(oldEntries.Contains(entry));
			}
		}
	}
}
