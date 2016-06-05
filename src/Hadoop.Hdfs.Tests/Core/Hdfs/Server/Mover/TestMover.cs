using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Balancer;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Mover
{
	public class TestMover
	{
		/// <exception cref="System.IO.IOException"/>
		internal static Org.Apache.Hadoop.Hdfs.Server.Mover.Mover NewMover(Configuration 
			conf)
		{
			ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
			NUnit.Framework.Assert.AreEqual(1, namenodes.Count);
			IDictionary<URI, IList<Path>> nnMap = Maps.NewHashMap();
			foreach (URI nn in namenodes)
			{
				nnMap[nn] = null;
			}
			IList<NameNodeConnector> nncs = NameNodeConnector.NewNameNodeConnectors(nnMap, typeof(
				Org.Apache.Hadoop.Hdfs.Server.Mover.Mover).Name, Org.Apache.Hadoop.Hdfs.Server.Mover.Mover
				.MoverIdPath, conf, NameNodeConnector.DefaultMaxIdleIterations);
			return new Org.Apache.Hadoop.Hdfs.Server.Mover.Mover(nncs[0], conf, new AtomicInteger
				(0));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestScheduleSameBlock()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				string file = "/testScheduleSameBlock/file";
				{
					FSDataOutputStream @out = dfs.Create(new Path(file));
					@out.WriteChars("testScheduleSameBlock");
					@out.Close();
				}
				Org.Apache.Hadoop.Hdfs.Server.Mover.Mover mover = NewMover(conf);
				mover.Init();
				Mover.Processor processor = new Mover.Processor(this);
				LocatedBlock lb = dfs.GetClient().GetLocatedBlocks(file, 0).Get(0);
				IList<Mover.MLocation> locations = Mover.MLocation.ToLocations(lb);
				Mover.MLocation ml = locations[0];
				Dispatcher.DBlock db = mover.NewDBlock(lb.GetBlock().GetLocalBlock(), locations);
				IList<StorageType> storageTypes = new AList<StorageType>(Arrays.AsList(StorageType
					.Default, StorageType.Default));
				NUnit.Framework.Assert.IsTrue(processor.ScheduleMoveReplica(db, ml, storageTypes)
					);
				NUnit.Framework.Assert.IsFalse(processor.ScheduleMoveReplica(db, ml, storageTypes
					));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestScheduleBlockWithinSameNode()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).StorageTypes
				(new StorageType[] { StorageType.Disk, StorageType.Archive }).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				string file = "/testScheduleWithinSameNode/file";
				Path dir = new Path("/testScheduleWithinSameNode");
				dfs.Mkdirs(dir);
				// write to DISK
				dfs.SetStoragePolicy(dir, "HOT");
				{
					FSDataOutputStream @out = dfs.Create(new Path(file));
					@out.WriteChars("testScheduleWithinSameNode");
					@out.Close();
				}
				//verify before movement
				LocatedBlock lb = dfs.GetClient().GetLocatedBlocks(file, 0).Get(0);
				StorageType[] storageTypes = lb.GetStorageTypes();
				foreach (StorageType storageType in storageTypes)
				{
					NUnit.Framework.Assert.IsTrue(StorageType.Disk == storageType);
				}
				// move to ARCHIVE
				dfs.SetStoragePolicy(dir, "COLD");
				int rc = ToolRunner.Run(conf, new Mover.Cli(), new string[] { "-p", dir.ToString(
					) });
				NUnit.Framework.Assert.AreEqual("Movement to ARCHIVE should be successfull", 0, rc
					);
				// Wait till namenode notified
				Sharpen.Thread.Sleep(3000);
				lb = dfs.GetClient().GetLocatedBlocks(file, 0).Get(0);
				storageTypes = lb.GetStorageTypes();
				foreach (StorageType storageType_1 in storageTypes)
				{
					NUnit.Framework.Assert.IsTrue(StorageType.Archive == storageType_1);
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private void CheckMovePaths(IList<Path> actual, params Path[] expected)
		{
			NUnit.Framework.Assert.AreEqual(expected.Length, actual.Count);
			foreach (Path p in expected)
			{
				NUnit.Framework.Assert.IsTrue(actual.Contains(p));
			}
		}

		/// <summary>Test Mover Cli by specifying a list of files/directories using option "-p".
		/// 	</summary>
		/// <remarks>
		/// Test Mover Cli by specifying a list of files/directories using option "-p".
		/// There is only one namenode (and hence name service) specified in the conf.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoverCli()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).NumDataNodes
				(0).Build();
			try
			{
				Configuration conf = cluster.GetConfiguration(0);
				try
				{
					Mover.Cli.GetNameNodePathsToMove(conf, "-p", "/foo", "bar");
					NUnit.Framework.Assert.Fail("Expected exception for illegal path bar");
				}
				catch (ArgumentException e)
				{
					GenericTestUtils.AssertExceptionContains("bar is not absolute", e);
				}
				IDictionary<URI, IList<Path>> movePaths = Mover.Cli.GetNameNodePathsToMove(conf);
				ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
				NUnit.Framework.Assert.AreEqual(1, namenodes.Count);
				NUnit.Framework.Assert.AreEqual(1, movePaths.Count);
				URI nn = namenodes.GetEnumerator().Next();
				NUnit.Framework.Assert.IsTrue(movePaths.Contains(nn));
				NUnit.Framework.Assert.IsNull(movePaths[nn]);
				movePaths = Mover.Cli.GetNameNodePathsToMove(conf, "-p", "/foo", "/bar");
				namenodes = DFSUtil.GetNsServiceRpcUris(conf);
				NUnit.Framework.Assert.AreEqual(1, movePaths.Count);
				nn = namenodes.GetEnumerator().Next();
				NUnit.Framework.Assert.IsTrue(movePaths.Contains(nn));
				CheckMovePaths(movePaths[nn], new Path("/foo"), new Path("/bar"));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoverCliWithHAConf()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).NnTopology
				(MiniDFSNNTopology.SimpleHATopology()).NumDataNodes(0).Build();
			HATestUtil.SetFailoverConfigurations(cluster, conf, "MyCluster");
			try
			{
				IDictionary<URI, IList<Path>> movePaths = Mover.Cli.GetNameNodePathsToMove(conf, 
					"-p", "/foo", "/bar");
				ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
				NUnit.Framework.Assert.AreEqual(1, namenodes.Count);
				NUnit.Framework.Assert.AreEqual(1, movePaths.Count);
				URI nn = namenodes.GetEnumerator().Next();
				NUnit.Framework.Assert.AreEqual(new URI("hdfs://MyCluster"), nn);
				NUnit.Framework.Assert.IsTrue(movePaths.Contains(nn));
				CheckMovePaths(movePaths[nn], new Path("/foo"), new Path("/bar"));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoverCliWithFederation()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).NnTopology
				(MiniDFSNNTopology.SimpleFederatedTopology(3)).NumDataNodes(0).Build();
			Configuration conf = new HdfsConfiguration();
			DFSTestUtil.SetFederatedConfiguration(cluster, conf);
			try
			{
				ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
				NUnit.Framework.Assert.AreEqual(3, namenodes.Count);
				try
				{
					Mover.Cli.GetNameNodePathsToMove(conf, "-p", "/foo");
					NUnit.Framework.Assert.Fail("Expect exception for missing authority information");
				}
				catch (ArgumentException e)
				{
					GenericTestUtils.AssertExceptionContains("does not contain scheme and authority", 
						e);
				}
				try
				{
					Mover.Cli.GetNameNodePathsToMove(conf, "-p", "hdfs:///foo");
					NUnit.Framework.Assert.Fail("Expect exception for missing authority information");
				}
				catch (ArgumentException e)
				{
					GenericTestUtils.AssertExceptionContains("does not contain scheme and authority", 
						e);
				}
				try
				{
					Mover.Cli.GetNameNodePathsToMove(conf, "-p", "wrong-hdfs://ns1/foo");
					NUnit.Framework.Assert.Fail("Expect exception for wrong scheme");
				}
				catch (ArgumentException e)
				{
					GenericTestUtils.AssertExceptionContains("Cannot resolve the path", e);
				}
				IEnumerator<URI> iter = namenodes.GetEnumerator();
				URI nn1 = iter.Next();
				URI nn2 = iter.Next();
				IDictionary<URI, IList<Path>> movePaths = Mover.Cli.GetNameNodePathsToMove(conf, 
					"-p", nn1 + "/foo", nn1 + "/bar", nn2 + "/foo/bar");
				NUnit.Framework.Assert.AreEqual(2, movePaths.Count);
				CheckMovePaths(movePaths[nn1], new Path("/foo"), new Path("/bar"));
				CheckMovePaths(movePaths[nn2], new Path("/foo/bar"));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoverCliWithFederationHA()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).NnTopology
				(MiniDFSNNTopology.SimpleHAFederatedTopology(3)).NumDataNodes(0).Build();
			Configuration conf = new HdfsConfiguration();
			DFSTestUtil.SetFederatedHAConfiguration(cluster, conf);
			try
			{
				ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
				NUnit.Framework.Assert.AreEqual(3, namenodes.Count);
				IEnumerator<URI> iter = namenodes.GetEnumerator();
				URI nn1 = iter.Next();
				URI nn2 = iter.Next();
				URI nn3 = iter.Next();
				IDictionary<URI, IList<Path>> movePaths = Mover.Cli.GetNameNodePathsToMove(conf, 
					"-p", nn1 + "/foo", nn1 + "/bar", nn2 + "/foo/bar", nn3 + "/foobar");
				NUnit.Framework.Assert.AreEqual(3, movePaths.Count);
				CheckMovePaths(movePaths[nn1], new Path("/foo"), new Path("/bar"));
				CheckMovePaths(movePaths[nn2], new Path("/foo/bar"));
				CheckMovePaths(movePaths[nn3], new Path("/foobar"));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTwoReplicaSameStorageTypeShouldNotSelect()
		{
			// HDFS-8147
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).StorageTypes
				(new StorageType[][] { new StorageType[] { StorageType.Disk, StorageType.Archive
				 }, new StorageType[] { StorageType.Disk, StorageType.Disk }, new StorageType[] 
				{ StorageType.Disk, StorageType.Archive } }).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				string file = "/testForTwoReplicaSameStorageTypeShouldNotSelect";
				// write to DISK
				FSDataOutputStream @out = dfs.Create(new Path(file), (short)2);
				@out.WriteChars("testForTwoReplicaSameStorageTypeShouldNotSelect");
				@out.Close();
				// verify before movement
				LocatedBlock lb = dfs.GetClient().GetLocatedBlocks(file, 0).Get(0);
				StorageType[] storageTypes = lb.GetStorageTypes();
				foreach (StorageType storageType in storageTypes)
				{
					NUnit.Framework.Assert.IsTrue(StorageType.Disk == storageType);
				}
				// move to ARCHIVE
				dfs.SetStoragePolicy(new Path(file), "COLD");
				int rc = ToolRunner.Run(conf, new Mover.Cli(), new string[] { "-p", file.ToString
					() });
				NUnit.Framework.Assert.AreEqual("Movement to ARCHIVE should be successfull", 0, rc
					);
				// Wait till namenode notified
				Sharpen.Thread.Sleep(3000);
				lb = dfs.GetClient().GetLocatedBlocks(file, 0).Get(0);
				storageTypes = lb.GetStorageTypes();
				int archiveCount = 0;
				foreach (StorageType storageType_1 in storageTypes)
				{
					if (StorageType.Archive == storageType_1)
					{
						archiveCount++;
					}
				}
				NUnit.Framework.Assert.AreEqual(archiveCount, 2);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoverFailedRetry()
		{
			// HDFS-8147
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsMoverRetryMaxAttemptsKey, "2");
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).StorageTypes
				(new StorageType[][] { new StorageType[] { StorageType.Disk, StorageType.Archive
				 }, new StorageType[] { StorageType.Disk, StorageType.Archive }, new StorageType
				[] { StorageType.Disk, StorageType.Archive } }).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				string file = "/testMoverFailedRetry";
				// write to DISK
				FSDataOutputStream @out = dfs.Create(new Path(file), (short)2);
				@out.WriteChars("testMoverFailedRetry");
				@out.Close();
				// Delete block file so, block move will fail with FileNotFoundException
				LocatedBlock lb = dfs.GetClient().GetLocatedBlocks(file, 0).Get(0);
				cluster.CorruptBlockOnDataNodesByDeletingBlockFile(lb.GetBlock());
				// move to ARCHIVE
				dfs.SetStoragePolicy(new Path(file), "COLD");
				int rc = ToolRunner.Run(conf, new Mover.Cli(), new string[] { "-p", file.ToString
					() });
				NUnit.Framework.Assert.AreEqual("Movement should fail after some retry", ExitStatus
					.IoException.GetExitCode(), rc);
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
