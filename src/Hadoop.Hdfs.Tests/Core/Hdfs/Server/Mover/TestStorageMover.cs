using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Server.Balancer;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.IO;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Mover
{
	/// <summary>Test the data migration tool (for Archival Storage)</summary>
	public class TestStorageMover
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestStorageMover));

		static TestStorageMover()
		{
			((Log4JLogger)LogFactory.GetLog(typeof(BlockPlacementPolicy))).GetLogger().SetLevel
				(Level.All);
			((Log4JLogger)LogFactory.GetLog(typeof(Dispatcher))).GetLogger().SetLevel(Level.All
				);
			((Log4JLogger)LogFactory.GetLog(typeof(DataTransferProtocol))).GetLogger().SetLevel
				(Level.All);
		}

		private const int BlockSize = 1024;

		private const short Repl = 3;

		private const int NumDatanodes = 6;

		private static readonly Configuration DefaultConf = new HdfsConfiguration();

		private static readonly BlockStoragePolicySuite DefaultPolicies;

		private static readonly BlockStoragePolicy Hot;

		private static readonly BlockStoragePolicy Warm;

		private static readonly BlockStoragePolicy Cold;

		static TestStorageMover()
		{
			DefaultConf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			DefaultConf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1L);
			DefaultConf.SetLong(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 2L);
			DefaultConf.SetLong(DFSConfigKeys.DfsMoverMovedwinwidthKey, 2000L);
			DefaultPolicies = BlockStoragePolicySuite.CreateDefaultSuite();
			Hot = DefaultPolicies.GetPolicy(HdfsConstants.HotStoragePolicyName);
			Warm = DefaultPolicies.GetPolicy(HdfsConstants.WarmStoragePolicyName);
			Cold = DefaultPolicies.GetPolicy(HdfsConstants.ColdStoragePolicyName);
			TestBalancer.InitTestSetup();
			Dispatcher.SetDelayAfterErrors(1000L);
		}

		/// <summary>This scheme defines files/directories and their block storage policies.</summary>
		/// <remarks>
		/// This scheme defines files/directories and their block storage policies. It
		/// also defines snapshots.
		/// </remarks>
		internal class NamespaceScheme
		{
			internal readonly IList<Path> dirs;

			internal readonly IList<Path> files;

			internal readonly long fileSize;

			internal readonly IDictionary<Path, IList<string>> snapshotMap;

			internal readonly IDictionary<Path, BlockStoragePolicy> policyMap;

			internal NamespaceScheme(IList<Path> dirs, IList<Path> files, long fileSize, IDictionary
				<Path, IList<string>> snapshotMap, IDictionary<Path, BlockStoragePolicy> policyMap
				)
			{
				this.dirs = dirs == null ? Sharpen.Collections.EmptyList<Path>() : dirs;
				this.files = files == null ? Sharpen.Collections.EmptyList<Path>() : files;
				this.fileSize = fileSize;
				this.snapshotMap = snapshotMap == null ? Sharpen.Collections.EmptyMap<Path, IList
					<string>>() : snapshotMap;
				this.policyMap = policyMap;
			}

			/// <summary>Create files/directories/snapshots.</summary>
			/// <exception cref="System.Exception"/>
			internal virtual void Prepare(DistributedFileSystem dfs, short repl)
			{
				foreach (Path d in dirs)
				{
					dfs.Mkdirs(d);
				}
				foreach (Path file in files)
				{
					DFSTestUtil.CreateFile(dfs, file, fileSize, repl, 0L);
				}
				foreach (KeyValuePair<Path, IList<string>> entry in snapshotMap)
				{
					foreach (string snapshot in entry.Value)
					{
						SnapshotTestHelper.CreateSnapshot(dfs, entry.Key, snapshot);
					}
				}
			}

			/// <summary>Set storage policies according to the corresponding scheme.</summary>
			/// <exception cref="System.Exception"/>
			internal virtual void SetStoragePolicy(DistributedFileSystem dfs)
			{
				foreach (KeyValuePair<Path, BlockStoragePolicy> entry in policyMap)
				{
					dfs.SetStoragePolicy(entry.Key, entry.Value.GetName());
				}
			}
		}

		/// <summary>
		/// This scheme defines DataNodes and their storage, including storage types
		/// and remaining capacities.
		/// </summary>
		internal class ClusterScheme
		{
			internal readonly Configuration conf;

			internal readonly int numDataNodes;

			internal readonly short repl;

			internal readonly StorageType[][] storageTypes;

			internal readonly long[][] storageCapacities;

			internal ClusterScheme()
				: this(DefaultConf, NumDatanodes, Repl, GenStorageTypes(NumDatanodes), null)
			{
			}

			internal ClusterScheme(Configuration conf, int numDataNodes, short repl, StorageType
				[][] types, long[][] capacities)
			{
				Preconditions.CheckArgument(types == null || types.Length == numDataNodes);
				Preconditions.CheckArgument(capacities == null || capacities.Length == numDataNodes
					);
				this.conf = conf;
				this.numDataNodes = numDataNodes;
				this.repl = repl;
				this.storageTypes = types;
				this.storageCapacities = capacities;
			}
		}

		internal class MigrationTest
		{
			private readonly TestStorageMover.ClusterScheme clusterScheme;

			private readonly TestStorageMover.NamespaceScheme nsScheme;

			private readonly Configuration conf;

			private MiniDFSCluster cluster;

			private DistributedFileSystem dfs;

			private readonly BlockStoragePolicySuite policies;

			internal MigrationTest(TestStorageMover _enclosing, TestStorageMover.ClusterScheme
				 cScheme, TestStorageMover.NamespaceScheme nsScheme)
			{
				this._enclosing = _enclosing;
				this.clusterScheme = cScheme;
				this.nsScheme = nsScheme;
				this.conf = this.clusterScheme.conf;
				this.policies = TestStorageMover.DefaultPolicies;
			}

			/// <summary>
			/// Set up the cluster and start NameNode and DataNodes according to the
			/// corresponding scheme.
			/// </summary>
			/// <exception cref="System.Exception"/>
			internal virtual void SetupCluster()
			{
				this.cluster = new MiniDFSCluster.Builder(this.conf).NumDataNodes(this.clusterScheme
					.numDataNodes).StorageTypes(this.clusterScheme.storageTypes).StorageCapacities(this
					.clusterScheme.storageCapacities).Build();
				this.cluster.WaitActive();
				this.dfs = this.cluster.GetFileSystem();
			}

			/// <exception cref="System.Exception"/>
			private void RunBasicTest(bool shutdown)
			{
				this.SetupCluster();
				try
				{
					this.PrepareNamespace();
					this.Verify(true);
					this.SetStoragePolicy();
					this.Migrate();
					this.Verify(true);
				}
				finally
				{
					if (shutdown)
					{
						this.ShutdownCluster();
					}
				}
			}

			/// <exception cref="System.Exception"/>
			internal virtual void ShutdownCluster()
			{
				IOUtils.Cleanup(null, this.dfs);
				if (this.cluster != null)
				{
					this.cluster.Shutdown();
				}
			}

			/// <summary>
			/// Create files/directories and set their storage policies according to the
			/// corresponding scheme.
			/// </summary>
			/// <exception cref="System.Exception"/>
			internal virtual void PrepareNamespace()
			{
				this.nsScheme.Prepare(this.dfs, this.clusterScheme.repl);
			}

			/// <exception cref="System.Exception"/>
			internal virtual void SetStoragePolicy()
			{
				this.nsScheme.SetStoragePolicy(this.dfs);
			}

			/// <summary>Run the migration tool.</summary>
			/// <exception cref="System.Exception"/>
			internal virtual void Migrate()
			{
				this.RunMover();
				Sharpen.Thread.Sleep(5000);
			}

			// let the NN finish deletion
			/// <summary>Verify block locations after running the migration tool.</summary>
			/// <exception cref="System.Exception"/>
			internal virtual void Verify(bool verifyAll)
			{
				foreach (DataNode dn in this.cluster.GetDataNodes())
				{
					DataNodeTestUtils.TriggerBlockReport(dn);
				}
				if (verifyAll)
				{
					this.VerifyNamespace();
				}
			}

			/// <exception cref="System.Exception"/>
			private void RunMover()
			{
				ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(this.conf);
				IDictionary<URI, IList<Path>> nnMap = Maps.NewHashMap();
				foreach (URI nn in namenodes)
				{
					nnMap[nn] = null;
				}
				int result = Org.Apache.Hadoop.Hdfs.Server.Mover.Mover.Run(nnMap, this.conf);
				NUnit.Framework.Assert.AreEqual(ExitStatus.Success.GetExitCode(), result);
			}

			/// <exception cref="System.Exception"/>
			private void VerifyNamespace()
			{
				HdfsFileStatus status = this.dfs.GetClient().GetFileInfo("/");
				this.VerifyRecursively(null, status);
			}

			/// <exception cref="System.Exception"/>
			private void VerifyRecursively(Path parent, HdfsFileStatus status)
			{
				if (status.IsDir())
				{
					Path fullPath = parent == null ? new Path("/") : status.GetFullPath(parent);
					DirectoryListing children = this.dfs.GetClient().ListPaths(fullPath.ToString(), HdfsFileStatus
						.EmptyName, true);
					foreach (HdfsFileStatus child in children.GetPartialListing())
					{
						this.VerifyRecursively(fullPath, child);
					}
				}
				else
				{
					if (!status.IsSymlink())
					{
						// is file
						this.VerifyFile(parent, status, null);
					}
				}
			}

			/// <exception cref="System.Exception"/>
			internal virtual void VerifyFile(Path file, byte expectedPolicyId)
			{
				Path parent = file.GetParent();
				DirectoryListing children = this.dfs.GetClient().ListPaths(parent.ToString(), HdfsFileStatus
					.EmptyName, true);
				foreach (HdfsFileStatus child in children.GetPartialListing())
				{
					if (child.GetLocalName().Equals(file.GetName()))
					{
						this.VerifyFile(parent, child, expectedPolicyId);
						return;
					}
				}
				NUnit.Framework.Assert.Fail("File " + file + " not found.");
			}

			/// <exception cref="System.Exception"/>
			private void VerifyFile(Path parent, HdfsFileStatus status, byte expectedPolicyId
				)
			{
				HdfsLocatedFileStatus fileStatus = (HdfsLocatedFileStatus)status;
				byte policyId = fileStatus.GetStoragePolicy();
				BlockStoragePolicy policy = this.policies.GetPolicy(policyId);
				if (expectedPolicyId != null)
				{
					NUnit.Framework.Assert.AreEqual(unchecked((byte)expectedPolicyId), policy.GetId()
						);
				}
				IList<StorageType> types = policy.ChooseStorageTypes(status.GetReplication());
				foreach (LocatedBlock lb in fileStatus.GetBlockLocations().GetLocatedBlocks())
				{
					Mover.StorageTypeDiff diff = new Mover.StorageTypeDiff(types, lb.GetStorageTypes(
						));
					NUnit.Framework.Assert.IsTrue(fileStatus.GetFullName(parent.ToString()) + " with policy "
						 + policy + " has non-empty overlap: " + diff + ", the corresponding block is " 
						+ lb.GetBlock().GetLocalBlock(), diff.RemoveOverlap(true));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual TestStorageMover.Replication GetReplication(Path file)
			{
				return this.GetOrVerifyReplication(file, null);
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual TestStorageMover.Replication VerifyReplication(Path file, int expectedDiskCount
				, int expectedArchiveCount)
			{
				TestStorageMover.Replication r = new TestStorageMover.Replication();
				r.disk = expectedDiskCount;
				r.archive = expectedArchiveCount;
				return this.GetOrVerifyReplication(file, r);
			}

			/// <exception cref="System.IO.IOException"/>
			private TestStorageMover.Replication GetOrVerifyReplication(Path file, TestStorageMover.Replication
				 expected)
			{
				IList<LocatedBlock> lbs = this.dfs.GetClient().GetLocatedBlocks(file.ToString(), 
					0).GetLocatedBlocks();
				NUnit.Framework.Assert.AreEqual(1, lbs.Count);
				LocatedBlock lb = lbs[0];
				StringBuilder types = new StringBuilder();
				TestStorageMover.Replication r = new TestStorageMover.Replication();
				foreach (StorageType t in lb.GetStorageTypes())
				{
					types.Append(t).Append(", ");
					if (t == StorageType.Disk)
					{
						r.disk++;
					}
					else
					{
						if (t == StorageType.Archive)
						{
							r.archive++;
						}
						else
						{
							NUnit.Framework.Assert.Fail("Unexpected storage type " + t);
						}
					}
				}
				if (expected != null)
				{
					string s = "file = " + file + "\n  types = [" + types + "]";
					NUnit.Framework.Assert.AreEqual(s, expected, r);
				}
				return r;
			}

			private readonly TestStorageMover _enclosing;
		}

		internal class Replication
		{
			internal int disk;

			internal int archive;

			public override int GetHashCode()
			{
				return disk ^ archive;
			}

			public override bool Equals(object obj)
			{
				if (obj == this)
				{
					return true;
				}
				else
				{
					if (obj == null || !(obj is TestStorageMover.Replication))
					{
						return false;
					}
				}
				TestStorageMover.Replication that = (TestStorageMover.Replication)obj;
				return this.disk == that.disk && this.archive == that.archive;
			}

			public override string ToString()
			{
				return "[disk=" + disk + ", archive=" + archive + "]";
			}
		}

		private static StorageType[][] GenStorageTypes(int numDataNodes)
		{
			return GenStorageTypes(numDataNodes, 0, 0, 0);
		}

		private static StorageType[][] GenStorageTypes(int numDataNodes, int numAllDisk, 
			int numAllArchive)
		{
			return GenStorageTypes(numDataNodes, numAllDisk, numAllArchive, 0);
		}

		private static StorageType[][] GenStorageTypes(int numDataNodes, int numAllDisk, 
			int numAllArchive, int numRamDisk)
		{
			Preconditions.CheckArgument((numAllDisk + numAllArchive + numRamDisk) <= numDataNodes
				);
			StorageType[][] types = new StorageType[numDataNodes][];
			int i = 0;
			for (; i < numRamDisk; i++)
			{
				types[i] = new StorageType[] { StorageType.RamDisk, StorageType.Disk };
			}
			for (; i < numRamDisk + numAllDisk; i++)
			{
				types[i] = new StorageType[] { StorageType.Disk, StorageType.Disk };
			}
			for (; i < numRamDisk + numAllDisk + numAllArchive; i++)
			{
				types[i] = new StorageType[] { StorageType.Archive, StorageType.Archive };
			}
			for (; i < types.Length; i++)
			{
				types[i] = new StorageType[] { StorageType.Disk, StorageType.Archive };
			}
			return types;
		}

		private static long[][] GenCapacities(int nDatanodes, int numAllDisk, int numAllArchive
			, int numRamDisk, long diskCapacity, long archiveCapacity, long ramDiskCapacity)
		{
			long[][] capacities = new long[nDatanodes][];
			int i = 0;
			for (; i < numRamDisk; i++)
			{
				capacities[i] = new long[] { ramDiskCapacity, diskCapacity };
			}
			for (; i < numRamDisk + numAllDisk; i++)
			{
				capacities[i] = new long[] { diskCapacity, diskCapacity };
			}
			for (; i < numRamDisk + numAllDisk + numAllArchive; i++)
			{
				capacities[i] = new long[] { archiveCapacity, archiveCapacity };
			}
			for (; i < capacities.Length; i++)
			{
				capacities[i] = new long[] { diskCapacity, archiveCapacity };
			}
			return capacities;
		}

		private class PathPolicyMap
		{
			internal readonly IDictionary<Path, BlockStoragePolicy> map = Maps.NewHashMap();

			internal readonly Path hot = new Path("/hot");

			internal readonly Path warm = new Path("/warm");

			internal readonly Path cold = new Path("/cold");

			internal readonly IList<Path> files;

			internal PathPolicyMap(int filesPerDir)
			{
				map[hot] = Hot;
				map[warm] = Warm;
				map[cold] = Cold;
				files = new AList<Path>();
				foreach (Path dir in map.Keys)
				{
					for (int i = 0; i < filesPerDir; i++)
					{
						files.AddItem(new Path(dir, "file" + i));
					}
				}
			}

			internal virtual TestStorageMover.NamespaceScheme NewNamespaceScheme()
			{
				return new TestStorageMover.NamespaceScheme(Arrays.AsList(hot, warm, cold), files
					, BlockSize / 2, null, map);
			}

			/// <summary>
			/// Move hot files to warm and cold, warm files to hot and cold,
			/// and cold files to hot and warm.
			/// </summary>
			/// <exception cref="System.Exception"/>
			internal virtual void MoveAround(DistributedFileSystem dfs)
			{
				foreach (Path srcDir in map.Keys)
				{
					int i = 0;
					foreach (Path dstDir in map.Keys)
					{
						if (!srcDir.Equals(dstDir))
						{
							Path src = new Path(srcDir, "file" + i++);
							Path dst = new Path(dstDir, srcDir.GetName() + "2" + dstDir.GetName());
							Log.Info("rename " + src + " to " + dst);
							dfs.Rename(src, dst);
						}
					}
				}
			}
		}

		/// <summary>A normal case for Mover: move a file into archival storage</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMigrateFileToArchival()
		{
			Log.Info("testMigrateFileToArchival");
			Path foo = new Path("/foo");
			IDictionary<Path, BlockStoragePolicy> policyMap = Maps.NewHashMap();
			policyMap[foo] = Cold;
			TestStorageMover.NamespaceScheme nsScheme = new TestStorageMover.NamespaceScheme(
				null, Arrays.AsList(foo), 2 * BlockSize, null, policyMap);
			TestStorageMover.ClusterScheme clusterScheme = new TestStorageMover.ClusterScheme
				(DefaultConf, NumDatanodes, Repl, GenStorageTypes(NumDatanodes), null);
			new TestStorageMover.MigrationTest(this, clusterScheme, nsScheme).RunBasicTest(true
				);
		}

		/// <summary>Print a big banner in the test log to make debug easier.</summary>
		internal static void Banner(string @string)
		{
			Log.Info("\n\n\n\n================================================\n" + @string +
				 "\n" + "==================================================\n\n");
		}

		/// <summary>Run Mover with arguments specifying files and directories</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveSpecificPaths()
		{
			Log.Info("testMoveSpecificPaths");
			Path foo = new Path("/foo");
			Path barFile = new Path(foo, "bar");
			Path foo2 = new Path("/foo2");
			Path bar2File = new Path(foo2, "bar2");
			IDictionary<Path, BlockStoragePolicy> policyMap = Maps.NewHashMap();
			policyMap[foo] = Cold;
			policyMap[foo2] = Warm;
			TestStorageMover.NamespaceScheme nsScheme = new TestStorageMover.NamespaceScheme(
				Arrays.AsList(foo, foo2), Arrays.AsList(barFile, bar2File), BlockSize, null, policyMap
				);
			TestStorageMover.ClusterScheme clusterScheme = new TestStorageMover.ClusterScheme
				(DefaultConf, NumDatanodes, Repl, GenStorageTypes(NumDatanodes), null);
			TestStorageMover.MigrationTest test = new TestStorageMover.MigrationTest(this, clusterScheme
				, nsScheme);
			test.SetupCluster();
			try
			{
				test.PrepareNamespace();
				test.SetStoragePolicy();
				IDictionary<URI, IList<Path>> map = Mover.Cli.GetNameNodePathsToMove(test.conf, "-p"
					, "/foo/bar", "/foo2");
				int result = Org.Apache.Hadoop.Hdfs.Server.Mover.Mover.Run(map, test.conf);
				NUnit.Framework.Assert.AreEqual(ExitStatus.Success.GetExitCode(), result);
				Sharpen.Thread.Sleep(5000);
				test.Verify(true);
			}
			finally
			{
				test.ShutdownCluster();
			}
		}

		/// <summary>Move an open file into archival storage</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMigrateOpenFileToArchival()
		{
			Log.Info("testMigrateOpenFileToArchival");
			Path fooDir = new Path("/foo");
			IDictionary<Path, BlockStoragePolicy> policyMap = Maps.NewHashMap();
			policyMap[fooDir] = Cold;
			TestStorageMover.NamespaceScheme nsScheme = new TestStorageMover.NamespaceScheme(
				Arrays.AsList(fooDir), null, BlockSize, null, policyMap);
			TestStorageMover.ClusterScheme clusterScheme = new TestStorageMover.ClusterScheme
				(DefaultConf, NumDatanodes, Repl, GenStorageTypes(NumDatanodes), null);
			TestStorageMover.MigrationTest test = new TestStorageMover.MigrationTest(this, clusterScheme
				, nsScheme);
			test.SetupCluster();
			// create an open file
			Banner("writing to file /foo/bar");
			Path barFile = new Path(fooDir, "bar");
			DFSTestUtil.CreateFile(test.dfs, barFile, BlockSize, (short)1, 0L);
			FSDataOutputStream @out = test.dfs.Append(barFile);
			@out.WriteBytes("hello, ");
			((DFSOutputStream)@out.GetWrappedStream()).Hsync();
			try
			{
				Banner("start data migration");
				test.SetStoragePolicy();
				// set /foo to COLD
				test.Migrate();
				// make sure the under construction block has not been migrated
				LocatedBlocks lbs = test.dfs.GetClient().GetLocatedBlocks(barFile.ToString(), BlockSize
					);
				Log.Info("Locations: " + lbs);
				IList<LocatedBlock> blks = lbs.GetLocatedBlocks();
				NUnit.Framework.Assert.AreEqual(1, blks.Count);
				NUnit.Framework.Assert.AreEqual(1, blks[0].GetLocations().Length);
				Banner("finish the migration, continue writing");
				// make sure the writing can continue
				@out.WriteBytes("world!");
				((DFSOutputStream)@out.GetWrappedStream()).Hsync();
				IOUtils.Cleanup(Log, @out);
				lbs = test.dfs.GetClient().GetLocatedBlocks(barFile.ToString(), BlockSize);
				Log.Info("Locations: " + lbs);
				blks = lbs.GetLocatedBlocks();
				NUnit.Framework.Assert.AreEqual(1, blks.Count);
				NUnit.Framework.Assert.AreEqual(1, blks[0].GetLocations().Length);
				Banner("finish writing, starting reading");
				// check the content of /foo/bar
				FSDataInputStream @in = test.dfs.Open(barFile);
				byte[] buf = new byte[13];
				// read from offset 1024
				@in.ReadFully(BlockSize, buf, 0, buf.Length);
				IOUtils.Cleanup(Log, @in);
				NUnit.Framework.Assert.AreEqual("hello, world!", Sharpen.Runtime.GetStringForBytes
					(buf));
			}
			finally
			{
				test.ShutdownCluster();
			}
		}

		/// <summary>Test directories with Hot, Warm and Cold polices.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHotWarmColdDirs()
		{
			Log.Info("testHotWarmColdDirs");
			TestStorageMover.PathPolicyMap pathPolicyMap = new TestStorageMover.PathPolicyMap
				(3);
			TestStorageMover.NamespaceScheme nsScheme = pathPolicyMap.NewNamespaceScheme();
			TestStorageMover.ClusterScheme clusterScheme = new TestStorageMover.ClusterScheme
				();
			TestStorageMover.MigrationTest test = new TestStorageMover.MigrationTest(this, clusterScheme
				, nsScheme);
			try
			{
				test.RunBasicTest(false);
				pathPolicyMap.MoveAround(test.dfs);
				test.Migrate();
				test.Verify(true);
			}
			finally
			{
				test.ShutdownCluster();
			}
		}

		/// <exception cref="System.Exception"/>
		private void WaitForAllReplicas(int expectedReplicaNum, Path file, DistributedFileSystem
			 dfs)
		{
			for (int i = 0; i < 5; i++)
			{
				LocatedBlocks lbs = dfs.GetClient().GetLocatedBlocks(file.ToString(), 0, BlockSize
					);
				LocatedBlock lb = lbs.Get(0);
				if (lb.GetLocations().Length >= expectedReplicaNum)
				{
					return;
				}
				else
				{
					Sharpen.Thread.Sleep(1000);
				}
			}
		}

		private void SetVolumeFull(DataNode dn, StorageType type)
		{
			IList<FsVolumeSpi> volumes = dn.GetFSDataset().GetVolumes();
			foreach (FsVolumeSpi v in volumes)
			{
				FsVolumeImpl volume = (FsVolumeImpl)v;
				if (volume.GetStorageType() == type)
				{
					Log.Info("setCapacity to 0 for [" + volume.GetStorageType() + "]" + volume.GetStorageID
						());
					volume.SetCapacityForTesting(0);
				}
			}
		}

		/// <summary>Test DISK is running out of spaces.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoSpaceDisk()
		{
			Log.Info("testNoSpaceDisk");
			TestStorageMover.PathPolicyMap pathPolicyMap = new TestStorageMover.PathPolicyMap
				(0);
			TestStorageMover.NamespaceScheme nsScheme = pathPolicyMap.NewNamespaceScheme();
			Configuration conf = new Configuration(DefaultConf);
			TestStorageMover.ClusterScheme clusterScheme = new TestStorageMover.ClusterScheme
				(conf, NumDatanodes, Repl, GenStorageTypes(NumDatanodes), null);
			TestStorageMover.MigrationTest test = new TestStorageMover.MigrationTest(this, clusterScheme
				, nsScheme);
			try
			{
				test.RunBasicTest(false);
				// create 2 hot files with replication 3
				short replication = 3;
				for (int i = 0; i < 2; i++)
				{
					Path p = new Path(pathPolicyMap.hot, "file" + i);
					DFSTestUtil.CreateFile(test.dfs, p, BlockSize, replication, 0L);
					WaitForAllReplicas(replication, p, test.dfs);
				}
				// set all the DISK volume to full
				foreach (DataNode dn in test.cluster.GetDataNodes())
				{
					SetVolumeFull(dn, StorageType.Disk);
					DataNodeTestUtils.TriggerHeartbeat(dn);
				}
				// test increasing replication.  Since DISK is full,
				// new replicas should be stored in ARCHIVE as a fallback storage.
				Path file0 = new Path(pathPolicyMap.hot, "file0");
				TestStorageMover.Replication r = test.GetReplication(file0);
				short newReplication = (short)5;
				test.dfs.SetReplication(file0, newReplication);
				Sharpen.Thread.Sleep(10000);
				test.VerifyReplication(file0, r.disk, newReplication - r.disk);
				// test creating a cold file and then increase replication
				Path p_1 = new Path(pathPolicyMap.cold, "foo");
				DFSTestUtil.CreateFile(test.dfs, p_1, BlockSize, replication, 0L);
				test.VerifyReplication(p_1, 0, replication);
				test.dfs.SetReplication(p_1, newReplication);
				Sharpen.Thread.Sleep(10000);
				test.VerifyReplication(p_1, 0, newReplication);
				//test move a hot file to warm
				Path file1 = new Path(pathPolicyMap.hot, "file1");
				test.dfs.Rename(file1, pathPolicyMap.warm);
				test.Migrate();
				test.VerifyFile(new Path(pathPolicyMap.warm, "file1"), Warm.GetId());
			}
			finally
			{
				test.ShutdownCluster();
			}
		}

		/// <summary>Test ARCHIVE is running out of spaces.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoSpaceArchive()
		{
			Log.Info("testNoSpaceArchive");
			TestStorageMover.PathPolicyMap pathPolicyMap = new TestStorageMover.PathPolicyMap
				(0);
			TestStorageMover.NamespaceScheme nsScheme = pathPolicyMap.NewNamespaceScheme();
			TestStorageMover.ClusterScheme clusterScheme = new TestStorageMover.ClusterScheme
				(DefaultConf, NumDatanodes, Repl, GenStorageTypes(NumDatanodes), null);
			TestStorageMover.MigrationTest test = new TestStorageMover.MigrationTest(this, clusterScheme
				, nsScheme);
			try
			{
				test.RunBasicTest(false);
				// create 2 hot files with replication 3
				short replication = 3;
				for (int i = 0; i < 2; i++)
				{
					Path p = new Path(pathPolicyMap.cold, "file" + i);
					DFSTestUtil.CreateFile(test.dfs, p, BlockSize, replication, 0L);
					WaitForAllReplicas(replication, p, test.dfs);
				}
				// set all the ARCHIVE volume to full
				foreach (DataNode dn in test.cluster.GetDataNodes())
				{
					SetVolumeFull(dn, StorageType.Archive);
					DataNodeTestUtils.TriggerHeartbeat(dn);
				}
				{
					// test increasing replication but new replicas cannot be created
					// since no more ARCHIVE space.
					Path file0 = new Path(pathPolicyMap.cold, "file0");
					TestStorageMover.Replication r = test.GetReplication(file0);
					NUnit.Framework.Assert.AreEqual(0, r.disk);
					short newReplication = (short)5;
					test.dfs.SetReplication(file0, newReplication);
					Sharpen.Thread.Sleep(10000);
					test.VerifyReplication(file0, 0, r.archive);
				}
				{
					// test creating a hot file
					Path p = new Path(pathPolicyMap.hot, "foo");
					DFSTestUtil.CreateFile(test.dfs, p, BlockSize, (short)3, 0L);
				}
				{
					//test move a cold file to warm
					Path file1 = new Path(pathPolicyMap.cold, "file1");
					test.dfs.Rename(file1, pathPolicyMap.warm);
					test.Migrate();
					test.Verify(true);
				}
			}
			finally
			{
				test.ShutdownCluster();
			}
		}
	}
}
