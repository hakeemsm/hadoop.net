using System;
using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	public abstract class LazyPersistTestCase
	{
		static LazyPersistTestCase()
		{
			DFSTestUtil.SetNameNodeLogLevel(Level.All);
			GenericTestUtils.SetLogLevel(FsDatasetImpl.Log, Level.All);
		}

		protected internal const int BlockSize = 5 * 1024 * 1024;

		protected internal const int BufferLength = 4096;

		protected internal const int EvictionLowWatermark = 1;

		private const long HeartbeatIntervalSec = 1;

		private const int HeartbeatRecheckIntervalMsec = 500;

		private const string JmxRamDiskMetricsPattern = "^RamDisk";

		private const string JmxServiceName = "DataNode";

		protected internal const int LazyWriteFileScrubberIntervalSec = 3;

		protected internal const int LazyWriterIntervalSec = 1;

		protected internal static readonly Log Log = LogFactory.GetLog(typeof(LazyPersistTestCase
			));

		protected internal const short ReplFactor = 1;

		protected internal MiniDFSCluster cluster;

		protected internal DistributedFileSystem fs;

		protected internal DFSClient client;

		protected internal JMXGet jmx;

		protected internal TemporarySocketDirectory sockDir;

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void ShutDownCluster()
		{
			// Dump all RamDisk JMX metrics before shutdown the cluster
			PrintRamDiskJMXMetrics();
			if (fs != null)
			{
				fs.Close();
				fs = null;
				client = null;
			}
			if (cluster != null)
			{
				cluster.ShutdownDataNodes();
				cluster.Shutdown();
				cluster = null;
			}
			if (jmx != null)
			{
				jmx = null;
			}
			IOUtils.CloseQuietly(sockDir);
			sockDir = null;
		}

		[Rule]
		public Timeout timeout = new Timeout(300000);

		/// <exception cref="System.IO.IOException"/>
		protected internal LocatedBlocks EnsureFileReplicasOnStorageType(Path path, StorageType
			 storageType)
		{
			// Ensure that returned block locations returned are correct!
			Log.Info("Ensure path: " + path + " is on StorageType: " + storageType);
			Assert.AssertThat(fs.Exists(path), IS.Is(true));
			long fileLength = client.GetFileInfo(path.ToString()).GetLen();
			LocatedBlocks locatedBlocks = client.GetLocatedBlocks(path.ToString(), 0, fileLength
				);
			foreach (LocatedBlock locatedBlock in locatedBlocks.GetLocatedBlocks())
			{
				Assert.AssertThat(locatedBlock.GetStorageTypes()[0], IS.Is(storageType));
			}
			return locatedBlocks;
		}

		/// <summary>Make sure at least one non-transient volume has a saved copy of the replica.
		/// 	</summary>
		/// <remarks>
		/// Make sure at least one non-transient volume has a saved copy of the replica.
		/// An infinite loop is used to ensure the async lazy persist tasks are completely
		/// done before verification. Caller of ensureLazyPersistBlocksAreSaved expects
		/// either a successful pass or timeout failure.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal void EnsureLazyPersistBlocksAreSaved(LocatedBlocks locatedBlocks
			)
		{
			string bpid = cluster.GetNamesystem().GetBlockPoolId();
			IList<FsVolumeSpi> volumes = cluster.GetDataNodes()[0].GetFSDataset().GetVolumes(
				);
			ICollection<long> persistedBlockIds = new HashSet<long>();
			while (persistedBlockIds.Count < locatedBlocks.GetLocatedBlocks().Count)
			{
				// Take 1 second sleep before each verification iteration
				Sharpen.Thread.Sleep(1000);
				foreach (LocatedBlock lb in locatedBlocks.GetLocatedBlocks())
				{
					foreach (FsVolumeSpi v in volumes)
					{
						if (v.IsTransientStorage())
						{
							continue;
						}
						FsVolumeImpl volume = (FsVolumeImpl)v;
						FilePath lazyPersistDir = volume.GetBlockPoolSlice(bpid).GetLazypersistDir();
						long blockId = lb.GetBlock().GetBlockId();
						FilePath targetDir = DatanodeUtil.IdToBlockDir(lazyPersistDir, blockId);
						FilePath blockFile = new FilePath(targetDir, lb.GetBlock().GetBlockName());
						if (blockFile.Exists())
						{
							// Found a persisted copy for this block and added to the Set
							persistedBlockIds.AddItem(blockId);
						}
					}
				}
			}
			// We should have found a persisted copy for each located block.
			Assert.AssertThat(persistedBlockIds.Count, IS.Is(locatedBlocks.GetLocatedBlocks()
				.Count));
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal void MakeRandomTestFile(Path path, long length, bool isLazyPersist
			, long seed)
		{
			DFSTestUtil.CreateFile(fs, path, isLazyPersist, BufferLength, length, BlockSize, 
				ReplFactor, seed, true);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal void MakeTestFile(Path path, long length, bool isLazyPersist)
		{
			EnumSet<CreateFlag> createFlags = EnumSet.Of(CreateFlag.Create);
			if (isLazyPersist)
			{
				createFlags.AddItem(CreateFlag.LazyPersist);
			}
			FSDataOutputStream fos = null;
			try
			{
				fos = fs.Create(path, FsPermission.GetFileDefault(), createFlags, BufferLength, ReplFactor
					, BlockSize, null);
				// Allocate a block.
				byte[] buffer = new byte[BufferLength];
				for (int bytesWritten = 0; bytesWritten < length; )
				{
					fos.Write(buffer, 0, buffer.Length);
					bytesWritten += buffer.Length;
				}
				if (length > 0)
				{
					fos.Hsync();
				}
			}
			finally
			{
				IOUtils.CloseQuietly(fos);
			}
		}

		/// <summary>
		/// If ramDiskStorageLimit is &gt;=0, then RAM_DISK capacity is artificially
		/// capped.
		/// </summary>
		/// <remarks>
		/// If ramDiskStorageLimit is &gt;=0, then RAM_DISK capacity is artificially
		/// capped. If ramDiskStorageLimit &lt; 0 then it is ignored.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal void StartUpCluster(bool hasTransientStorage, int ramDiskReplicaCapacity
			, bool useSCR, bool useLegacyBlockReaderLocal)
		{
			Configuration conf = new Configuration();
			conf.SetLong(DfsBlockSizeKey, BlockSize);
			conf.SetInt(DfsNamenodeLazyPersistFileScrubIntervalSec, LazyWriteFileScrubberIntervalSec
				);
			conf.SetLong(DfsHeartbeatIntervalKey, HeartbeatIntervalSec);
			conf.SetInt(DfsNamenodeHeartbeatRecheckIntervalKey, HeartbeatRecheckIntervalMsec);
			conf.SetInt(DfsDatanodeLazyWriterIntervalSec, LazyWriterIntervalSec);
			conf.SetInt(DfsDatanodeRamDiskLowWatermarkBytes, EvictionLowWatermark * BlockSize
				);
			if (useSCR)
			{
				conf.SetBoolean(DfsClientReadShortcircuitKey, true);
				// Do not share a client context across tests.
				conf.Set(DfsClientContext, UUID.RandomUUID().ToString());
				if (useLegacyBlockReaderLocal)
				{
					conf.SetBoolean(DfsClientUseLegacyBlockreaderlocal, true);
					conf.Set(DfsBlockLocalPathAccessUserKey, UserGroupInformation.GetCurrentUser().GetShortUserName
						());
				}
				else
				{
					sockDir = new TemporarySocketDirectory();
					conf.Set(DfsDomainSocketPathKey, new FilePath(sockDir.GetDir(), this.GetType().Name
						 + "._PORT.sock").GetAbsolutePath());
				}
			}
			long[] capacities = null;
			if (hasTransientStorage && ramDiskReplicaCapacity >= 0)
			{
				// Convert replica count to byte count, add some delta for .meta and
				// VERSION files.
				long ramDiskStorageLimit = ((long)ramDiskReplicaCapacity * BlockSize) + (BlockSize
					 - 1);
				capacities = new long[] { ramDiskStorageLimit, -1 };
			}
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(ReplFactor).StorageCapacities
				(capacities).StorageTypes(hasTransientStorage ? new StorageType[] { StorageType.
				RamDisk, StorageType.Default } : null).Build();
			fs = cluster.GetFileSystem();
			client = fs.GetClient();
			try
			{
				jmx = InitJMX();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("Failed initialize JMX for testing: " + e);
			}
			Log.Info("Cluster startup complete");
		}

		/// <summary>
		/// If ramDiskStorageLimit is &gt;=0, then RAM_DISK capacity is artificially
		/// capped.
		/// </summary>
		/// <remarks>
		/// If ramDiskStorageLimit is &gt;=0, then RAM_DISK capacity is artificially
		/// capped. If ramDiskStorageLimit &lt; 0 then it is ignored.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal void StartUpCluster(int numDataNodes, StorageType[] storageTypes
			, long ramDiskStorageLimit, bool useSCR)
		{
			Configuration conf = new Configuration();
			conf.SetLong(DfsBlockSizeKey, BlockSize);
			conf.SetInt(DfsNamenodeLazyPersistFileScrubIntervalSec, LazyWriteFileScrubberIntervalSec
				);
			conf.SetLong(DfsHeartbeatIntervalKey, HeartbeatIntervalSec);
			conf.SetInt(DfsNamenodeHeartbeatRecheckIntervalKey, HeartbeatRecheckIntervalMsec);
			conf.SetInt(DfsDatanodeLazyWriterIntervalSec, LazyWriterIntervalSec);
			if (useSCR)
			{
				conf.SetBoolean(DfsClientReadShortcircuitKey, useSCR);
				conf.Set(DfsClientContext, UUID.RandomUUID().ToString());
				sockDir = new TemporarySocketDirectory();
				conf.Set(DfsDomainSocketPathKey, new FilePath(sockDir.GetDir(), this.GetType().Name
					 + "._PORT.sock").GetAbsolutePath());
				conf.Set(DfsBlockLocalPathAccessUserKey, UserGroupInformation.GetCurrentUser().GetShortUserName
					());
			}
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes).StorageTypes
				(storageTypes != null ? storageTypes : new StorageType[] { StorageType.Default, 
				StorageType.Default }).Build();
			fs = cluster.GetFileSystem();
			client = fs.GetClient();
			// Artificially cap the storage capacity of the RAM_DISK volume.
			if (ramDiskStorageLimit >= 0)
			{
				IList<FsVolumeSpi> volumes = cluster.GetDataNodes()[0].GetFSDataset().GetVolumes(
					);
				foreach (FsVolumeSpi volume in volumes)
				{
					if (volume.GetStorageType() == StorageType.RamDisk)
					{
						((FsVolumeImpl)volume).SetCapacityForTesting(ramDiskStorageLimit);
					}
				}
			}
			Log.Info("Cluster startup complete");
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal void StartUpCluster(bool hasTransientStorage, int ramDiskReplicaCapacity
			)
		{
			StartUpCluster(hasTransientStorage, ramDiskReplicaCapacity, false, false);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal void TriggerBlockReport()
		{
			// Trigger block report to NN
			DataNodeTestUtils.TriggerBlockReport(cluster.GetDataNodes()[0]);
			Sharpen.Thread.Sleep(10 * 1000);
		}

		protected internal bool VerifyBlockDeletedFromDir(FilePath dir, LocatedBlocks locatedBlocks
			)
		{
			foreach (LocatedBlock lb in locatedBlocks.GetLocatedBlocks())
			{
				FilePath targetDir = DatanodeUtil.IdToBlockDir(dir, lb.GetBlock().GetBlockId());
				FilePath blockFile = new FilePath(targetDir, lb.GetBlock().GetBlockName());
				if (blockFile.Exists())
				{
					Log.Warn("blockFile: " + blockFile.GetAbsolutePath() + " exists after deletion.");
					return false;
				}
				FilePath metaFile = new FilePath(targetDir, DatanodeUtil.GetMetaName(lb.GetBlock(
					).GetBlockName(), lb.GetBlock().GetGenerationStamp()));
				if (metaFile.Exists())
				{
					Log.Warn("metaFile: " + metaFile.GetAbsolutePath() + " exists after deletion.");
					return false;
				}
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal bool VerifyDeletedBlocks(LocatedBlocks locatedBlocks)
		{
			Log.Info("Verifying replica has no saved copy after deletion.");
			TriggerBlockReport();
			while (DataNodeTestUtils.GetPendingAsyncDeletions(cluster.GetDataNodes()[0]) > 0L
				)
			{
				Sharpen.Thread.Sleep(1000);
			}
			string bpid = cluster.GetNamesystem().GetBlockPoolId();
			IList<FsVolumeSpi> volumes = cluster.GetDataNodes()[0].GetFSDataset().GetVolumes(
				);
			// Make sure deleted replica does not have a copy on either finalized dir of
			// transient volume or finalized dir of non-transient volume
			foreach (FsVolumeSpi v in volumes)
			{
				FsVolumeImpl volume = (FsVolumeImpl)v;
				FilePath targetDir = (v.IsTransientStorage()) ? volume.GetBlockPoolSlice(bpid).GetFinalizedDir
					() : volume.GetBlockPoolSlice(bpid).GetLazypersistDir();
				if (VerifyBlockDeletedFromDir(targetDir, locatedBlocks) == false)
				{
					return false;
				}
			}
			return true;
		}

		/// <exception cref="System.Exception"/>
		protected internal void VerifyRamDiskJMXMetric(string metricName, long expectedValue
			)
		{
			NUnit.Framework.Assert.AreEqual(expectedValue, System.Convert.ToInt32(jmx.GetValue
				(metricName)));
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal bool VerifyReadRandomFile(Path path, int fileLength, int seed)
		{
			byte[] contents = DFSTestUtil.ReadFileBuffer(fs, path);
			byte[] expected = DFSTestUtil.CalculateFileContentsFromSeed(seed, fileLength);
			return Arrays.Equals(contents, expected);
		}

		/// <exception cref="System.Exception"/>
		private JMXGet InitJMX()
		{
			JMXGet jmx = new JMXGet();
			jmx.SetService(JmxServiceName);
			jmx.Init();
			return jmx;
		}

		private void PrintRamDiskJMXMetrics()
		{
			try
			{
				if (jmx != null)
				{
					jmx.PrintAllMatchedAttributes(JmxRamDiskMetricsPattern);
				}
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
		}
	}
}
