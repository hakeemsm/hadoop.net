using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	public class TestFsDatasetImpl
	{
		private static readonly string BaseDir = new FileSystemTestHelper().GetTestRootDir
			();

		private const int NumInitVolumes = 2;

		private const string ClusterId = "cluser-id";

		private static readonly string[] BlockPoolIds = new string[] { "bpid-0", "bpid-1"
			 };

		private static readonly DataStorage dsForStorageUuid = new DataStorage(new StorageInfo
			(HdfsServerConstants.NodeType.DataNode));

		private Configuration conf;

		private DataNode datanode;

		private DataStorage storage;

		private FsDatasetImpl dataset;

		private const string Blockpool = "BP-TEST";

		// Use to generate storageUuid
		private static Storage.StorageDirectory CreateStorageDirectory(FilePath root)
		{
			Storage.StorageDirectory sd = new Storage.StorageDirectory(root);
			dsForStorageUuid.CreateStorageID(sd, false);
			return sd;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateStorageDirs(DataStorage storage, Configuration conf, int
			 numDirs)
		{
			IList<Storage.StorageDirectory> dirs = new AList<Storage.StorageDirectory>();
			IList<string> dirStrings = new AList<string>();
			for (int i = 0; i < numDirs; i++)
			{
				FilePath loc = new FilePath(BaseDir + "/data" + i);
				dirStrings.AddItem(new Path(loc.ToString()).ToUri().ToString());
				loc.Mkdirs();
				dirs.AddItem(CreateStorageDirectory(loc));
				Org.Mockito.Mockito.When(storage.GetStorageDir(i)).ThenReturn(dirs[i]);
			}
			string dataDir = StringUtils.Join(",", dirStrings);
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, dataDir);
			Org.Mockito.Mockito.When(storage.DirIterator()).ThenReturn(dirs.GetEnumerator());
			Org.Mockito.Mockito.When(storage.GetNumStorageDirs()).ThenReturn(numDirs);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			datanode = Org.Mockito.Mockito.Mock<DataNode>();
			storage = Org.Mockito.Mockito.Mock<DataStorage>();
			this.conf = new Configuration();
			this.conf.SetLong(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, 0);
			DNConf dnConf = new DNConf(conf);
			Org.Mockito.Mockito.When(datanode.GetConf()).ThenReturn(conf);
			Org.Mockito.Mockito.When(datanode.GetDnConf()).ThenReturn(dnConf);
			BlockScanner disabledBlockScanner = new BlockScanner(datanode, conf);
			Org.Mockito.Mockito.When(datanode.GetBlockScanner()).ThenReturn(disabledBlockScanner
				);
			ShortCircuitRegistry shortCircuitRegistry = new ShortCircuitRegistry(conf);
			Org.Mockito.Mockito.When(datanode.GetShortCircuitRegistry()).ThenReturn(shortCircuitRegistry
				);
			CreateStorageDirs(storage, conf, NumInitVolumes);
			dataset = new FsDatasetImpl(datanode, storage, conf);
			foreach (string bpid in BlockPoolIds)
			{
				dataset.AddBlockPool(bpid, conf);
			}
			NUnit.Framework.Assert.AreEqual(NumInitVolumes, dataset.GetVolumes().Count);
			NUnit.Framework.Assert.AreEqual(0, dataset.GetNumFailedVolumes());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAddVolumes()
		{
			int numNewVolumes = 3;
			int numExistingVolumes = dataset.GetVolumes().Count;
			int totalVolumes = numNewVolumes + numExistingVolumes;
			ICollection<string> expectedVolumes = new HashSet<string>();
			IList<NamespaceInfo> nsInfos = Lists.NewArrayList();
			foreach (string bpid in BlockPoolIds)
			{
				nsInfos.AddItem(new NamespaceInfo(0, ClusterId, bpid, 1));
			}
			for (int i = 0; i < numNewVolumes; i++)
			{
				string path = BaseDir + "/newData" + i;
				string pathUri = new Path(path).ToUri().ToString();
				expectedVolumes.AddItem(new FilePath(pathUri).ToString());
				StorageLocation loc = StorageLocation.Parse(pathUri);
				Storage.StorageDirectory sd = CreateStorageDirectory(new FilePath(path));
				DataStorage.VolumeBuilder builder = new DataStorage.VolumeBuilder(storage, sd);
				Org.Mockito.Mockito.When(storage.PrepareVolume(Matchers.Eq(datanode), Matchers.Eq
					(loc.GetFile()), Matchers.AnyListOf<NamespaceInfo>())).ThenReturn(builder);
				dataset.AddVolume(loc, nsInfos);
			}
			NUnit.Framework.Assert.AreEqual(totalVolumes, dataset.GetVolumes().Count);
			NUnit.Framework.Assert.AreEqual(totalVolumes, dataset.storageMap.Count);
			ICollection<string> actualVolumes = new HashSet<string>();
			for (int i_1 = 0; i_1 < numNewVolumes; i_1++)
			{
				actualVolumes.AddItem(dataset.GetVolumes()[numExistingVolumes + i_1].GetBasePath(
					));
			}
			NUnit.Framework.Assert.AreEqual(actualVolumes.Count, expectedVolumes.Count);
			NUnit.Framework.Assert.IsTrue(actualVolumes.ContainsAll(expectedVolumes));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRemoveVolumes()
		{
			// Feed FsDataset with block metadata.
			int NumBlocks = 100;
			for (int i = 0; i < NumBlocks; i++)
			{
				string bpid = BlockPoolIds[NumBlocks % BlockPoolIds.Length];
				ExtendedBlock eb = new ExtendedBlock(bpid, i);
				using (ReplicaHandler replica = dataset.CreateRbw(StorageType.Default, eb, false))
				{
				}
			}
			string[] dataDirs = conf.Get(DFSConfigKeys.DfsDatanodeDataDirKey).Split(",");
			string volumePathToRemove = dataDirs[0];
			ICollection<FilePath> volumesToRemove = new HashSet<FilePath>();
			volumesToRemove.AddItem(StorageLocation.Parse(volumePathToRemove).GetFile());
			dataset.RemoveVolumes(volumesToRemove, true);
			int expectedNumVolumes = dataDirs.Length - 1;
			NUnit.Framework.Assert.AreEqual("The volume has been removed from the volumeList."
				, expectedNumVolumes, dataset.GetVolumes().Count);
			NUnit.Framework.Assert.AreEqual("The volume has been removed from the storageMap."
				, expectedNumVolumes, dataset.storageMap.Count);
			try
			{
				dataset.asyncDiskService.Execute(volumesToRemove.GetEnumerator().Next(), new _Runnable_220
					());
				NUnit.Framework.Assert.Fail("Expect RuntimeException: the volume has been removed from the "
					 + "AsyncDiskService.");
			}
			catch (RuntimeException e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot find root", e);
			}
			int totalNumReplicas = 0;
			foreach (string bpid_1 in dataset.volumeMap.GetBlockPoolList())
			{
				totalNumReplicas += dataset.volumeMap.Size(bpid_1);
			}
			NUnit.Framework.Assert.AreEqual("The replica infos on this volume has been removed from the "
				 + "volumeMap.", NumBlocks / NumInitVolumes, totalNumReplicas);
		}

		private sealed class _Runnable_220 : Runnable
		{
			public _Runnable_220()
			{
			}

			public void Run()
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRemoveNewlyAddedVolume()
		{
			int numExistingVolumes = dataset.GetVolumes().Count;
			IList<NamespaceInfo> nsInfos = new AList<NamespaceInfo>();
			foreach (string bpid in BlockPoolIds)
			{
				nsInfos.AddItem(new NamespaceInfo(0, ClusterId, bpid, 1));
			}
			string newVolumePath = BaseDir + "/newVolumeToRemoveLater";
			StorageLocation loc = StorageLocation.Parse(newVolumePath);
			Storage.StorageDirectory sd = CreateStorageDirectory(new FilePath(newVolumePath));
			DataStorage.VolumeBuilder builder = new DataStorage.VolumeBuilder(storage, sd);
			Org.Mockito.Mockito.When(storage.PrepareVolume(Matchers.Eq(datanode), Matchers.Eq
				(loc.GetFile()), Matchers.AnyListOf<NamespaceInfo>())).ThenReturn(builder);
			dataset.AddVolume(loc, nsInfos);
			NUnit.Framework.Assert.AreEqual(numExistingVolumes + 1, dataset.GetVolumes().Count
				);
			Org.Mockito.Mockito.When(storage.GetNumStorageDirs()).ThenReturn(numExistingVolumes
				 + 1);
			Org.Mockito.Mockito.When(storage.GetStorageDir(numExistingVolumes)).ThenReturn(sd
				);
			ICollection<FilePath> volumesToRemove = new HashSet<FilePath>();
			volumesToRemove.AddItem(loc.GetFile());
			dataset.RemoveVolumes(volumesToRemove, true);
			NUnit.Framework.Assert.AreEqual(numExistingVolumes, dataset.GetVolumes().Count);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestChangeVolumeWithRunningCheckDirs()
		{
			RoundRobinVolumeChoosingPolicy<FsVolumeImpl> blockChooser = new RoundRobinVolumeChoosingPolicy
				<FsVolumeImpl>();
			conf.SetLong(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, -1);
			BlockScanner blockScanner = new BlockScanner(datanode, conf);
			FsVolumeList volumeList = new FsVolumeList(Sharpen.Collections.EmptyList<VolumeFailureInfo
				>(), blockScanner, blockChooser);
			IList<FsVolumeImpl> oldVolumes = new AList<FsVolumeImpl>();
			// Initialize FsVolumeList with 5 mock volumes.
			int NumVolumes = 5;
			for (int i = 0; i < NumVolumes; i++)
			{
				FsVolumeImpl volume = Org.Mockito.Mockito.Mock<FsVolumeImpl>();
				oldVolumes.AddItem(volume);
				Org.Mockito.Mockito.When(volume.GetBasePath()).ThenReturn("data" + i);
				FsVolumeReference @ref = Org.Mockito.Mockito.Mock<FsVolumeReference>();
				Org.Mockito.Mockito.When(@ref.GetVolume()).ThenReturn(volume);
				volumeList.AddVolume(@ref);
			}
			// When call checkDirs() on the 2nd volume, anther "thread" removes the 5th
			// volume and add another volume. It does not affect checkDirs() running.
			FsVolumeImpl newVolume = Org.Mockito.Mockito.Mock<FsVolumeImpl>();
			FsVolumeReference newRef = Org.Mockito.Mockito.Mock<FsVolumeReference>();
			Org.Mockito.Mockito.When(newRef.GetVolume()).ThenReturn(newVolume);
			Org.Mockito.Mockito.When(newVolume.GetBasePath()).ThenReturn("data4");
			FsVolumeImpl blockedVolume = volumeList.GetVolumes()[1];
			Org.Mockito.Mockito.DoAnswer(new _Answer_295(volumeList, newRef)).When(blockedVolume
				).CheckDirs();
			FsVolumeImpl brokenVolume = volumeList.GetVolumes()[2];
			Org.Mockito.Mockito.DoThrow(new DiskChecker.DiskErrorException("broken")).When(brokenVolume
				).CheckDirs();
			volumeList.CheckDirs();
			// Since FsVolumeImpl#checkDirs() get a snapshot of the list of volumes
			// before running removeVolume(), it is supposed to run checkDirs() on all
			// the old volumes.
			foreach (FsVolumeImpl volume_1 in oldVolumes)
			{
				Org.Mockito.Mockito.Verify(volume_1).CheckDirs();
			}
			// New volume is not visible to checkDirs() process.
			Org.Mockito.Mockito.Verify(newVolume, Org.Mockito.Mockito.Never()).CheckDirs();
			NUnit.Framework.Assert.IsTrue(volumeList.GetVolumes().Contains(newVolume));
			NUnit.Framework.Assert.IsFalse(volumeList.GetVolumes().Contains(brokenVolume));
			NUnit.Framework.Assert.AreEqual(NumVolumes - 1, volumeList.GetVolumes().Count);
		}

		private sealed class _Answer_295 : Answer
		{
			public _Answer_295(FsVolumeList volumeList, FsVolumeReference newRef)
			{
				this.volumeList = volumeList;
				this.newRef = newRef;
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocationOnMock)
			{
				volumeList.RemoveVolume(new FilePath("data4"), false);
				volumeList.AddVolume(newRef);
				return null;
			}

			private readonly FsVolumeList volumeList;

			private readonly FsVolumeReference newRef;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAddVolumeFailureReleasesInUseLock()
		{
			FsDatasetImpl spyDataset = Org.Mockito.Mockito.Spy(dataset);
			FsVolumeImpl mockVolume = Org.Mockito.Mockito.Mock<FsVolumeImpl>();
			FilePath badDir = new FilePath(BaseDir, "bad");
			badDir.Mkdirs();
			Org.Mockito.Mockito.DoReturn(mockVolume).When(spyDataset).CreateFsVolume(Matchers.AnyString
				(), Matchers.Any<FilePath>(), Matchers.Any<StorageType>());
			Org.Mockito.Mockito.DoThrow(new IOException("Failed to getVolumeMap()")).When(mockVolume
				).GetVolumeMap(Matchers.AnyString(), Matchers.Any<ReplicaMap>(), Matchers.Any<RamDiskReplicaLruTracker
				>());
			Storage.StorageDirectory sd = CreateStorageDirectory(badDir);
			sd.Lock();
			DataStorage.VolumeBuilder builder = new DataStorage.VolumeBuilder(storage, sd);
			Org.Mockito.Mockito.When(storage.PrepareVolume(Matchers.Eq(datanode), Matchers.Eq
				(badDir.GetAbsoluteFile()), Matchers.Any<IList<NamespaceInfo>>())).ThenReturn(builder
				);
			StorageLocation location = StorageLocation.Parse(badDir.ToString());
			IList<NamespaceInfo> nsInfos = Lists.NewArrayList();
			foreach (string bpid in BlockPoolIds)
			{
				nsInfos.AddItem(new NamespaceInfo(0, ClusterId, bpid, 1));
			}
			try
			{
				spyDataset.AddVolume(location, nsInfos);
				NUnit.Framework.Assert.Fail("Expect to throw MultipleIOException");
			}
			catch (MultipleIOException)
			{
			}
			FsDatasetTestUtil.AssertFileLockReleased(badDir.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDeletingBlocks()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).Build
				();
			try
			{
				cluster.WaitActive();
				DataNode dn = cluster.GetDataNodes()[0];
				FsDatasetImpl ds = (FsDatasetImpl)DataNodeTestUtils.GetFSDataset(dn);
				FsVolumeImpl vol = ds.GetVolumes()[0];
				ExtendedBlock eb;
				ReplicaInfo info;
				IList<Block> blockList = new AList<Block>();
				for (int i = 1; i <= 63; i++)
				{
					eb = new ExtendedBlock(Blockpool, i, 1, 1000 + i);
					info = new FinalizedReplica(eb.GetLocalBlock(), vol, vol.GetCurrentDir().GetParentFile
						());
					ds.volumeMap.Add(Blockpool, info);
					info.GetBlockFile().CreateNewFile();
					info.GetMetaFile().CreateNewFile();
					blockList.AddItem(info);
				}
				ds.Invalidate(Blockpool, Sharpen.Collections.ToArray(blockList, new Block[0]));
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
				// Nothing to do
				NUnit.Framework.Assert.IsTrue(ds.IsDeletingBlock(Blockpool, blockList[0].GetBlockId
					()));
				blockList.Clear();
				eb = new ExtendedBlock(Blockpool, 64, 1, 1064);
				info = new FinalizedReplica(eb.GetLocalBlock(), vol, vol.GetCurrentDir().GetParentFile
					());
				ds.volumeMap.Add(Blockpool, info);
				info.GetBlockFile().CreateNewFile();
				info.GetMetaFile().CreateNewFile();
				blockList.AddItem(info);
				ds.Invalidate(Blockpool, Sharpen.Collections.ToArray(blockList, new Block[0]));
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
				// Nothing to do
				NUnit.Framework.Assert.IsFalse(ds.IsDeletingBlock(Blockpool, blockList[0].GetBlockId
					()));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDuplicateReplicaResolution()
		{
			FsVolumeImpl fsv1 = Org.Mockito.Mockito.Mock<FsVolumeImpl>();
			FsVolumeImpl fsv2 = Org.Mockito.Mockito.Mock<FsVolumeImpl>();
			FilePath f1 = new FilePath("d1/block");
			FilePath f2 = new FilePath("d2/block");
			ReplicaInfo replicaOlder = new FinalizedReplica(1, 1, 1, fsv1, f1);
			ReplicaInfo replica = new FinalizedReplica(1, 2, 2, fsv1, f1);
			ReplicaInfo replicaSame = new FinalizedReplica(1, 2, 2, fsv1, f1);
			ReplicaInfo replicaNewer = new FinalizedReplica(1, 3, 3, fsv1, f1);
			ReplicaInfo replicaOtherOlder = new FinalizedReplica(1, 1, 1, fsv2, f2);
			ReplicaInfo replicaOtherSame = new FinalizedReplica(1, 2, 2, fsv2, f2);
			ReplicaInfo replicaOtherNewer = new FinalizedReplica(1, 3, 3, fsv2, f2);
			// equivalent path so don't remove either
			NUnit.Framework.Assert.IsNull(BlockPoolSlice.SelectReplicaToDelete(replicaSame, replica
				));
			NUnit.Framework.Assert.IsNull(BlockPoolSlice.SelectReplicaToDelete(replicaOlder, 
				replica));
			NUnit.Framework.Assert.IsNull(BlockPoolSlice.SelectReplicaToDelete(replicaNewer, 
				replica));
			// keep latest found replica
			NUnit.Framework.Assert.AreSame(replica, BlockPoolSlice.SelectReplicaToDelete(replicaOtherSame
				, replica));
			NUnit.Framework.Assert.AreSame(replicaOtherOlder, BlockPoolSlice.SelectReplicaToDelete
				(replicaOtherOlder, replica));
			NUnit.Framework.Assert.AreSame(replica, BlockPoolSlice.SelectReplicaToDelete(replicaOtherNewer
				, replica));
		}
	}
}
