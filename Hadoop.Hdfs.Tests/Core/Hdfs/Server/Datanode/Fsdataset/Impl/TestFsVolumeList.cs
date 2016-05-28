using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	public class TestFsVolumeList
	{
		private readonly Configuration conf = new Configuration();

		private VolumeChoosingPolicy<FsVolumeImpl> blockChooser = new RoundRobinVolumeChoosingPolicy
			<FsVolumeImpl>();

		private FsDatasetImpl dataset = null;

		private string baseDir;

		private BlockScanner blockScanner;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			dataset = Org.Mockito.Mockito.Mock<FsDatasetImpl>();
			baseDir = new FileSystemTestHelper().GetTestRootDir();
			Configuration blockScannerConf = new Configuration();
			blockScannerConf.SetInt(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, -1);
			blockScanner = new BlockScanner(null, blockScannerConf);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetNextVolumeWithClosedVolume()
		{
			FsVolumeList volumeList = new FsVolumeList(Collections.EmptyList<VolumeFailureInfo
				>(), blockScanner, blockChooser);
			IList<FsVolumeImpl> volumes = new AList<FsVolumeImpl>();
			for (int i = 0; i < 3; i++)
			{
				FilePath curDir = new FilePath(baseDir, "nextvolume-" + i);
				curDir.Mkdirs();
				FsVolumeImpl volume = new FsVolumeImpl(dataset, "storage-id", curDir, conf, StorageType
					.Default);
				volume.SetCapacityForTesting(1024 * 1024 * 1024);
				volumes.AddItem(volume);
				volumeList.AddVolume(volume.ObtainReference());
			}
			// Close the second volume.
			volumes[1].CloseAndWait();
			for (int i_1 = 0; i_1 < 10; i_1++)
			{
				using (FsVolumeReference @ref = volumeList.GetNextVolume(StorageType.Default, 128
					))
				{
					// volume No.2 will not be chosen.
					Assert.AssertNotEquals(@ref.GetVolume(), volumes[1]);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckDirsWithClosedVolume()
		{
			FsVolumeList volumeList = new FsVolumeList(Sharpen.Collections.EmptyList<VolumeFailureInfo
				>(), blockScanner, blockChooser);
			IList<FsVolumeImpl> volumes = new AList<FsVolumeImpl>();
			for (int i = 0; i < 3; i++)
			{
				FilePath curDir = new FilePath(baseDir, "volume-" + i);
				curDir.Mkdirs();
				FsVolumeImpl volume = new FsVolumeImpl(dataset, "storage-id", curDir, conf, StorageType
					.Default);
				volumes.AddItem(volume);
				volumeList.AddVolume(volume.ObtainReference());
			}
			// Close the 2nd volume.
			volumes[1].CloseAndWait();
			// checkDirs() should ignore the 2nd volume since it is closed.
			volumeList.CheckDirs();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReleaseVolumeRefIfNoBlockScanner()
		{
			FsVolumeList volumeList = new FsVolumeList(Sharpen.Collections.EmptyList<VolumeFailureInfo
				>(), null, blockChooser);
			FilePath volDir = new FilePath(baseDir, "volume-0");
			volDir.Mkdirs();
			FsVolumeImpl volume = new FsVolumeImpl(dataset, "storage-id", volDir, conf, StorageType
				.Default);
			FsVolumeReference @ref = volume.ObtainReference();
			volumeList.AddVolume(@ref);
			try
			{
				@ref.Close();
				NUnit.Framework.Assert.Fail("Should throw exception because the reference is closed in "
					 + "VolumeList#addVolume().");
			}
			catch (InvalidOperationException)
			{
			}
		}
	}
}
