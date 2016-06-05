using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestNameNodeResourceChecker
	{
		private static readonly FilePath BaseDir = PathUtils.GetTestDir(typeof(TestNameNodeResourceChecker
			));

		private Configuration conf;

		private FilePath baseDir;

		private FilePath nameDir;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			nameDir = new FilePath(BaseDir, "resource-check-name-dir");
			nameDir.Mkdirs();
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameDir.GetAbsolutePath());
		}

		/// <summary>
		/// Tests that hasAvailableDiskSpace returns true if disk usage is below
		/// threshold.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckAvailability()
		{
			conf.SetLong(DFSConfigKeys.DfsNamenodeDuReservedKey, 0);
			NameNodeResourceChecker nb = new NameNodeResourceChecker(conf);
			NUnit.Framework.Assert.IsTrue("isResourceAvailable must return true if " + "disk usage is lower than threshold"
				, nb.HasAvailableDiskSpace());
		}

		/// <summary>
		/// Tests that hasAvailableDiskSpace returns false if disk usage is above
		/// threshold.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckAvailabilityNeg()
		{
			conf.SetLong(DFSConfigKeys.DfsNamenodeDuReservedKey, long.MaxValue);
			NameNodeResourceChecker nb = new NameNodeResourceChecker(conf);
			NUnit.Framework.Assert.IsFalse("isResourceAvailable must return false if " + "disk usage is higher than threshold"
				, nb.HasAvailableDiskSpace());
		}

		/// <summary>
		/// Tests that NameNode resource monitor causes the NN to enter safe mode when
		/// resources are low.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckThatNameNodeResourceMonitorIsRunning()
		{
			MiniDFSCluster cluster = null;
			try
			{
				conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameDir.GetAbsolutePath());
				conf.SetLong(DFSConfigKeys.DfsNamenodeResourceCheckIntervalKey, 1);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				NameNodeResourceChecker mockResourceChecker = Org.Mockito.Mockito.Mock<NameNodeResourceChecker
					>();
				Org.Mockito.Mockito.When(mockResourceChecker.HasAvailableDiskSpace()).ThenReturn(
					true);
				cluster.GetNameNode().GetNamesystem().nnResourceChecker = mockResourceChecker;
				cluster.WaitActive();
				string name = typeof(FSNamesystem.NameNodeResourceMonitor).FullName;
				bool isNameNodeMonitorRunning = false;
				ICollection<Sharpen.Thread> runningThreads = Sharpen.Thread.GetAllStackTraces().Keys;
				foreach (Sharpen.Thread runningThread in runningThreads)
				{
					if (runningThread.ToString().StartsWith("Thread[" + name))
					{
						isNameNodeMonitorRunning = true;
						break;
					}
				}
				NUnit.Framework.Assert.IsTrue("NN resource monitor should be running", isNameNodeMonitorRunning
					);
				NUnit.Framework.Assert.IsFalse("NN should not presently be in safe mode", cluster
					.GetNameNode().IsInSafeMode());
				Org.Mockito.Mockito.When(mockResourceChecker.HasAvailableDiskSpace()).ThenReturn(
					false);
				// Make sure the NNRM thread has a chance to run.
				long startMillis = Time.Now();
				while (!cluster.GetNameNode().IsInSafeMode() && Time.Now() < startMillis + (60 * 
					1000))
				{
					Sharpen.Thread.Sleep(1000);
				}
				NUnit.Framework.Assert.IsTrue("NN should be in safe mode after resources crossed threshold"
					, cluster.GetNameNode().IsInSafeMode());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Tests that only a single space check is performed if two name dirs are
		/// supplied which are on the same volume.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestChecking2NameDirsOnOneVolume()
		{
			Configuration conf = new Configuration();
			FilePath nameDir1 = new FilePath(BaseDir, "name-dir1");
			FilePath nameDir2 = new FilePath(BaseDir, "name-dir2");
			nameDir1.Mkdirs();
			nameDir2.Mkdirs();
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameDir1.GetAbsolutePath() + "," +
				 nameDir2.GetAbsolutePath());
			conf.SetLong(DFSConfigKeys.DfsNamenodeDuReservedKey, long.MaxValue);
			NameNodeResourceChecker nb = new NameNodeResourceChecker(conf);
			NUnit.Framework.Assert.AreEqual("Should not check the same volume more than once."
				, 1, nb.GetVolumesLowOnSpace().Count);
		}

		/// <summary>
		/// Tests that only a single space check is performed if extra volumes are
		/// configured manually which also coincide with a volume the name dir is on.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckingExtraVolumes()
		{
			Configuration conf = new Configuration();
			FilePath nameDir = new FilePath(BaseDir, "name-dir");
			nameDir.Mkdirs();
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameDir.GetAbsolutePath());
			conf.Set(DFSConfigKeys.DfsNamenodeCheckedVolumesKey, nameDir.GetAbsolutePath());
			conf.SetLong(DFSConfigKeys.DfsNamenodeDuReservedKey, long.MaxValue);
			NameNodeResourceChecker nb = new NameNodeResourceChecker(conf);
			NUnit.Framework.Assert.AreEqual("Should not check the same volume more than once."
				, 1, nb.GetVolumesLowOnSpace().Count);
		}

		/// <summary>
		/// Test that the NN is considered to be out of resources only once all
		/// redundant configured volumes are low on resources, or when any required
		/// volume is low on resources.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestLowResourceVolumePolicy()
		{
			Configuration conf = new Configuration();
			FilePath nameDir1 = new FilePath(BaseDir, "name-dir1");
			FilePath nameDir2 = new FilePath(BaseDir, "name-dir2");
			nameDir1.Mkdirs();
			nameDir2.Mkdirs();
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameDir1.GetAbsolutePath() + "," +
				 nameDir2.GetAbsolutePath());
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckedVolumesMinimumKey, 2);
			NameNodeResourceChecker nnrc = new NameNodeResourceChecker(conf);
			// For the purpose of this test, we need to force the name dirs to appear to
			// be on different volumes.
			IDictionary<string, NameNodeResourceChecker.CheckedVolume> volumes = new Dictionary
				<string, NameNodeResourceChecker.CheckedVolume>();
			NameNodeResourceChecker.CheckedVolume volume1 = Org.Mockito.Mockito.Mock<NameNodeResourceChecker.CheckedVolume
				>();
			NameNodeResourceChecker.CheckedVolume volume2 = Org.Mockito.Mockito.Mock<NameNodeResourceChecker.CheckedVolume
				>();
			NameNodeResourceChecker.CheckedVolume volume3 = Org.Mockito.Mockito.Mock<NameNodeResourceChecker.CheckedVolume
				>();
			NameNodeResourceChecker.CheckedVolume volume4 = Org.Mockito.Mockito.Mock<NameNodeResourceChecker.CheckedVolume
				>();
			NameNodeResourceChecker.CheckedVolume volume5 = Org.Mockito.Mockito.Mock<NameNodeResourceChecker.CheckedVolume
				>();
			Org.Mockito.Mockito.When(volume1.IsResourceAvailable()).ThenReturn(true);
			Org.Mockito.Mockito.When(volume2.IsResourceAvailable()).ThenReturn(true);
			Org.Mockito.Mockito.When(volume3.IsResourceAvailable()).ThenReturn(true);
			Org.Mockito.Mockito.When(volume4.IsResourceAvailable()).ThenReturn(true);
			Org.Mockito.Mockito.When(volume5.IsResourceAvailable()).ThenReturn(true);
			// Make volumes 4 and 5 required.
			Org.Mockito.Mockito.When(volume4.IsRequired()).ThenReturn(true);
			Org.Mockito.Mockito.When(volume5.IsRequired()).ThenReturn(true);
			volumes["volume1"] = volume1;
			volumes["volume2"] = volume2;
			volumes["volume3"] = volume3;
			volumes["volume4"] = volume4;
			volumes["volume5"] = volume5;
			nnrc.SetVolumes(volumes);
			// Initially all dirs have space.
			NUnit.Framework.Assert.IsTrue(nnrc.HasAvailableDiskSpace());
			// 1/3 redundant dir is low on space.
			Org.Mockito.Mockito.When(volume1.IsResourceAvailable()).ThenReturn(false);
			NUnit.Framework.Assert.IsTrue(nnrc.HasAvailableDiskSpace());
			// 2/3 redundant dirs are low on space.
			Org.Mockito.Mockito.When(volume2.IsResourceAvailable()).ThenReturn(false);
			NUnit.Framework.Assert.IsFalse(nnrc.HasAvailableDiskSpace());
			// Lower the minimum number of redundant volumes that must be available.
			nnrc.SetMinimumReduntdantVolumes(1);
			NUnit.Framework.Assert.IsTrue(nnrc.HasAvailableDiskSpace());
			// Just one required dir is low on space.
			Org.Mockito.Mockito.When(volume3.IsResourceAvailable()).ThenReturn(false);
			NUnit.Framework.Assert.IsFalse(nnrc.HasAvailableDiskSpace());
			// Just the other required dir is low on space.
			Org.Mockito.Mockito.When(volume3.IsResourceAvailable()).ThenReturn(true);
			Org.Mockito.Mockito.When(volume4.IsResourceAvailable()).ThenReturn(false);
			NUnit.Framework.Assert.IsFalse(nnrc.HasAvailableDiskSpace());
		}
	}
}
