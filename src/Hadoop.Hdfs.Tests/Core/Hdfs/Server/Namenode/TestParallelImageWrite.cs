using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>A JUnit test for checking if restarting DFS preserves integrity.</summary>
	/// <remarks>
	/// A JUnit test for checking if restarting DFS preserves integrity.
	/// Specifically with FSImage being written in parallel
	/// </remarks>
	public class TestParallelImageWrite
	{
		private const int NumDatanodes = 4;

		/// <summary>check if DFS remains in proper condition after a restart</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRestartDFS()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			FSNamesystem fsn = null;
			int numNamenodeDirs;
			DFSTestUtil files = new DFSTestUtil.Builder().SetName("TestRestartDFS").SetNumFiles
				(200).Build();
			string dir = "/srcdat";
			Path rootpath = new Path("/");
			Path dirpath = new Path(dir);
			long rootmtime;
			FileStatus rootstatus;
			FileStatus dirstatus;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Format(true).NumDataNodes(NumDatanodes
					).Build();
				string[] nameNodeDirs = conf.GetStrings(DFSConfigKeys.DfsNamenodeNameDirKey, new 
					string[] {  });
				numNamenodeDirs = nameNodeDirs.Length;
				NUnit.Framework.Assert.IsTrue("failed to get number of Namenode StorageDirs", numNamenodeDirs
					 != 0);
				FileSystem fs = cluster.GetFileSystem();
				files.CreateFiles(fs, dir);
				rootmtime = fs.GetFileStatus(rootpath).GetModificationTime();
				rootstatus = fs.GetFileStatus(dirpath);
				dirstatus = fs.GetFileStatus(dirpath);
				fs.SetOwner(rootpath, rootstatus.GetOwner() + "_XXX", null);
				fs.SetOwner(dirpath, null, dirstatus.GetGroup() + "_XXX");
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			try
			{
				// Force the NN to save its images on startup so long as
				// there are any uncheckpointed txns
				conf.SetInt(DFSConfigKeys.DfsNamenodeCheckpointTxnsKey, 1);
				// Here we restart the MiniDFScluster without formatting namenode
				cluster = new MiniDFSCluster.Builder(conf).Format(false).NumDataNodes(NumDatanodes
					).Build();
				fsn = cluster.GetNamesystem();
				FileSystem fs = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue("Filesystem corrupted after restart.", files.CheckFiles
					(fs, dir));
				FileStatus newrootstatus = fs.GetFileStatus(rootpath);
				NUnit.Framework.Assert.AreEqual(rootmtime, newrootstatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(rootstatus.GetOwner() + "_XXX", newrootstatus.GetOwner
					());
				NUnit.Framework.Assert.AreEqual(rootstatus.GetGroup(), newrootstatus.GetGroup());
				FileStatus newdirstatus = fs.GetFileStatus(dirpath);
				NUnit.Framework.Assert.AreEqual(dirstatus.GetOwner(), newdirstatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(dirstatus.GetGroup() + "_XXX", newdirstatus.GetGroup
					());
				rootmtime = fs.GetFileStatus(rootpath).GetModificationTime();
				string checkAfterRestart = CheckImages(fsn, numNamenodeDirs);
				// Modify the system and then perform saveNamespace
				files.Cleanup(fs, dir);
				files.CreateFiles(fs, dir);
				fsn.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				cluster.GetNameNodeRpc().SaveNamespace();
				string checkAfterModify = CheckImages(fsn, numNamenodeDirs);
				NUnit.Framework.Assert.IsFalse("Modified namespace should change fsimage contents. "
					 + "was: " + checkAfterRestart + " now: " + checkAfterModify, checkAfterRestart.
					Equals(checkAfterModify));
				fsn.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
				files.Cleanup(fs, dir);
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
		/// Confirm that FSImage files in all StorageDirectory are the same,
		/// and non-empty, and there are the expected number of them.
		/// </summary>
		/// <param name="fsn">- the FSNamesystem being checked.</param>
		/// <param name="numImageDirs">- the configured number of StorageDirectory of type IMAGE.
		/// 	</param>
		/// <returns>- the md5 hash of the most recent FSImage files, which must all be the same.
		/// 	</returns>
		/// <exception cref="System.Exception">
		/// if image files are empty or different,
		/// if less than two StorageDirectory are provided, or if the
		/// actual number of StorageDirectory is less than configured.
		/// </exception>
		public static string CheckImages(FSNamesystem fsn, int numImageDirs)
		{
			NNStorage stg = fsn.GetFSImage().GetStorage();
			//any failed StorageDirectory is removed from the storageDirs list
			NUnit.Framework.Assert.AreEqual("Some StorageDirectories failed Upgrade", numImageDirs
				, stg.GetNumStorageDirs(NNStorage.NameNodeDirType.Image));
			NUnit.Framework.Assert.IsTrue("Not enough fsimage copies in MiniDFSCluster " + "to test parallel write"
				, numImageDirs > 1);
			// List of "current/" directory from each SD
			IList<FilePath> dirs = FSImageTestUtil.GetCurrentDirs(stg, NNStorage.NameNodeDirType
				.Image);
			// across directories, all files with same names should be identical hashes   
			FSImageTestUtil.AssertParallelFilesAreIdentical(dirs, Sharpen.Collections.EmptySet
				<string>());
			FSImageTestUtil.AssertSameNewestImage(dirs);
			// Return the hash of the newest image file
			Storage.StorageDirectory firstSd = stg.DirIterator(NNStorage.NameNodeDirType.Image
				).Next();
			FilePath latestImage = FSImageTestUtil.FindLatestImageFile(firstSd);
			string md5 = FSImageTestUtil.GetImageFileMD5IgnoringTxId(latestImage);
			System.Console.Error.WriteLine("md5 of " + latestImage + ": " + md5);
			return md5;
		}
	}
}
