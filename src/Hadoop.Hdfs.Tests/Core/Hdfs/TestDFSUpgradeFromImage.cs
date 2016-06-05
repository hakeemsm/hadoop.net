using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Inotify;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This tests data transfer protocol handling in the Datanode.</summary>
	/// <remarks>
	/// This tests data transfer protocol handling in the Datanode. It sends
	/// various forms of wrong data and verifies that Datanode handles it well.
	/// This test uses the following items from src/test/.../dfs directory :
	/// 1) hadoop-22-dfs-dir.tgz and other tarred pre-upgrade NN / DN
	/// directory images
	/// 2) hadoop-dfs-dir.txt : checksums that are compared in this test.
	/// Please read hadoop-dfs-dir.txt for more information.
	/// </remarks>
	public class TestDFSUpgradeFromImage
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDFSUpgradeFromImage
			));

		private static readonly FilePath TestRootDir = new FilePath(MiniDFSCluster.GetBaseDirectory
			());

		private const string HadoopDfsDirTxt = "hadoop-dfs-dir.txt";

		private const string Hadoop22Image = "hadoop-22-dfs-dir.tgz";

		private const string Hadoop1BbwImage = "hadoop1-bbw.tgz";

		private const string Hadoop1ReservedImage = "hadoop-1-reserved.tgz";

		private const string Hadoop023ReservedImage = "hadoop-0.23-reserved.tgz";

		private const string Hadoop2ReservedImage = "hadoop-2-reserved.tgz";

		private const string Hadoop252Image = "hadoop-252-dfs-dir.tgz";

		private class ReferenceFileInfo
		{
			internal string path;

			internal long checksum;
		}

		internal static readonly Configuration upgradeConf;

		static TestDFSUpgradeFromImage()
		{
			upgradeConf = new HdfsConfiguration();
			upgradeConf.SetInt(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, -1);
			// block scanning off
			if (Runtime.GetProperty("test.build.data") == null)
			{
				// to allow test to be run outside of Maven
				Runtime.SetProperty("test.build.data", "build/test/data");
			}
		}

		public interface ClusterVerifier
		{
			/// <exception cref="System.IO.IOException"/>
			void VerifyClusterPostUpgrade(MiniDFSCluster cluster);
		}

		internal readonly List<TestDFSUpgradeFromImage.ReferenceFileInfo> refList = new List
			<TestDFSUpgradeFromImage.ReferenceFileInfo>();

		internal IEnumerator<TestDFSUpgradeFromImage.ReferenceFileInfo> refIter;

		internal bool printChecksum = false;

		/// <exception cref="System.IO.IOException"/>
		internal virtual void UnpackStorage(string tarFileName, string referenceName)
		{
			string tarFile = Runtime.GetProperty("test.cache.data", "build/test/cache") + "/"
				 + tarFileName;
			string dataDir = Runtime.GetProperty("test.build.data", "build/test/data");
			FilePath dfsDir = new FilePath(dataDir, "dfs");
			if (dfsDir.Exists() && !FileUtil.FullyDelete(dfsDir))
			{
				throw new IOException("Could not delete dfs directory '" + dfsDir + "'");
			}
			Log.Info("Unpacking " + tarFile);
			FileUtil.UnTar(new FilePath(tarFile), new FilePath(dataDir));
			//Now read the reference info
			BufferedReader reader = new BufferedReader(new FileReader(Runtime.GetProperty("test.cache.data"
				, "build/test/cache") + "/" + referenceName));
			string line;
			while ((line = reader.ReadLine()) != null)
			{
				line = line.Trim();
				if (line.Length <= 0 || line.StartsWith("#"))
				{
					continue;
				}
				string[] arr = line.Split("\\s+");
				if (arr.Length < 1)
				{
					continue;
				}
				if (arr[0].Equals("printChecksums"))
				{
					printChecksum = true;
					break;
				}
				if (arr.Length < 2)
				{
					continue;
				}
				TestDFSUpgradeFromImage.ReferenceFileInfo info = new TestDFSUpgradeFromImage.ReferenceFileInfo
					();
				info.path = arr[0];
				info.checksum = long.Parse(arr[1]);
				refList.AddItem(info);
			}
			reader.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyChecksum(string path, long checksum)
		{
			if (refIter == null)
			{
				refIter = refList.GetEnumerator();
			}
			if (printChecksum)
			{
				Log.Info("CRC info for reference file : " + path + " \t " + checksum);
			}
			else
			{
				if (!refIter.HasNext())
				{
					throw new IOException("Checking checksum for " + path + "Not enough elements in the refList"
						);
				}
				TestDFSUpgradeFromImage.ReferenceFileInfo info = refIter.Next();
				// The paths are expected to be listed in the same order 
				// as they are traversed here.
				NUnit.Framework.Assert.AreEqual(info.path, path);
				NUnit.Framework.Assert.AreEqual("Checking checksum for " + path, info.checksum, checksum
					);
			}
		}

		/// <summary>Try to open a file for reading several times.</summary>
		/// <remarks>
		/// Try to open a file for reading several times.
		/// If we fail because lease recovery hasn't completed, retry the open.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static FSInputStream DfsOpenFileWithRetries(DistributedFileSystem dfs, string
			 pathName)
		{
			IOException exc = null;
			for (int tries = 0; tries < 10; tries++)
			{
				try
				{
					return dfs.dfs.Open(pathName);
				}
				catch (IOException e)
				{
					exc = e;
				}
				if (!exc.Message.Contains("Cannot obtain " + "block length for LocatedBlock"))
				{
					throw exc;
				}
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
			}
			throw exc;
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyDir(DistributedFileSystem dfs, Path dir, CRC32 overallChecksum
			)
		{
			FileStatus[] fileArr = dfs.ListStatus(dir);
			SortedDictionary<Path, bool> fileMap = new SortedDictionary<Path, bool>();
			foreach (FileStatus file in fileArr)
			{
				fileMap[file.GetPath()] = Sharpen.Extensions.ValueOf(file.IsDirectory());
			}
			for (IEnumerator<Path> it = fileMap.Keys.GetEnumerator(); it.HasNext(); )
			{
				Path path = it.Next();
				bool isDir = fileMap[path];
				string pathName = path.ToUri().GetPath();
				overallChecksum.Update(Sharpen.Runtime.GetBytesForString(pathName));
				if (isDir)
				{
					VerifyDir(dfs, path, overallChecksum);
				}
				else
				{
					// this is not a directory. Checksum the file data.
					CRC32 fileCRC = new CRC32();
					FSInputStream @in = DfsOpenFileWithRetries(dfs, pathName);
					byte[] buf = new byte[4096];
					int nRead = 0;
					while ((nRead = @in.Read(buf, 0, buf.Length)) > 0)
					{
						fileCRC.Update(buf, 0, nRead);
					}
					VerifyChecksum(pathName, fileCRC.GetValue());
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyFileSystem(DistributedFileSystem dfs)
		{
			CRC32 overallChecksum = new CRC32();
			VerifyDir(dfs, new Path("/"), overallChecksum);
			VerifyChecksum("overallCRC", overallChecksum.GetValue());
			if (printChecksum)
			{
				throw new IOException("Checksums are written to log as requested. " + "Throwing this exception to force an error "
					 + "for this test.");
			}
		}

		/// <summary>
		/// Test that sets up a fake image from Hadoop 0.3.0 and tries to start a
		/// NN, verifying that the correct error message is thrown.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailOnPreUpgradeImage()
		{
			Configuration conf = new HdfsConfiguration();
			FilePath namenodeStorage = new FilePath(TestRootDir, "nnimage-0.3.0");
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, namenodeStorage.ToString());
			// Set up a fake NN storage that looks like an ancient Hadoop dir circa 0.3.0
			FileUtil.FullyDelete(namenodeStorage);
			NUnit.Framework.Assert.IsTrue("Make " + namenodeStorage, namenodeStorage.Mkdirs()
				);
			FilePath imageDir = new FilePath(namenodeStorage, "image");
			NUnit.Framework.Assert.IsTrue("Make " + imageDir, imageDir.Mkdirs());
			// Hex dump of a formatted image from Hadoop 0.3.0
			FilePath imageFile = new FilePath(imageDir, "fsimage");
			byte[] imageBytes = StringUtils.HexStringToByte("fffffffee17c0d2700000000");
			FileOutputStream fos = new FileOutputStream(imageFile);
			try
			{
				fos.Write(imageBytes);
			}
			finally
			{
				fos.Close();
			}
			// Now try to start an NN from it
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).ManageDataDfsDirs
					(false).ManageNameDfsDirs(false).StartupOption(HdfsServerConstants.StartupOption
					.Regular).Build();
				NUnit.Framework.Assert.Fail("Was able to start NN from 0.3.0 image");
			}
			catch (IOException ioe)
			{
				if (!ioe.ToString().Contains("Old layout version is 'too old'"))
				{
					throw;
				}
			}
			finally
			{
				// We expect startup to fail, but just in case it didn't, shutdown now.
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test upgrade from 0.22 image</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgradeFromRel22Image()
		{
			UnpackStorage(Hadoop22Image, HadoopDfsDirTxt);
			UpgradeAndVerify(new MiniDFSCluster.Builder(upgradeConf).NumDataNodes(4), null);
		}

		/// <summary>
		/// Test upgrade from 0.22 image with corrupt md5, make sure it
		/// fails to upgrade
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgradeFromCorruptRel22Image()
		{
			UnpackStorage(Hadoop22Image, HadoopDfsDirTxt);
			// Overwrite the md5 stored in the VERSION files
			FilePath baseDir = new FilePath(MiniDFSCluster.GetBaseDirectory());
			FSImageTestUtil.CorruptVersionFile(new FilePath(baseDir, "name1/current/VERSION")
				, "imageMD5Digest", "22222222222222222222222222222222");
			FSImageTestUtil.CorruptVersionFile(new FilePath(baseDir, "name2/current/VERSION")
				, "imageMD5Digest", "22222222222222222222222222222222");
			// Attach our own log appender so we can verify output
			LogVerificationAppender appender = new LogVerificationAppender();
			Logger logger = Logger.GetRootLogger();
			logger.AddAppender(appender);
			// Upgrade should now fail
			try
			{
				UpgradeAndVerify(new MiniDFSCluster.Builder(upgradeConf).NumDataNodes(4), null);
				NUnit.Framework.Assert.Fail("Upgrade did not fail with bad MD5");
			}
			catch (IOException ioe)
			{
				string msg = StringUtils.StringifyException(ioe);
				if (!msg.Contains("Failed to load an FSImage file"))
				{
					throw;
				}
				int md5failures = appender.CountExceptionsWithMessage(" is corrupt with MD5 checksum of "
					);
				NUnit.Framework.Assert.AreEqual("Upgrade did not fail with bad MD5", 1, md5failures
					);
			}
		}

		/// <summary>Test upgrade from a branch-1.2 image with reserved paths</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgradeFromRel1ReservedImage()
		{
			UnpackStorage(Hadoop1ReservedImage, HadoopDfsDirTxt);
			MiniDFSCluster cluster = null;
			// Try it once without setting the upgrade flag to ensure it fails
			Configuration conf = new Configuration();
			// Try it again with a custom rename string
			try
			{
				FSImageFormat.SetRenameReservedPairs(".snapshot=.user-snapshot," + ".reserved=.my-reserved"
					);
				cluster = new MiniDFSCluster.Builder(conf).Format(false).StartupOption(HdfsServerConstants.StartupOption
					.Upgrade).NumDataNodes(0).Build();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				// Make sure the paths were renamed as expected
				// Also check that paths are present after a restart, checks that the
				// upgraded fsimage has the same state.
				string[] expected = new string[] { "/.my-reserved", "/.user-snapshot", "/.user-snapshot/.user-snapshot"
					, "/.user-snapshot/open", "/dir1", "/dir1/.user-snapshot", "/dir2", "/dir2/.user-snapshot"
					, "/user", "/user/andrew", "/user/andrew/.user-snapshot" };
				for (int i = 0; i < 2; i++)
				{
					// Restart the second time through this loop
					if (i == 1)
					{
						cluster.FinalizeCluster(conf);
						cluster.RestartNameNode(true);
					}
					AList<Path> toList = new AList<Path>();
					toList.AddItem(new Path("/"));
					AList<string> found = new AList<string>();
					while (!toList.IsEmpty())
					{
						Path p = toList.Remove(0);
						FileStatus[] statuses = dfs.ListStatus(p);
						foreach (FileStatus status in statuses)
						{
							string path = status.GetPath().ToUri().GetPath();
							System.Console.Out.WriteLine("Found path " + path);
							found.AddItem(path);
							if (status.IsDirectory())
							{
								toList.AddItem(status.GetPath());
							}
						}
					}
					foreach (string s in expected)
					{
						NUnit.Framework.Assert.IsTrue("Did not find expected path " + s, found.Contains(s
							));
					}
					NUnit.Framework.Assert.AreEqual("Found an unexpected path while listing filesystem"
						, found.Count, expected.Length);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test upgrade from a 0.23.11 image with reserved paths</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgradeFromRel023ReservedImage()
		{
			UnpackStorage(Hadoop023ReservedImage, HadoopDfsDirTxt);
			MiniDFSCluster cluster = null;
			// Try it once without setting the upgrade flag to ensure it fails
			Configuration conf = new Configuration();
			// Try it again with a custom rename string
			try
			{
				FSImageFormat.SetRenameReservedPairs(".snapshot=.user-snapshot," + ".reserved=.my-reserved"
					);
				cluster = new MiniDFSCluster.Builder(conf).Format(false).StartupOption(HdfsServerConstants.StartupOption
					.Upgrade).NumDataNodes(0).Build();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				// Make sure the paths were renamed as expected
				// Also check that paths are present after a restart, checks that the
				// upgraded fsimage has the same state.
				string[] expected = new string[] { "/.user-snapshot", "/dir1", "/dir1/.user-snapshot"
					, "/dir2", "/dir2/.user-snapshot" };
				for (int i = 0; i < 2; i++)
				{
					// Restart the second time through this loop
					if (i == 1)
					{
						cluster.FinalizeCluster(conf);
						cluster.RestartNameNode(true);
					}
					AList<Path> toList = new AList<Path>();
					toList.AddItem(new Path("/"));
					AList<string> found = new AList<string>();
					while (!toList.IsEmpty())
					{
						Path p = toList.Remove(0);
						FileStatus[] statuses = dfs.ListStatus(p);
						foreach (FileStatus status in statuses)
						{
							string path = status.GetPath().ToUri().GetPath();
							System.Console.Out.WriteLine("Found path " + path);
							found.AddItem(path);
							if (status.IsDirectory())
							{
								toList.AddItem(status.GetPath());
							}
						}
					}
					foreach (string s in expected)
					{
						NUnit.Framework.Assert.IsTrue("Did not find expected path " + s, found.Contains(s
							));
					}
					NUnit.Framework.Assert.AreEqual("Found an unexpected path while listing filesystem"
						, found.Count, expected.Length);
				}
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
		/// Test upgrade from 2.0 image with a variety of .snapshot and .reserved
		/// paths to test renaming on upgrade
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgradeFromRel2ReservedImage()
		{
			UnpackStorage(Hadoop2ReservedImage, HadoopDfsDirTxt);
			MiniDFSCluster cluster = null;
			// Try it once without setting the upgrade flag to ensure it fails
			Configuration conf = new Configuration();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Format(false).StartupOption(HdfsServerConstants.StartupOption
					.Upgrade).NumDataNodes(0).Build();
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("reserved path component in this version"
					, e);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			// Try it again with a custom rename string
			try
			{
				FSImageFormat.SetRenameReservedPairs(".snapshot=.user-snapshot," + ".reserved=.my-reserved"
					);
				cluster = new MiniDFSCluster.Builder(conf).Format(false).StartupOption(HdfsServerConstants.StartupOption
					.Upgrade).NumDataNodes(0).Build();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				// Make sure the paths were renamed as expected
				// Also check that paths are present after a restart, checks that the
				// upgraded fsimage has the same state.
				string[] expected = new string[] { "/edits", "/edits/.reserved", "/edits/.user-snapshot"
					, "/edits/.user-snapshot/editsdir", "/edits/.user-snapshot/editsdir/editscontents"
					, "/edits/.user-snapshot/editsdir/editsdir2", "/image", "/image/.reserved", "/image/.user-snapshot"
					, "/image/.user-snapshot/imagedir", "/image/.user-snapshot/imagedir/imagecontents"
					, "/image/.user-snapshot/imagedir/imagedir2", "/.my-reserved", "/.my-reserved/edits-touch"
					, "/.my-reserved/image-touch" };
				for (int i = 0; i < 2; i++)
				{
					// Restart the second time through this loop
					if (i == 1)
					{
						cluster.FinalizeCluster(conf);
						cluster.RestartNameNode(true);
					}
					AList<Path> toList = new AList<Path>();
					toList.AddItem(new Path("/"));
					AList<string> found = new AList<string>();
					while (!toList.IsEmpty())
					{
						Path p = toList.Remove(0);
						FileStatus[] statuses = dfs.ListStatus(p);
						foreach (FileStatus status in statuses)
						{
							string path = status.GetPath().ToUri().GetPath();
							System.Console.Out.WriteLine("Found path " + path);
							found.AddItem(path);
							if (status.IsDirectory())
							{
								toList.AddItem(status.GetPath());
							}
						}
					}
					foreach (string s in expected)
					{
						NUnit.Framework.Assert.IsTrue("Did not find expected path " + s, found.Contains(s
							));
					}
					NUnit.Framework.Assert.AreEqual("Found an unexpected path while listing filesystem"
						, found.Count, expected.Length);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void RecoverAllLeases(DFSClient dfs, Path path)
		{
			string pathStr = path.ToString();
			HdfsFileStatus status = dfs.GetFileInfo(pathStr);
			if (!status.IsDir())
			{
				dfs.RecoverLease(pathStr);
				return;
			}
			byte[] prev = HdfsFileStatus.EmptyName;
			DirectoryListing dirList;
			do
			{
				dirList = dfs.ListPaths(pathStr, prev);
				HdfsFileStatus[] files = dirList.GetPartialListing();
				foreach (HdfsFileStatus f in files)
				{
					RecoverAllLeases(dfs, f.GetFullPath(path));
				}
				prev = dirList.GetLastName();
			}
			while (dirList.HasMore());
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void UpgradeAndVerify(MiniDFSCluster.Builder bld, TestDFSUpgradeFromImage.ClusterVerifier
			 verifier)
		{
			MiniDFSCluster cluster = null;
			try
			{
				bld.Format(false).StartupOption(HdfsServerConstants.StartupOption.Upgrade).ClusterId
					("testClusterId");
				cluster = bld.Build();
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				DFSClient dfsClient = dfs.dfs;
				//Safemode will be off only after upgrade is complete. Wait for it.
				while (dfsClient.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeGet))
				{
					Log.Info("Waiting for SafeMode to be OFF.");
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
					}
				}
				RecoverAllLeases(dfsClient, new Path("/"));
				VerifyFileSystem(dfs);
				if (verifier != null)
				{
					verifier.VerifyClusterPostUpgrade(cluster);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test upgrade from a 1.x image with some blocksBeingWritten</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgradeFromRel1BBWImage()
		{
			UnpackStorage(Hadoop1BbwImage, HadoopDfsDirTxt);
			Configuration conf = new Configuration(upgradeConf);
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, Runtime.GetProperty("test.build.data"
				) + FilePath.separator + "dfs" + FilePath.separator + "data" + FilePath.separator
				 + "data1");
			UpgradeAndVerify(new MiniDFSCluster.Builder(conf).NumDataNodes(1).EnableManagedDfsDirsRedundancy
				(false).ManageDataDfsDirs(false), null);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPreserveEditLogs()
		{
			UnpackStorage(Hadoop252Image, HadoopDfsDirTxt);
			Configuration conf = new HdfsConfiguration();
			conf = UpgradeUtilities.InitializeStorageStateConf(1, conf);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(
				false).ManageDataDfsDirs(false).ManageNameDfsDirs(false).StartupOption(HdfsServerConstants.StartupOption
				.Upgrade).Build();
			DFSInotifyEventInputStream ieis = cluster.GetFileSystem().GetInotifyEventStream(0
				);
			EventBatch batch;
			Event.CreateEvent ce;
			Event.RenameEvent re;
			// mkdir /input
			batch = TestDFSInotifyEventInputStream.WaitForNextEvents(ieis);
			NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
			NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
				.Create);
			ce = (Event.CreateEvent)batch.GetEvents()[0];
			NUnit.Framework.Assert.AreEqual(ce.GetPath(), "/input");
			// mkdir /input/dir1~5
			for (int i = 1; i <= 5; i++)
			{
				batch = TestDFSInotifyEventInputStream.WaitForNextEvents(ieis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Create);
				ce = (Event.CreateEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.AreEqual(ce.GetPath(), "/input/dir" + i);
			}
			// copyFromLocal randome_file_1~2 /input/dir1~2
			for (int i_1 = 1; i_1 <= 2; i_1++)
			{
				batch = TestDFSInotifyEventInputStream.WaitForNextEvents(ieis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				if (batch.GetEvents()[0].GetEventType() != Event.EventType.Create)
				{
					FSImage.Log.Debug(string.Empty);
				}
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Create);
				// copyFromLocal randome_file_1 /input/dir1, CLOSE
				batch = TestDFSInotifyEventInputStream.WaitForNextEvents(ieis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Close);
				// copyFromLocal randome_file_1 /input/dir1, CLOSE
				batch = TestDFSInotifyEventInputStream.WaitForNextEvents(ieis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Rename);
				re = (Event.RenameEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.AreEqual(re.GetDstPath(), "/input/dir" + i_1 + "/randome_file_"
					 + i_1);
			}
			// mv /input/dir1/randome_file_1 /input/dir3/randome_file_3
			long txIDBeforeRename = batch.GetTxid();
			batch = TestDFSInotifyEventInputStream.WaitForNextEvents(ieis);
			NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
			NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
				.Rename);
			re = (Event.RenameEvent)batch.GetEvents()[0];
			NUnit.Framework.Assert.AreEqual(re.GetDstPath(), "/input/dir3/randome_file_3");
			// rmdir /input/dir1
			batch = TestDFSInotifyEventInputStream.WaitForNextEvents(ieis);
			NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
			NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
				.Unlink);
			NUnit.Framework.Assert.AreEqual(((Event.UnlinkEvent)batch.GetEvents()[0]).GetPath
				(), "/input/dir1");
			long lastTxID = batch.GetTxid();
			// Start inotify from the tx before rename /input/dir1/randome_file_1
			ieis = cluster.GetFileSystem().GetInotifyEventStream(txIDBeforeRename);
			batch = TestDFSInotifyEventInputStream.WaitForNextEvents(ieis);
			NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
			NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
				.Rename);
			re = (Event.RenameEvent)batch.GetEvents()[0];
			NUnit.Framework.Assert.AreEqual(re.GetDstPath(), "/input/dir3/randome_file_3");
			// Try to read beyond available edits
			ieis = cluster.GetFileSystem().GetInotifyEventStream(lastTxID + 1);
			NUnit.Framework.Assert.IsNull(ieis.Poll());
			cluster.Shutdown();
		}
	}
}
