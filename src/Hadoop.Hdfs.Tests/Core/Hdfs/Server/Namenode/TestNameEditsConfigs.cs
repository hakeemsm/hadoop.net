using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// This class tests various combinations of dfs.namenode.name.dir
	/// and dfs.namenode.edits.dir configurations.
	/// </summary>
	public class TestNameEditsConfigs
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(FSEditLog));

		internal const long Seed = unchecked((long)(0xDEADBEEFL));

		internal const int BlockSize = 4096;

		internal const int FileSize = 8192;

		internal const int NumDataNodes = 3;

		internal const string FileImage = "current/fsimage";

		internal const string FileEdits = "current/edits";

		internal short replication = 3;

		private readonly FilePath base_dir = new FilePath(PathUtils.GetTestDir(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.TestNameEditsConfigs
			)), "dfs");

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			if (base_dir.Exists() && !FileUtil.FullyDelete(base_dir))
			{
				throw new IOException("Cannot remove directory " + base_dir);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FileSystem fileSys, Path name, int repl)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)repl, BlockSize);
			byte[] buffer = new byte[FileSize];
			Random rand = new Random(Seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckImageAndEditsFilesExistence(FilePath dir, bool shouldHaveImages
			, bool shouldHaveEdits)
		{
			FSImageTransactionalStorageInspector ins = Inspect(dir);
			if (shouldHaveImages)
			{
				NUnit.Framework.Assert.IsTrue("Expect images in " + dir, ins.foundImages.Count > 
					0);
			}
			else
			{
				NUnit.Framework.Assert.IsTrue("Expect no images in " + dir, ins.foundImages.IsEmpty
					());
			}
			IList<FileJournalManager.EditLogFile> editlogs = FileJournalManager.MatchEditLogs
				(new FilePath(dir, "current").ListFiles());
			if (shouldHaveEdits)
			{
				NUnit.Framework.Assert.IsTrue("Expect edits in " + dir, editlogs.Count > 0);
			}
			else
			{
				NUnit.Framework.Assert.IsTrue("Expect no edits in " + dir, editlogs.IsEmpty());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckFile(FileSystem fileSys, Path name, int repl)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			int replication = fileSys.GetFileStatus(name).GetReplication();
			NUnit.Framework.Assert.AreEqual("replication for " + name, repl, replication);
			long size = fileSys.GetContentSummary(name).GetLength();
			NUnit.Framework.Assert.AreEqual("file size for " + name, size, FileSize);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual SecondaryNameNode StartSecondaryNameNode(Configuration conf)
		{
			conf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, "0.0.0.0:0");
			return new SecondaryNameNode(conf);
		}

		/// <summary>
		/// Test various configuration options of dfs.namenode.name.dir and dfs.namenode.edits.dir
		/// The test creates files and restarts cluster with different configs.
		/// </summary>
		/// <remarks>
		/// Test various configuration options of dfs.namenode.name.dir and dfs.namenode.edits.dir
		/// The test creates files and restarts cluster with different configs.
		/// 1. Starts cluster with shared name and edits dirs
		/// 2. Restarts cluster by adding additional (different) name and edits dirs
		/// 3. Restarts cluster by removing shared name and edits dirs by allowing to
		/// start using separate name and edits dirs
		/// 4. Restart cluster by adding shared directory again, but make sure we
		/// do not read any stale image or edits.
		/// All along the test, we create and delete files at reach restart to make
		/// sure we are reading proper edits and image.
		/// </remarks>
		/// <exception cref="System.Exception"></exception>
		[NUnit.Framework.Test]
		public virtual void TestNameEditsConfigs()
		{
			Path file1 = new Path("TestNameEditsConfigs1");
			Path file2 = new Path("TestNameEditsConfigs2");
			Path file3 = new Path("TestNameEditsConfigs3");
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			Configuration conf = null;
			FileSystem fileSys = null;
			FilePath newNameDir = new FilePath(base_dir, "name");
			FilePath newEditsDir = new FilePath(base_dir, "edits");
			FilePath nameAndEdits = new FilePath(base_dir, "name_and_edits");
			FilePath checkpointNameDir = new FilePath(base_dir, "secondname");
			FilePath checkpointEditsDir = new FilePath(base_dir, "secondedits");
			FilePath checkpointNameAndEdits = new FilePath(base_dir, "second_name_and_edits");
			ImmutableList<FilePath> allCurrentDirs = ImmutableList.Of(new FilePath(nameAndEdits
				, "current"), new FilePath(newNameDir, "current"), new FilePath(newEditsDir, "current"
				), new FilePath(checkpointNameAndEdits, "current"), new FilePath(checkpointNameDir
				, "current"), new FilePath(checkpointEditsDir, "current"));
			ImmutableList<FilePath> imageCurrentDirs = ImmutableList.Of(new FilePath(nameAndEdits
				, "current"), new FilePath(newNameDir, "current"), new FilePath(checkpointNameAndEdits
				, "current"), new FilePath(checkpointNameDir, "current"));
			// Start namenode with same dfs.namenode.name.dir and dfs.namenode.edits.dir
			conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameAndEdits.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameAndEdits.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, checkpointNameAndEdits.GetPath
				());
			conf.Set(DFSConfigKeys.DfsNamenodeCheckpointEditsDirKey, checkpointNameAndEdits.GetPath
				());
			replication = (short)conf.GetInt(DFSConfigKeys.DfsReplicationKey, 3);
			// Manage our own dfs directories
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).ManageNameDfsDirs
				(false).Build();
			cluster.WaitActive();
			secondary = StartSecondaryNameNode(conf);
			fileSys = cluster.GetFileSystem();
			try
			{
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file1));
				WriteFile(fileSys, file1, replication);
				CheckFile(fileSys, file1, replication);
				secondary.DoCheckpoint();
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
				secondary.Shutdown();
			}
			// Start namenode with additional dfs.namenode.name.dir and dfs.namenode.edits.dir
			conf = new HdfsConfiguration();
			NUnit.Framework.Assert.IsTrue(newNameDir.Mkdir());
			NUnit.Framework.Assert.IsTrue(newEditsDir.Mkdir());
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameAndEdits.GetPath() + "," + newNameDir
				.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameAndEdits.GetPath() + "," + newEditsDir
				.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, checkpointNameDir.GetPath() +
				 "," + checkpointNameAndEdits.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeCheckpointEditsDirKey, checkpointEditsDir.GetPath
				() + "," + checkpointNameAndEdits.GetPath());
			replication = (short)conf.GetInt(DFSConfigKeys.DfsReplicationKey, 3);
			// Manage our own dfs directories. Do not format.
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Format(false
				).ManageNameDfsDirs(false).Build();
			cluster.WaitActive();
			secondary = StartSecondaryNameNode(conf);
			fileSys = cluster.GetFileSystem();
			try
			{
				NUnit.Framework.Assert.IsTrue(fileSys.Exists(file1));
				CheckFile(fileSys, file1, replication);
				CleanupFile(fileSys, file1);
				WriteFile(fileSys, file2, replication);
				CheckFile(fileSys, file2, replication);
				secondary.DoCheckpoint();
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
				secondary.Shutdown();
			}
			FSImageTestUtil.AssertParallelFilesAreIdentical(allCurrentDirs, ImmutableSet.Of("VERSION"
				));
			FSImageTestUtil.AssertSameNewestImage(imageCurrentDirs);
			// Now remove common directory both have and start namenode with 
			// separate name and edits dirs
			conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, newNameDir.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, newEditsDir.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, checkpointNameDir.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeCheckpointEditsDirKey, checkpointEditsDir.GetPath
				());
			replication = (short)conf.GetInt(DFSConfigKeys.DfsReplicationKey, 3);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Format(false
				).ManageNameDfsDirs(false).Build();
			cluster.WaitActive();
			secondary = StartSecondaryNameNode(conf);
			fileSys = cluster.GetFileSystem();
			try
			{
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file1));
				NUnit.Framework.Assert.IsTrue(fileSys.Exists(file2));
				CheckFile(fileSys, file2, replication);
				CleanupFile(fileSys, file2);
				WriteFile(fileSys, file3, replication);
				CheckFile(fileSys, file3, replication);
				secondary.DoCheckpoint();
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
				secondary.Shutdown();
			}
			// No edit logs in new name dir
			CheckImageAndEditsFilesExistence(newNameDir, true, false);
			CheckImageAndEditsFilesExistence(newEditsDir, false, true);
			CheckImageAndEditsFilesExistence(checkpointNameDir, true, false);
			CheckImageAndEditsFilesExistence(checkpointEditsDir, false, true);
			// Add old name_and_edits dir. File system should not read image or edits
			// from old dir
			NUnit.Framework.Assert.IsTrue(FileUtil.FullyDelete(new FilePath(nameAndEdits, "current"
				)));
			NUnit.Framework.Assert.IsTrue(FileUtil.FullyDelete(new FilePath(checkpointNameAndEdits
				, "current")));
			conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameAndEdits.GetPath() + "," + newNameDir
				.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameAndEdits + "," + newEditsDir.GetPath
				());
			conf.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, checkpointNameDir.GetPath() +
				 "," + checkpointNameAndEdits.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeCheckpointEditsDirKey, checkpointEditsDir.GetPath
				() + "," + checkpointNameAndEdits.GetPath());
			replication = (short)conf.GetInt(DFSConfigKeys.DfsReplicationKey, 3);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Format(false
				).ManageNameDfsDirs(false).Build();
			cluster.WaitActive();
			secondary = StartSecondaryNameNode(conf);
			fileSys = cluster.GetFileSystem();
			try
			{
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file1));
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file2));
				NUnit.Framework.Assert.IsTrue(fileSys.Exists(file3));
				CheckFile(fileSys, file3, replication);
				secondary.DoCheckpoint();
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
				secondary.Shutdown();
			}
			CheckImageAndEditsFilesExistence(nameAndEdits, true, true);
			CheckImageAndEditsFilesExistence(checkpointNameAndEdits, true, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private FSImageTransactionalStorageInspector Inspect(FilePath storageDir)
		{
			return FSImageTestUtil.InspectStorageDirectory(new FilePath(storageDir, "current"
				), NNStorage.NameNodeDirType.ImageAndEdits);
		}

		/// <summary>Test edits.dir.required configuration options.</summary>
		/// <remarks>
		/// Test edits.dir.required configuration options.
		/// 1. Directory present in dfs.namenode.edits.dir.required but not in
		/// dfs.namenode.edits.dir. Expected to fail.
		/// 2. Directory present in both dfs.namenode.edits.dir.required and
		/// dfs.namenode.edits.dir. Expected to succeed.
		/// 3. Directory present only in dfs.namenode.edits.dir. Expected to
		/// succeed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNameEditsRequiredConfigs()
		{
			MiniDFSCluster cluster = null;
			FilePath nameAndEditsDir = new FilePath(base_dir, "name_and_edits");
			FilePath nameAndEditsDir2 = new FilePath(base_dir, "name_and_edits2");
			FilePath nameDir = new FilePath(base_dir, "name");
			// 1
			// Bad configuration. Add a directory to dfs.namenode.edits.dir.required
			// without adding it to dfs.namenode.edits.dir.
			try
			{
				Configuration conf = new HdfsConfiguration();
				conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDir.GetAbsolutePath());
				conf.Set(DFSConfigKeys.DfsNamenodeEditsDirRequiredKey, nameAndEditsDir2.ToURI().ToString
					());
				conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameAndEditsDir.ToURI().ToString()
					);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).ManageNameDfsDirs
					(false).Build();
				NUnit.Framework.Assert.Fail("Successfully started cluster but should not have been able to."
					);
			}
			catch (ArgumentException iae)
			{
				// expect to fail
				Log.Info("EXPECTED: cluster start failed due to bad configuration" + iae);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				cluster = null;
			}
			// 2
			// Good configuration. Add a directory to both dfs.namenode.edits.dir.required
			// and dfs.namenode.edits.dir.
			try
			{
				Configuration conf = new HdfsConfiguration();
				conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDir.GetAbsolutePath());
				conf.SetStrings(DFSConfigKeys.DfsNamenodeEditsDirKey, nameAndEditsDir.ToURI().ToString
					(), nameAndEditsDir2.ToURI().ToString());
				conf.Set(DFSConfigKeys.DfsNamenodeEditsDirRequiredKey, nameAndEditsDir2.ToURI().ToString
					());
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).ManageNameDfsDirs
					(false).Build();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			// 3
			// Good configuration. Adds a directory to dfs.namenode.edits.dir but not to
			// dfs.namenode.edits.dir.required.
			try
			{
				Configuration conf = new HdfsConfiguration();
				conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDir.GetAbsolutePath());
				conf.SetStrings(DFSConfigKeys.DfsNamenodeEditsDirKey, nameAndEditsDir.ToURI().ToString
					(), nameAndEditsDir2.ToURI().ToString());
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).ManageNameDfsDirs
					(false).Build();
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
		/// Test various configuration options of dfs.namenode.name.dir and dfs.namenode.edits.dir
		/// This test tries to simulate failure scenarios.
		/// </summary>
		/// <remarks>
		/// Test various configuration options of dfs.namenode.name.dir and dfs.namenode.edits.dir
		/// This test tries to simulate failure scenarios.
		/// 1. Start cluster with shared name and edits dir
		/// 2. Restart cluster by adding separate name and edits dirs
		/// 3. Restart cluster by removing shared name and edits dir
		/// 4. Restart cluster with old shared name and edits dir, but only latest
		/// name dir. This should fail since we don't have latest edits dir
		/// 5. Restart cluster with old shared name and edits dir, but only latest
		/// edits dir. This should succeed since the latest edits will have
		/// segments leading all the way from the image in name_and_edits.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNameEditsConfigsFailure()
		{
			Path file1 = new Path("TestNameEditsConfigs1");
			Path file2 = new Path("TestNameEditsConfigs2");
			Path file3 = new Path("TestNameEditsConfigs3");
			MiniDFSCluster cluster = null;
			Configuration conf = null;
			FileSystem fileSys = null;
			FilePath nameOnlyDir = new FilePath(base_dir, "name");
			FilePath editsOnlyDir = new FilePath(base_dir, "edits");
			FilePath nameAndEditsDir = new FilePath(base_dir, "name_and_edits");
			// 1
			// Start namenode with same dfs.namenode.name.dir and dfs.namenode.edits.dir
			conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameAndEditsDir.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameAndEditsDir.GetPath());
			replication = (short)conf.GetInt(DFSConfigKeys.DfsReplicationKey, 3);
			try
			{
				// Manage our own dfs directories
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).ManageNameDfsDirs
					(false).Build();
				cluster.WaitActive();
				// Check that the dir has a VERSION file
				NUnit.Framework.Assert.IsTrue(new FilePath(nameAndEditsDir, "current/VERSION").Exists
					());
				fileSys = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file1));
				WriteFile(fileSys, file1, replication);
				CheckFile(fileSys, file1, replication);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
			// 2
			// Start namenode with additional dfs.namenode.name.dir and dfs.namenode.edits.dir
			conf = new HdfsConfiguration();
			NUnit.Framework.Assert.IsTrue(nameOnlyDir.Mkdir());
			NUnit.Framework.Assert.IsTrue(editsOnlyDir.Mkdir());
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameAndEditsDir.GetPath() + "," + nameOnlyDir
				.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameAndEditsDir.GetPath() + "," + 
				editsOnlyDir.GetPath());
			replication = (short)conf.GetInt(DFSConfigKeys.DfsReplicationKey, 3);
			try
			{
				// Manage our own dfs directories. Do not format.
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Format(false
					).ManageNameDfsDirs(false).Build();
				cluster.WaitActive();
				// Check that the dirs have a VERSION file
				NUnit.Framework.Assert.IsTrue(new FilePath(nameAndEditsDir, "current/VERSION").Exists
					());
				NUnit.Framework.Assert.IsTrue(new FilePath(nameOnlyDir, "current/VERSION").Exists
					());
				NUnit.Framework.Assert.IsTrue(new FilePath(editsOnlyDir, "current/VERSION").Exists
					());
				fileSys = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue(fileSys.Exists(file1));
				CheckFile(fileSys, file1, replication);
				CleanupFile(fileSys, file1);
				WriteFile(fileSys, file2, replication);
				CheckFile(fileSys, file2, replication);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
			// 3
			// Now remove common directory both have and start namenode with 
			// separate name and edits dirs
			try
			{
				conf = new HdfsConfiguration();
				conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameOnlyDir.GetPath());
				conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, editsOnlyDir.GetPath());
				replication = (short)conf.GetInt(DFSConfigKeys.DfsReplicationKey, 3);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Format(false
					).ManageNameDfsDirs(false).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsFalse(fileSys.Exists(file1));
				NUnit.Framework.Assert.IsTrue(fileSys.Exists(file2));
				CheckFile(fileSys, file2, replication);
				CleanupFile(fileSys, file2);
				WriteFile(fileSys, file3, replication);
				CheckFile(fileSys, file3, replication);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
			// 4
			// Add old shared directory for name and edits along with latest name
			conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameOnlyDir.GetPath() + "," + nameAndEditsDir
				.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameAndEditsDir.GetPath());
			replication = (short)conf.GetInt(DFSConfigKeys.DfsReplicationKey, 3);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Format(false
					).ManageNameDfsDirs(false).Build();
				NUnit.Framework.Assert.Fail("Successfully started cluster but should not have been able to."
					);
			}
			catch (IOException e)
			{
				// expect to fail
				Log.Info("EXPECTED: cluster start failed due to missing " + "latest edits dir", e
					);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				cluster = null;
			}
			// 5
			// Add old shared directory for name and edits along with latest edits. 
			// This is OK, since the latest edits will have segments leading all
			// the way from the image in name_and_edits.
			conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameAndEditsDir.GetPath());
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, editsOnlyDir.GetPath() + "," + nameAndEditsDir
				.GetPath());
			replication = (short)conf.GetInt(DFSConfigKeys.DfsReplicationKey, 3);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Format(false
					).ManageNameDfsDirs(false).Build();
				fileSys = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsFalse(fileSys.Exists(file1));
				NUnit.Framework.Assert.IsFalse(fileSys.Exists(file2));
				NUnit.Framework.Assert.IsTrue(fileSys.Exists(file3));
				CheckFile(fileSys, file3, replication);
				CleanupFile(fileSys, file3);
				WriteFile(fileSys, file3, replication);
				CheckFile(fileSys, file3, replication);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test dfs.namenode.checkpoint.dir and dfs.namenode.checkpoint.edits.dir
		/// should tolerate white space between values.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckPointDirsAreTrimmed()
		{
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			FilePath checkpointNameDir1 = new FilePath(base_dir, "chkptName1");
			FilePath checkpointEditsDir1 = new FilePath(base_dir, "chkptEdits1");
			FilePath checkpointNameDir2 = new FilePath(base_dir, "chkptName2");
			FilePath checkpointEditsDir2 = new FilePath(base_dir, "chkptEdits2");
			FilePath nameDir = new FilePath(base_dir, "name1");
			string whiteSpace = "  \n   \n  ";
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDir.GetPath());
			conf.SetStrings(DFSConfigKeys.DfsNamenodeCheckpointDirKey, whiteSpace + checkpointNameDir1
				.GetPath() + whiteSpace, whiteSpace + checkpointNameDir2.GetPath() + whiteSpace);
			conf.SetStrings(DFSConfigKeys.DfsNamenodeCheckpointEditsDirKey, whiteSpace + checkpointEditsDir1
				.GetPath() + whiteSpace, whiteSpace + checkpointEditsDir2.GetPath() + whiteSpace
				);
			cluster = new MiniDFSCluster.Builder(conf).ManageNameDfsDirs(false).NumDataNodes(
				3).Build();
			try
			{
				cluster.WaitActive();
				secondary = StartSecondaryNameNode(conf);
				secondary.DoCheckpoint();
				NUnit.Framework.Assert.IsTrue(DFSConfigKeys.DfsNamenodeNameDirKey + " must be trimmed "
					, checkpointNameDir1.Exists());
				NUnit.Framework.Assert.IsTrue(DFSConfigKeys.DfsNamenodeNameDirKey + " must be trimmed "
					, checkpointNameDir2.Exists());
				NUnit.Framework.Assert.IsTrue(DFSConfigKeys.DfsNamenodeCheckpointEditsDirKey + " must be trimmed "
					, checkpointEditsDir1.Exists());
				NUnit.Framework.Assert.IsTrue(DFSConfigKeys.DfsNamenodeCheckpointEditsDirKey + " must be trimmed "
					, checkpointEditsDir2.Exists());
			}
			finally
			{
				secondary.Shutdown();
				cluster.Shutdown();
			}
		}
	}
}
