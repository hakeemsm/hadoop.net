using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Cli;
using Org.Apache.Hadoop.Cli.Util;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Startup and checkpoint tests</summary>
	public class TestStorageRestore
	{
		public const string NameNodeHost = "localhost:";

		public const string NameNodeHttpHost = "0.0.0.0:";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.TestStorageRestore
			).FullName);

		private Configuration config;

		private FilePath hdfsDir = null;

		internal const long seed = unchecked((long)(0xAAAAEEFL));

		internal const int blockSize = 4096;

		internal const int fileSize = 8192;

		private FilePath path1;

		private FilePath path2;

		private FilePath path3;

		private MiniDFSCluster cluster;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUpNameDirs()
		{
			config = new HdfsConfiguration();
			hdfsDir = new FilePath(MiniDFSCluster.GetBaseDirectory()).GetCanonicalFile();
			if (hdfsDir.Exists() && !FileUtil.FullyDelete(hdfsDir))
			{
				throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
			}
			hdfsDir.Mkdirs();
			path1 = new FilePath(hdfsDir, "name1");
			path2 = new FilePath(hdfsDir, "name2");
			path3 = new FilePath(hdfsDir, "name3");
			path1.Mkdir();
			path2.Mkdir();
			path3.Mkdir();
			if (!path2.Exists() || !path3.Exists() || !path1.Exists())
			{
				throw new IOException("Couldn't create dfs.name dirs in " + hdfsDir.GetAbsolutePath
					());
			}
			string dfs_name_dir = new string(path1.GetPath() + "," + path2.GetPath());
			System.Console.Out.WriteLine("configuring hdfsdir is " + hdfsDir.GetAbsolutePath(
				) + "; dfs_name_dir = " + dfs_name_dir + ";dfs_name_edits_dir(only)=" + path3.GetPath
				());
			config.Set(DFSConfigKeys.DfsNamenodeNameDirKey, dfs_name_dir);
			config.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, dfs_name_dir + "," + path3.GetPath
				());
			config.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, new FilePath(hdfsDir, "secondary"
				).GetPath());
			FileSystem.SetDefaultUri(config, "hdfs://" + NameNodeHost + "0");
			config.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, "0.0.0.0:0");
			// set the restore feature on
			config.SetBoolean(DFSConfigKeys.DfsNamenodeNameDirRestoreKey, true);
		}

		/// <summary>invalidate storage by removing the second and third storage directories</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void InvalidateStorage(FSImage fi, ICollection<FilePath> filesToInvalidate
			)
		{
			AList<Storage.StorageDirectory> al = new AList<Storage.StorageDirectory>(2);
			IEnumerator<Storage.StorageDirectory> it = fi.GetStorage().DirIterator();
			while (it.HasNext())
			{
				Storage.StorageDirectory sd = it.Next();
				if (filesToInvalidate.Contains(sd.GetRoot()))
				{
					Log.Info("causing IO error on " + sd.GetRoot());
					al.AddItem(sd);
				}
			}
			// simulate an error
			fi.GetStorage().ReportErrorsOnDirectories(al);
			foreach (JournalSet.JournalAndStream j in fi.GetEditLog().GetJournals())
			{
				if (j.GetManager() is FileJournalManager)
				{
					FileJournalManager fm = (FileJournalManager)j.GetManager();
					if (fm.GetStorageDirectory().GetRoot().Equals(path2) || fm.GetStorageDirectory().
						GetRoot().Equals(path3))
					{
						EditLogOutputStream mockStream = Org.Mockito.Mockito.Spy(j.GetCurrentStream());
						j.SetCurrentStreamForTests(mockStream);
						Org.Mockito.Mockito.DoThrow(new IOException("Injected fault: write")).When(mockStream
							).Write(Org.Mockito.Mockito.AnyObject<FSEditLogOp>());
					}
				}
			}
		}

		/// <summary>test</summary>
		private void PrintStorages(FSImage image)
		{
			FSImageTestUtil.LogStorageContents(Log, image.GetStorage());
		}

		/// <summary>
		/// test
		/// 1.
		/// </summary>
		/// <remarks>
		/// test
		/// 1. create DFS cluster with 3 storage directories - 2 EDITS_IMAGE, 1 EDITS
		/// 2. create a cluster and write a file
		/// 3. corrupt/disable one storage (or two) by removing
		/// 4. run doCheckpoint - it will fail on removed dirs (which
		/// will invalidate the storages)
		/// 5. write another file
		/// 6. check that edits and fsimage differ
		/// 7. run doCheckpoint
		/// 8. verify that all the image and edits files are the same.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStorageRestore()
		{
			int numDatanodes = 0;
			cluster = new MiniDFSCluster.Builder(config).NumDataNodes(numDatanodes).ManageNameDfsDirs
				(false).Build();
			cluster.WaitActive();
			SecondaryNameNode secondary = new SecondaryNameNode(config);
			System.Console.Out.WriteLine("****testStorageRestore: Cluster and SNN started");
			PrintStorages(cluster.GetNameNode().GetFSImage());
			FileSystem fs = cluster.GetFileSystem();
			Path path = new Path("/", "test");
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(path));
			System.Console.Out.WriteLine("****testStorageRestore: dir 'test' created, invalidating storage..."
				);
			InvalidateStorage(cluster.GetNameNode().GetFSImage(), ImmutableSet.Of(path2, path3
				));
			PrintStorages(cluster.GetNameNode().GetFSImage());
			System.Console.Out.WriteLine("****testStorageRestore: storage invalidated");
			path = new Path("/", "test1");
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(path));
			System.Console.Out.WriteLine("****testStorageRestore: dir 'test1' created");
			// We did another edit, so the still-active directory at 'path1'
			// should now differ from the others
			FSImageTestUtil.AssertFileContentsDifferent(2, new FilePath(path1, "current/" + NNStorage.GetInProgressEditsFileName
				(1)), new FilePath(path2, "current/" + NNStorage.GetInProgressEditsFileName(1)), 
				new FilePath(path3, "current/" + NNStorage.GetInProgressEditsFileName(1)));
			FSImageTestUtil.AssertFileContentsSame(new FilePath(path2, "current/" + NNStorage.GetInProgressEditsFileName
				(1)), new FilePath(path3, "current/" + NNStorage.GetInProgressEditsFileName(1)));
			System.Console.Out.WriteLine("****testStorageRestore: checkfiles(false) run");
			secondary.DoCheckpoint();
			///should enable storage..
			// We should have a checkpoint through txid 4 in the two image dirs
			// (txid=4 for BEGIN, mkdir, mkdir, END)
			FSImageTestUtil.AssertFileContentsSame(new FilePath(path1, "current/" + NNStorage.GetImageFileName
				(4)), new FilePath(path2, "current/" + NNStorage.GetImageFileName(4)));
			NUnit.Framework.Assert.IsFalse("Should not have any image in an edits-only directory"
				, new FilePath(path3, "current/" + NNStorage.GetImageFileName(4)).Exists());
			// Should have finalized logs in the directory that didn't fail
			NUnit.Framework.Assert.IsTrue("Should have finalized logs in the directory that didn't fail"
				, new FilePath(path1, "current/" + NNStorage.GetFinalizedEditsFileName(1, 4)).Exists
				());
			// Should not have finalized logs in the failed directories
			NUnit.Framework.Assert.IsFalse("Should not have finalized logs in the failed directories"
				, new FilePath(path2, "current/" + NNStorage.GetFinalizedEditsFileName(1, 4)).Exists
				());
			NUnit.Framework.Assert.IsFalse("Should not have finalized logs in the failed directories"
				, new FilePath(path3, "current/" + NNStorage.GetFinalizedEditsFileName(1, 4)).Exists
				());
			// The new log segment should be in all of the directories.
			FSImageTestUtil.AssertFileContentsSame(new FilePath(path1, "current/" + NNStorage.GetInProgressEditsFileName
				(5)), new FilePath(path2, "current/" + NNStorage.GetInProgressEditsFileName(5)), 
				new FilePath(path3, "current/" + NNStorage.GetInProgressEditsFileName(5)));
			string md5BeforeEdit = FSImageTestUtil.GetFileMD5(new FilePath(path1, "current/" 
				+ NNStorage.GetInProgressEditsFileName(5)));
			// The original image should still be the previously failed image
			// directory after it got restored, since it's still useful for
			// a recovery!
			FSImageTestUtil.AssertFileContentsSame(new FilePath(path1, "current/" + NNStorage.GetImageFileName
				(0)), new FilePath(path2, "current/" + NNStorage.GetImageFileName(0)));
			// Do another edit to verify that all the logs are active.
			path = new Path("/", "test2");
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(path));
			// Logs should be changed by the edit.
			string md5AfterEdit = FSImageTestUtil.GetFileMD5(new FilePath(path1, "current/" +
				 NNStorage.GetInProgressEditsFileName(5)));
			NUnit.Framework.Assert.IsFalse(md5BeforeEdit.Equals(md5AfterEdit));
			// And all logs should be changed.
			FSImageTestUtil.AssertFileContentsSame(new FilePath(path1, "current/" + NNStorage.GetInProgressEditsFileName
				(5)), new FilePath(path2, "current/" + NNStorage.GetInProgressEditsFileName(5)), 
				new FilePath(path3, "current/" + NNStorage.GetInProgressEditsFileName(5)));
			secondary.Shutdown();
			cluster.Shutdown();
			// All logs should be finalized by clean shutdown
			FSImageTestUtil.AssertFileContentsSame(new FilePath(path1, "current/" + NNStorage.GetFinalizedEditsFileName
				(5, 7)), new FilePath(path2, "current/" + NNStorage.GetFinalizedEditsFileName(5, 
				7)), new FilePath(path3, "current/" + NNStorage.GetFinalizedEditsFileName(5, 7))
				);
		}

		/// <summary>Test dfsadmin -restoreFailedStorage command</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDfsAdminCmd()
		{
			cluster = new MiniDFSCluster.Builder(config).NumDataNodes(2).ManageNameDfsDirs(false
				).Build();
			cluster.WaitActive();
			try
			{
				FSImage fsi = cluster.GetNameNode().GetFSImage();
				// it is started with dfs.namenode.name.dir.restore set to true (in SetUp())
				bool restore = fsi.GetStorage().GetRestoreFailedStorage();
				Log.Info("Restore is " + restore);
				NUnit.Framework.Assert.AreEqual(restore, true);
				// now run DFSAdmnin command
				string cmd = "-fs NAMENODE -restoreFailedStorage false";
				string namenode = config.Get(DFSConfigKeys.FsDefaultNameKey, "file:///");
				CommandExecutor executor = new CLITestCmdDFS(cmd, new CLICommandDFSAdmin()).GetExecutor
					(namenode);
				executor.ExecuteCommand(cmd);
				restore = fsi.GetStorage().GetRestoreFailedStorage();
				NUnit.Framework.Assert.IsFalse("After set true call restore is " + restore, restore
					);
				// run one more time - to set it to true again
				cmd = "-fs NAMENODE -restoreFailedStorage true";
				executor.ExecuteCommand(cmd);
				restore = fsi.GetStorage().GetRestoreFailedStorage();
				NUnit.Framework.Assert.IsTrue("After set false call restore is " + restore, restore
					);
				// run one more time - no change in value
				cmd = "-fs NAMENODE -restoreFailedStorage check";
				CommandExecutor.Result cmdResult = executor.ExecuteCommand(cmd);
				restore = fsi.GetStorage().GetRestoreFailedStorage();
				NUnit.Framework.Assert.IsTrue("After check call restore is " + restore, restore);
				string commandOutput = cmdResult.GetCommandOutput();
				commandOutput.Trim();
				NUnit.Framework.Assert.IsTrue(commandOutput.Contains("restoreFailedStorage is set to true"
					));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test to simulate interleaved checkpointing by 2 2NNs after a storage
		/// directory has been taken offline.
		/// </summary>
		/// <remarks>
		/// Test to simulate interleaved checkpointing by 2 2NNs after a storage
		/// directory has been taken offline. The first will cause the directory to
		/// come back online, but it won't have any valid contents. The second 2NN will
		/// then try to perform a checkpoint. The NN should not serve up the image or
		/// edits from the restored (empty) dir.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleSecondaryCheckpoint()
		{
			SecondaryNameNode secondary = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(config).NumDataNodes(1).ManageNameDfsDirs(false
					).Build();
				cluster.WaitActive();
				secondary = new SecondaryNameNode(config);
				FSImage fsImage = cluster.GetNameNode().GetFSImage();
				PrintStorages(fsImage);
				FileSystem fs = cluster.GetFileSystem();
				Path testPath = new Path("/", "test");
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(testPath));
				PrintStorages(fsImage);
				// Take name1 offline
				InvalidateStorage(fsImage, ImmutableSet.Of(path1));
				// Simulate a 2NN beginning a checkpoint, but not finishing. This will
				// cause name1 to be restored.
				cluster.GetNameNodeRpc().RollEditLog();
				PrintStorages(fsImage);
				// Now another 2NN comes along to do a full checkpoint.
				secondary.DoCheckpoint();
				PrintStorages(fsImage);
				// The created file should still exist in the in-memory FS state after the
				// checkpoint.
				NUnit.Framework.Assert.IsTrue("path exists before restart", fs.Exists(testPath));
				secondary.Shutdown();
				// Restart the NN so it reloads the edits from on-disk.
				cluster.RestartNameNode();
				// The created file should still exist after the restart.
				NUnit.Framework.Assert.IsTrue("path should still exist after restart", fs.Exists(
					testPath));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				if (secondary != null)
				{
					secondary.Shutdown();
				}
			}
		}

		/// <summary>1.</summary>
		/// <remarks>
		/// 1. create DFS cluster with 3 storage directories
		/// - 2 EDITS_IMAGE(name1, name2), 1 EDITS(name3)
		/// 2. create a file
		/// 3. corrupt/disable name2 and name3 by removing rwx permission
		/// 4. run doCheckpoint
		/// - will fail on removed dirs (which invalidates them)
		/// 5. write another file
		/// 6. check there is only one healthy storage dir
		/// 7. run doCheckpoint - recover should fail but checkpoint should succeed
		/// 8. check there is still only one healthy storage dir
		/// 9. restore the access permission for name2 and name 3, run checkpoint again
		/// 10.verify there are 3 healthy storage dirs.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStorageRestoreFailure()
		{
			SecondaryNameNode secondary = null;
			// On windows, revoking write+execute permission on name2 does not
			// prevent us from creating files in name2\current. Hence we revoke
			// permissions on name2\current for the test.
			string nameDir2 = Shell.Windows ? (new FilePath(path2, "current").GetAbsolutePath
				()) : path2.ToString();
			string nameDir3 = Shell.Windows ? (new FilePath(path3, "current").GetAbsolutePath
				()) : path3.ToString();
			try
			{
				cluster = new MiniDFSCluster.Builder(config).NumDataNodes(0).ManageNameDfsDirs(false
					).Build();
				cluster.WaitActive();
				secondary = new SecondaryNameNode(config);
				PrintStorages(cluster.GetNameNode().GetFSImage());
				FileSystem fs = cluster.GetFileSystem();
				Path path = new Path("/", "test");
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(path));
				// invalidate storage by removing rwx permission from name2 and name3
				NUnit.Framework.Assert.IsTrue(FileUtil.Chmod(nameDir2, "000") == 0);
				NUnit.Framework.Assert.IsTrue(FileUtil.Chmod(nameDir3, "000") == 0);
				secondary.DoCheckpoint();
				// should remove name2 and name3
				PrintStorages(cluster.GetNameNode().GetFSImage());
				path = new Path("/", "test1");
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(path));
				System.Diagnostics.Debug.Assert((cluster.GetNameNode().GetFSImage().GetStorage().
					GetNumStorageDirs() == 1));
				secondary.DoCheckpoint();
				// shouldn't be able to restore name 2 and 3
				System.Diagnostics.Debug.Assert((cluster.GetNameNode().GetFSImage().GetStorage().
					GetNumStorageDirs() == 1));
				NUnit.Framework.Assert.IsTrue(FileUtil.Chmod(nameDir2, "755") == 0);
				NUnit.Framework.Assert.IsTrue(FileUtil.Chmod(nameDir3, "755") == 0);
				secondary.DoCheckpoint();
				// should restore name 2 and 3
				System.Diagnostics.Debug.Assert((cluster.GetNameNode().GetFSImage().GetStorage().
					GetNumStorageDirs() == 3));
			}
			finally
			{
				if (path2.Exists())
				{
					FileUtil.Chmod(nameDir2, "755");
				}
				if (path3.Exists())
				{
					FileUtil.Chmod(nameDir3, "755");
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				if (secondary != null)
				{
					secondary.Shutdown();
				}
			}
		}
	}
}
