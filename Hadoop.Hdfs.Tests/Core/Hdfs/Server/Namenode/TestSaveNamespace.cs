using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Mockito;
using Org.Mockito.Internal.Util.Reflection;
using Org.Mockito.Invocation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Test various failure scenarios during saveNamespace() operation.</summary>
	/// <remarks>
	/// Test various failure scenarios during saveNamespace() operation.
	/// Cases covered:
	/// <ol>
	/// <li>Recover from failure while saving into the second storage directory</li>
	/// <li>Recover from failure while moving current into lastcheckpoint.tmp</li>
	/// <li>Recover from failure while moving lastcheckpoint.tmp into
	/// previous.checkpoint</li>
	/// <li>Recover from failure while rolling edits file</li>
	/// </ol>
	/// </remarks>
	public class TestSaveNamespace
	{
		static TestSaveNamespace()
		{
			((Log4JLogger)FSImage.Log).GetLogger().SetLevel(Level.All);
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(TestSaveNamespace));

		private class FaultySaveImage : Org.Mockito.Stubbing.Answer<Void>
		{
			internal int count = 0;

			internal bool throwRTE = true;

			public FaultySaveImage(bool throwRTE)
			{
				// generate either a RuntimeException or IOException
				this.throwRTE = throwRTE;
			}

			/// <exception cref="System.Exception"/>
			public virtual Void Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				Storage.StorageDirectory sd = (Storage.StorageDirectory)args[1];
				if (count++ == 1)
				{
					Log.Info("Injecting fault for sd: " + sd);
					if (throwRTE)
					{
						throw new RuntimeException("Injected fault: saveFSImage second time");
					}
					else
					{
						throw new IOException("Injected fault: saveFSImage second time");
					}
				}
				Log.Info("Not injecting fault for sd: " + sd);
				return (Void)invocation.CallRealMethod();
			}
		}

		private enum Fault
		{
			SaveSecondFsimageRte,
			SaveSecondFsimageIoe,
			SaveAllFsimages,
			WriteStorageAll,
			WriteStorageOne
		}

		/// <exception cref="System.Exception"/>
		private void SaveNamespaceWithInjectedFault(TestSaveNamespace.Fault fault)
		{
			Configuration conf = GetConf();
			NameNode.InitMetrics(conf, HdfsServerConstants.NamenodeRole.Namenode);
			DFSTestUtil.FormatNameNode(conf);
			FSNamesystem fsn = FSNamesystem.LoadFromDisk(conf);
			// Replace the FSImage with a spy
			FSImage originalImage = fsn.GetFSImage();
			NNStorage storage = originalImage.GetStorage();
			NNStorage spyStorage = Org.Mockito.Mockito.Spy(storage);
			originalImage.storage = spyStorage;
			FSImage spyImage = Org.Mockito.Mockito.Spy(originalImage);
			Whitebox.SetInternalState(fsn, "fsImage", spyImage);
			bool shouldFail = false;
			switch (fault)
			{
				case TestSaveNamespace.Fault.SaveSecondFsimageRte:
				{
					// should we expect the save operation to fail
					// inject fault
					// The spy throws a RuntimeException when writing to the second directory
					Org.Mockito.Mockito.DoAnswer(new TestSaveNamespace.FaultySaveImage(true)).When(spyImage
						).SaveFSImage((SaveNamespaceContext)Matchers.AnyObject(), (Storage.StorageDirectory
						)Matchers.AnyObject(), (NNStorage.NameNodeFile)Matchers.AnyObject());
					shouldFail = false;
					break;
				}

				case TestSaveNamespace.Fault.SaveSecondFsimageIoe:
				{
					// The spy throws an IOException when writing to the second directory
					Org.Mockito.Mockito.DoAnswer(new TestSaveNamespace.FaultySaveImage(false)).When(spyImage
						).SaveFSImage((SaveNamespaceContext)Matchers.AnyObject(), (Storage.StorageDirectory
						)Matchers.AnyObject(), (NNStorage.NameNodeFile)Matchers.AnyObject());
					shouldFail = false;
					break;
				}

				case TestSaveNamespace.Fault.SaveAllFsimages:
				{
					// The spy throws IOException in all directories
					Org.Mockito.Mockito.DoThrow(new RuntimeException("Injected")).When(spyImage).SaveFSImage
						((SaveNamespaceContext)Matchers.AnyObject(), (Storage.StorageDirectory)Matchers.AnyObject
						(), (NNStorage.NameNodeFile)Matchers.AnyObject());
					shouldFail = true;
					break;
				}

				case TestSaveNamespace.Fault.WriteStorageAll:
				{
					// The spy throws an exception before writing any VERSION files
					Org.Mockito.Mockito.DoThrow(new RuntimeException("Injected")).When(spyStorage).WriteAll
						();
					shouldFail = true;
					break;
				}

				case TestSaveNamespace.Fault.WriteStorageOne:
				{
					// The spy throws on exception on one particular storage directory
					Org.Mockito.Mockito.DoAnswer(new TestSaveNamespace.FaultySaveImage(true)).When(spyStorage
						).WriteProperties((Storage.StorageDirectory)Matchers.AnyObject());
					// TODO: unfortunately this fails -- should be improved.
					// See HDFS-2173.
					shouldFail = true;
					break;
				}
			}
			try
			{
				DoAnEdit(fsn, 1);
				// Save namespace - this may fail, depending on fault injected
				fsn.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				try
				{
					fsn.SaveNamespace();
					if (shouldFail)
					{
						NUnit.Framework.Assert.Fail("Did not fail!");
					}
				}
				catch (Exception e)
				{
					if (!shouldFail)
					{
						throw;
					}
					else
					{
						Log.Info("Test caught expected exception", e);
					}
				}
				fsn.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
				// Should still be able to perform edits
				DoAnEdit(fsn, 2);
				// Now shut down and restart the namesystem
				originalImage.Close();
				fsn.Close();
				fsn = null;
				// Start a new namesystem, which should be able to recover
				// the namespace from the previous incarnation.
				fsn = FSNamesystem.LoadFromDisk(conf);
				// Make sure the image loaded including our edits.
				CheckEditExists(fsn, 1);
				CheckEditExists(fsn, 2);
			}
			finally
			{
				if (fsn != null)
				{
					fsn.Close();
				}
			}
		}

		/// <summary>
		/// Verify that a saveNamespace command brings faulty directories
		/// in fs.name.dir and fs.edit.dir back online.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestReinsertnamedirsInSavenamespace()
		{
			// create a configuration with the key to restore error
			// directories in fs.name.dir
			Configuration conf = GetConf();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeNameDirRestoreKey, true);
			NameNode.InitMetrics(conf, HdfsServerConstants.NamenodeRole.Namenode);
			DFSTestUtil.FormatNameNode(conf);
			FSNamesystem fsn = FSNamesystem.LoadFromDisk(conf);
			// Replace the FSImage with a spy
			FSImage originalImage = fsn.GetFSImage();
			NNStorage storage = originalImage.GetStorage();
			FSImage spyImage = Org.Mockito.Mockito.Spy(originalImage);
			Whitebox.SetInternalState(fsn, "fsImage", spyImage);
			FileSystem fs = FileSystem.GetLocal(conf);
			FilePath rootDir = storage.GetStorageDir(0).GetRoot();
			Path rootPath = new Path(rootDir.GetPath(), "current");
			FsPermission permissionNone = new FsPermission((short)0);
			FsPermission permissionAll = new FsPermission(FsAction.All, FsAction.ReadExecute, 
				FsAction.ReadExecute);
			fs.SetPermission(rootPath, permissionNone);
			try
			{
				DoAnEdit(fsn, 1);
				fsn.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				// Save namespace - should mark the first storage dir as faulty
				// since it's not traversable.
				Log.Info("Doing the first savenamespace.");
				fsn.SaveNamespace();
				Log.Info("First savenamespace sucessful.");
				NUnit.Framework.Assert.IsTrue("Savenamespace should have marked one directory as bad."
					 + " But found " + storage.GetRemovedStorageDirs().Count + " bad directories.", 
					storage.GetRemovedStorageDirs().Count == 1);
				fs.SetPermission(rootPath, permissionAll);
				// The next call to savenamespace should try inserting the
				// erroneous directory back to fs.name.dir. This command should
				// be successful.
				Log.Info("Doing the second savenamespace.");
				fsn.SaveNamespace();
				Log.Warn("Second savenamespace sucessful.");
				NUnit.Framework.Assert.IsTrue("Savenamespace should have been successful in removing "
					 + " bad directories from Image." + " But found " + storage.GetRemovedStorageDirs
					().Count + " bad directories.", storage.GetRemovedStorageDirs().Count == 0);
				// Now shut down and restart the namesystem
				Log.Info("Shutting down fsimage.");
				originalImage.Close();
				fsn.Close();
				fsn = null;
				// Start a new namesystem, which should be able to recover
				// the namespace from the previous incarnation.
				Log.Info("Loading new FSmage from disk.");
				fsn = FSNamesystem.LoadFromDisk(conf);
				// Make sure the image loaded including our edit.
				Log.Info("Checking reloaded image.");
				CheckEditExists(fsn, 1);
				Log.Info("Reloaded image is good.");
			}
			finally
			{
				if (rootDir.Exists())
				{
					fs.SetPermission(rootPath, permissionAll);
				}
				if (fsn != null)
				{
					try
					{
						fsn.Close();
					}
					catch (Exception t)
					{
						Log.Fatal("Failed to shut down", t);
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRTEWhileSavingSecondImage()
		{
			SaveNamespaceWithInjectedFault(TestSaveNamespace.Fault.SaveSecondFsimageRte);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIOEWhileSavingSecondImage()
		{
			SaveNamespaceWithInjectedFault(TestSaveNamespace.Fault.SaveSecondFsimageIoe);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCrashInAllImageDirs()
		{
			SaveNamespaceWithInjectedFault(TestSaveNamespace.Fault.SaveAllFsimages);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCrashWhenWritingVersionFiles()
		{
			SaveNamespaceWithInjectedFault(TestSaveNamespace.Fault.WriteStorageAll);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCrashWhenWritingVersionFileInOneDir()
		{
			SaveNamespaceWithInjectedFault(TestSaveNamespace.Fault.WriteStorageOne);
		}

		/// <summary>
		/// Test case where savenamespace fails in all directories
		/// and then the NN shuts down.
		/// </summary>
		/// <remarks>
		/// Test case where savenamespace fails in all directories
		/// and then the NN shuts down. Here we should recover from the
		/// failed checkpoint since it only affected ".ckpt" files, not
		/// valid image files
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestFailedSaveNamespace()
		{
			DoTestFailedSaveNamespace(false);
		}

		/// <summary>
		/// Test case where saveNamespace fails in all directories, but then
		/// the operator restores the directories and calls it again.
		/// </summary>
		/// <remarks>
		/// Test case where saveNamespace fails in all directories, but then
		/// the operator restores the directories and calls it again.
		/// This should leave the NN in a clean state for next start.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestFailedSaveNamespaceWithRecovery()
		{
			DoTestFailedSaveNamespace(true);
		}

		/// <summary>Injects a failure on all storage directories while saving namespace.</summary>
		/// <param name="restoreStorageAfterFailure">
		/// if true, will try to save again after
		/// clearing the failure injection
		/// </param>
		/// <exception cref="System.Exception"/>
		public virtual void DoTestFailedSaveNamespace(bool restoreStorageAfterFailure)
		{
			Configuration conf = GetConf();
			NameNode.InitMetrics(conf, HdfsServerConstants.NamenodeRole.Namenode);
			DFSTestUtil.FormatNameNode(conf);
			FSNamesystem fsn = FSNamesystem.LoadFromDisk(conf);
			// Replace the FSImage with a spy
			FSImage originalImage = fsn.GetFSImage();
			NNStorage storage = originalImage.GetStorage();
			storage.Close();
			// unlock any directories that FSNamesystem's initialization may have locked
			NNStorage spyStorage = Org.Mockito.Mockito.Spy(storage);
			originalImage.storage = spyStorage;
			FSImage spyImage = Org.Mockito.Mockito.Spy(originalImage);
			Whitebox.SetInternalState(fsn, "fsImage", spyImage);
			spyImage.storage.SetStorageDirectories(FSNamesystem.GetNamespaceDirs(conf), FSNamesystem
				.GetNamespaceEditsDirs(conf));
			Org.Mockito.Mockito.DoThrow(new IOException("Injected fault: saveFSImage")).When(
				spyImage).SaveFSImage((SaveNamespaceContext)Matchers.AnyObject(), (Storage.StorageDirectory
				)Matchers.AnyObject(), (NNStorage.NameNodeFile)Matchers.AnyObject());
			try
			{
				DoAnEdit(fsn, 1);
				// Save namespace
				fsn.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				try
				{
					fsn.SaveNamespace();
					NUnit.Framework.Assert.Fail("saveNamespace did not fail even when all directories failed!"
						);
				}
				catch (IOException ioe)
				{
					Log.Info("Got expected exception", ioe);
				}
				// Ensure that, if storage dirs come back online, things work again.
				if (restoreStorageAfterFailure)
				{
					Org.Mockito.Mockito.Reset(spyImage);
					spyStorage.SetRestoreFailedStorage(true);
					fsn.SaveNamespace();
					CheckEditExists(fsn, 1);
				}
				// Now shut down and restart the NN
				originalImage.Close();
				fsn.Close();
				fsn = null;
				// Start a new namesystem, which should be able to recover
				// the namespace from the previous incarnation.
				fsn = FSNamesystem.LoadFromDisk(conf);
				// Make sure the image loaded including our edits.
				CheckEditExists(fsn, 1);
			}
			finally
			{
				if (fsn != null)
				{
					fsn.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSaveWhileEditsRolled()
		{
			Configuration conf = GetConf();
			NameNode.InitMetrics(conf, HdfsServerConstants.NamenodeRole.Namenode);
			DFSTestUtil.FormatNameNode(conf);
			FSNamesystem fsn = FSNamesystem.LoadFromDisk(conf);
			try
			{
				DoAnEdit(fsn, 1);
				CheckpointSignature sig = fsn.RollEditLog();
				Log.Warn("Checkpoint signature: " + sig);
				// Do another edit
				DoAnEdit(fsn, 2);
				// Save namespace
				fsn.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				fsn.SaveNamespace();
				// Now shut down and restart the NN
				fsn.Close();
				fsn = null;
				// Start a new namesystem, which should be able to recover
				// the namespace from the previous incarnation.
				fsn = FSNamesystem.LoadFromDisk(conf);
				// Make sure the image loaded including our edits.
				CheckEditExists(fsn, 1);
				CheckEditExists(fsn, 2);
			}
			finally
			{
				if (fsn != null)
				{
					fsn.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTxIdPersistence()
		{
			Configuration conf = GetConf();
			NameNode.InitMetrics(conf, HdfsServerConstants.NamenodeRole.Namenode);
			DFSTestUtil.FormatNameNode(conf);
			FSNamesystem fsn = FSNamesystem.LoadFromDisk(conf);
			try
			{
				// We have a BEGIN_LOG_SEGMENT txn to start
				NUnit.Framework.Assert.AreEqual(1, fsn.GetEditLog().GetLastWrittenTxId());
				DoAnEdit(fsn, 1);
				NUnit.Framework.Assert.AreEqual(2, fsn.GetEditLog().GetLastWrittenTxId());
				fsn.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				fsn.SaveNamespace();
				// 2 more txns: END the first segment, BEGIN a new one
				NUnit.Framework.Assert.AreEqual(4, fsn.GetEditLog().GetLastWrittenTxId());
				// Shut down and restart
				fsn.GetFSImage().Close();
				fsn.Close();
				// 1 more txn to END that segment
				NUnit.Framework.Assert.AreEqual(5, fsn.GetEditLog().GetLastWrittenTxId());
				fsn = null;
				fsn = FSNamesystem.LoadFromDisk(conf);
				// 1 more txn to start new segment on restart
				NUnit.Framework.Assert.AreEqual(6, fsn.GetEditLog().GetLastWrittenTxId());
			}
			finally
			{
				if (fsn != null)
				{
					fsn.Close();
				}
			}
		}

		/// <summary>
		/// Test for save namespace should succeed when parent directory renamed with
		/// open lease and destination directory exist.
		/// </summary>
		/// <remarks>
		/// Test for save namespace should succeed when parent directory renamed with
		/// open lease and destination directory exist.
		/// This test is a regression for HDFS-2827
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSaveNamespaceWithRenamedLease()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration()).NumDataNodes
				(1).Build();
			cluster.WaitActive();
			DistributedFileSystem fs = (DistributedFileSystem)cluster.GetFileSystem();
			OutputStream @out = null;
			try
			{
				fs.Mkdirs(new Path("/test-target"));
				@out = fs.Create(new Path("/test-source/foo"));
				// don't close
				fs.Rename(new Path("/test-source/"), new Path("/test-target/"));
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				cluster.GetNameNodeRpc().SaveNamespace();
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			}
			finally
			{
				IOUtils.Cleanup(Log, @out, fs);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCancelSaveNamespace()
		{
			Configuration conf = GetConf();
			NameNode.InitMetrics(conf, HdfsServerConstants.NamenodeRole.Namenode);
			DFSTestUtil.FormatNameNode(conf);
			FSNamesystem fsn = FSNamesystem.LoadFromDisk(conf);
			// Replace the FSImage with a spy
			FSImage image = fsn.GetFSImage();
			NNStorage storage = image.GetStorage();
			storage.Close();
			// unlock any directories that FSNamesystem's initialization may have locked
			storage.SetStorageDirectories(FSNamesystem.GetNamespaceDirs(conf), FSNamesystem.GetNamespaceEditsDirs
				(conf));
			FSNamesystem spyFsn = Org.Mockito.Mockito.Spy(fsn);
			FSNamesystem finalFsn = spyFsn;
			GenericTestUtils.DelayAnswer delayer = new GenericTestUtils.DelayAnswer(Log);
			BlockIdManager bid = Org.Mockito.Mockito.Spy(spyFsn.GetBlockIdManager());
			Whitebox.SetInternalState(finalFsn, "blockIdManager", bid);
			Org.Mockito.Mockito.DoAnswer(delayer).When(bid).GetGenerationStampV2();
			ExecutorService pool = Executors.NewFixedThreadPool(2);
			try
			{
				DoAnEdit(fsn, 1);
				Canceler canceler = new Canceler();
				// Save namespace
				fsn.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				try
				{
					Future<Void> saverFuture = pool.Submit(new _Callable_561(image, finalFsn, canceler
						));
					// Wait until saveNamespace calls getGenerationStamp
					delayer.WaitForCall();
					// then cancel the saveNamespace
					Future<Void> cancelFuture = pool.Submit(new _Callable_572(canceler));
					// give the cancel call time to run
					Sharpen.Thread.Sleep(500);
					// allow saveNamespace to proceed - it should check the cancel flag after
					// this point and throw an exception
					delayer.Proceed();
					cancelFuture.Get();
					saverFuture.Get();
					NUnit.Framework.Assert.Fail("saveNamespace did not fail even though cancelled!");
				}
				catch (Exception t)
				{
					GenericTestUtils.AssertExceptionContains("SaveNamespaceCancelledException", t);
				}
				Log.Info("Successfully cancelled a saveNamespace");
				// Check that we have only the original image and not any
				// cruft left over from half-finished images
				FSImageTestUtil.LogStorageContents(Log, storage);
				foreach (Storage.StorageDirectory sd in storage.DirIterable(null))
				{
					FilePath curDir = sd.GetCurrentDir();
					GenericTestUtils.AssertGlobEquals(curDir, "fsimage_.*", NNStorage.GetImageFileName
						(0), NNStorage.GetImageFileName(0) + MD5FileUtils.Md5Suffix);
				}
			}
			finally
			{
				fsn.Close();
			}
		}

		private sealed class _Callable_561 : Callable<Void>
		{
			public _Callable_561(FSImage image, FSNamesystem finalFsn, Canceler canceler)
			{
				this.image = image;
				this.finalFsn = finalFsn;
				this.canceler = canceler;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				image.SaveNamespace(finalFsn, NNStorage.NameNodeFile.Image, canceler);
				return null;
			}

			private readonly FSImage image;

			private readonly FSNamesystem finalFsn;

			private readonly Canceler canceler;
		}

		private sealed class _Callable_572 : Callable<Void>
		{
			public _Callable_572(Canceler canceler)
			{
				this.canceler = canceler;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				canceler.Cancel("cancelled");
				return null;
			}

			private readonly Canceler canceler;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSaveNamespaceWithDanglingLease()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration()).NumDataNodes
				(1).Build();
			cluster.WaitActive();
			DistributedFileSystem fs = cluster.GetFileSystem();
			try
			{
				cluster.GetNamesystem().leaseManager.AddLease("me", "/non-existent");
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				cluster.GetNameNodeRpc().SaveNamespace();
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
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
		private void DoAnEdit(FSNamesystem fsn, int id)
		{
			// Make an edit
			fsn.Mkdirs("/test" + id, new PermissionStatus("test", "Test", new FsPermission((short
				)0x1ff)), true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckEditExists(FSNamesystem fsn, int id)
		{
			// Make sure the image loaded including our edit.
			NUnit.Framework.Assert.IsNotNull(fsn.GetFileInfo("/test" + id, false));
		}

		/// <exception cref="System.IO.IOException"/>
		private Configuration GetConf()
		{
			string baseDir = MiniDFSCluster.GetBaseDirectory();
			string nameDirs = Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI(new FilePath
				(baseDir, "name1")) + "," + Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI(
				new FilePath(baseDir, "name2"));
			Configuration conf = new HdfsConfiguration();
			FileSystem.SetDefaultUri(conf, "hdfs://localhost:0");
			conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "0.0.0.0:0");
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDirs);
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameDirs);
			conf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, "0.0.0.0:0");
			conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, false);
			return conf;
		}
	}
}
