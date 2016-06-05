using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.IO;
using Com.Google.Common.Primitives;
using NUnit.Framework;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This class tests the creation and validation of a checkpoint.</summary>
	public class TestCheckpoint
	{
		static TestCheckpoint()
		{
			((Log4JLogger)FSImage.Log).GetLogger().SetLevel(Level.All);
		}

		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.TestCheckpoint
			));

		internal const string NnMetrics = "NameNodeActivity";

		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 4096;

		internal const int fileSize = 8192;

		internal const int numDatanodes = 3;

		internal short replication = 3;

		private sealed class _FilenameFilter_125 : FilenameFilter
		{
			public _FilenameFilter_125()
			{
			}

			public bool Accept(FilePath dir, string name)
			{
				return name.StartsWith(NNStorage.NameNodeFile.EditsTmp.GetName());
			}
		}

		internal static readonly FilenameFilter tmpEditsFilter = new _FilenameFilter_125(
			);

		private CheckpointFaultInjector faultInjector;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			FileUtil.FullyDeleteContents(new FilePath(MiniDFSCluster.GetBaseDirectory()));
			faultInjector = Org.Mockito.Mockito.Mock<CheckpointFaultInjector>();
			CheckpointFaultInjector.instance = faultInjector;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void WriteFile(FileSystem fileSys, Path name, int repl)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)repl, blockSize);
			byte[] buffer = new byte[Org.Apache.Hadoop.Hdfs.Server.Namenode.TestCheckpoint.fileSize
				];
			Random rand = new Random(Org.Apache.Hadoop.Hdfs.Server.Namenode.TestCheckpoint.seed
				);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
		}

		[TearDown]
		public virtual void CheckForSNNThreads()
		{
			GenericTestUtils.AssertNoThreadsMatching(".*SecondaryNameNode.*");
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void CheckFile(FileSystem fileSys, Path name, int repl)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			int replication = fileSys.GetFileStatus(name).GetReplication();
			NUnit.Framework.Assert.AreEqual("replication for " + name, repl, replication);
		}

		//We should probably test for more of the file properties.    
		/// <exception cref="System.IO.IOException"/>
		internal static void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		/*
		* Verify that namenode does not startup if one namedir is bad.
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNameDirError()
		{
			Log.Info("Starting testNameDirError");
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
			ICollection<URI> nameDirs = cluster.GetNameDirs(0);
			cluster.Shutdown();
			cluster = null;
			foreach (URI nameDirUri in nameDirs)
			{
				FilePath dir = new FilePath(nameDirUri.GetPath());
				try
				{
					// Simulate the mount going read-only
					FileUtil.SetWritable(dir, false);
					cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).Build();
					NUnit.Framework.Assert.Fail("NN should have failed to start with " + dir + " set unreadable"
						);
				}
				catch (IOException ioe)
				{
					GenericTestUtils.AssertExceptionContains("storage directory does not exist or is not accessible"
						, ioe);
				}
				finally
				{
					Cleanup(cluster);
					cluster = null;
					FileUtil.SetWritable(dir, true);
				}
			}
		}

		/// <summary>
		/// Checks that an IOException in NNStorage.writeTransactionIdFile is handled
		/// correctly (by removing the storage directory)
		/// See https://issues.apache.org/jira/browse/HDFS-2011
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteTransactionIdHandlesIOE()
		{
			Log.Info("Check IOException handled correctly by writeTransactionIdFile");
			AList<URI> fsImageDirs = new AList<URI>();
			AList<URI> editsDirs = new AList<URI>();
			FilePath filePath = new FilePath(PathUtils.GetTestDir(GetType()), "storageDirToCheck"
				);
			NUnit.Framework.Assert.IsTrue("Couldn't create directory storageDirToCheck", filePath
				.Exists() || filePath.Mkdirs());
			fsImageDirs.AddItem(filePath.ToURI());
			editsDirs.AddItem(filePath.ToURI());
			NNStorage nnStorage = new NNStorage(new HdfsConfiguration(), fsImageDirs, editsDirs
				);
			try
			{
				NUnit.Framework.Assert.IsTrue("List of storage directories didn't have storageDirToCheck."
					, nnStorage.GetEditsDirectories().GetEnumerator().Next().ToString().IndexOf("storageDirToCheck"
					) != -1);
				NUnit.Framework.Assert.IsTrue("List of removed storage directories wasn't empty", 
					nnStorage.GetRemovedStorageDirs().IsEmpty());
			}
			finally
			{
				// Delete storage directory to cause IOException in writeTransactionIdFile 
				NUnit.Framework.Assert.IsTrue("Couldn't remove directory " + filePath.GetAbsolutePath
					(), filePath.Delete());
			}
			// Just call writeTransactionIdFile using any random number
			nnStorage.WriteTransactionIdFileToStorage(1);
			IList<Storage.StorageDirectory> listRsd = nnStorage.GetRemovedStorageDirs();
			NUnit.Framework.Assert.IsTrue("Removed directory wasn't what was expected", listRsd
				.Count > 0 && listRsd[listRsd.Count - 1].GetRoot().ToString().IndexOf("storageDirToCheck"
				) != -1);
			nnStorage.Close();
		}

		/*
		* Simulate exception during edit replay.
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReloadOnEditReplayFailure()
		{
			Configuration conf = new HdfsConfiguration();
			FSDataOutputStream fos = null;
			SecondaryNameNode secondary = null;
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				secondary = StartSecondaryNameNode(conf);
				fos = fs.Create(new Path("tmpfile0"));
				fos.Write(new byte[] { 0, 1, 2, 3 });
				secondary.DoCheckpoint();
				fos.Write(new byte[] { 0, 1, 2, 3 });
				fos.Hsync();
				// Cause merge to fail in next checkpoint.
				Org.Mockito.Mockito.DoThrow(new IOException("Injecting failure during merge")).When
					(faultInjector).DuringMerge();
				try
				{
					secondary.DoCheckpoint();
					NUnit.Framework.Assert.Fail("Fault injection failed.");
				}
				catch (IOException)
				{
				}
				// This is expected.
				Org.Mockito.Mockito.Reset(faultInjector);
				// The error must be recorded, so next checkpoint will reload image.
				fos.Write(new byte[] { 0, 1, 2, 3 });
				fos.Hsync();
				NUnit.Framework.Assert.IsTrue("Another checkpoint should have reloaded image", secondary
					.DoCheckpoint());
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
				Org.Mockito.Mockito.Reset(faultInjector);
			}
		}

		/*
		* Simulate 2NN exit due to too many merge failures.
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTooManyEditReplayFailures()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckpointMaxRetriesKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckpointPeriodKey, 1);
			FSDataOutputStream fos = null;
			SecondaryNameNode secondary = null;
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).CheckExitOnShutdown
					(false).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				fos = fs.Create(new Path("tmpfile0"));
				fos.Write(new byte[] { 0, 1, 2, 3 });
				// Cause merge to fail in next checkpoint.
				Org.Mockito.Mockito.DoThrow(new IOException("Injecting failure during merge")).When
					(faultInjector).DuringMerge();
				secondary = StartSecondaryNameNode(conf);
				secondary.DoWork();
				// Fail if we get here.
				NUnit.Framework.Assert.Fail("2NN did not exit.");
			}
			catch (ExitUtil.ExitException)
			{
				// ignore
				ExitUtil.ResetFirstExitException();
				NUnit.Framework.Assert.AreEqual("Max retries", 1, secondary.GetMergeErrorCount() 
					- 1);
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
				Org.Mockito.Mockito.Reset(faultInjector);
			}
		}

		/*
		* Simulate namenode crashing after rolling edit log.
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSecondaryNamenodeError1()
		{
			Log.Info("Starting testSecondaryNamenodeError1");
			Configuration conf = new HdfsConfiguration();
			Path file1 = new Path("checkpointxx.dat");
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			SecondaryNameNode secondary = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file1));
				// Make the checkpoint fail after rolling the edits log.
				secondary = StartSecondaryNameNode(conf);
				Org.Mockito.Mockito.DoThrow(new IOException("Injecting failure after rolling edit logs"
					)).When(faultInjector).AfterSecondaryCallsRollEditLog();
				try
				{
					secondary.DoCheckpoint();
					// this should fail
					NUnit.Framework.Assert.IsTrue(false);
				}
				catch (IOException)
				{
				}
				// expected
				Org.Mockito.Mockito.Reset(faultInjector);
				//
				// Create a new file
				//
				WriteFile(fileSys, file1, replication);
				CheckFile(fileSys, file1, replication);
			}
			finally
			{
				fileSys.Close();
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
			//
			// Restart cluster and verify that file exists.
			// Then take another checkpoint to verify that the 
			// namenode restart accounted for the rolled edit logs.
			//
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(false
					).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				CheckFile(fileSys, file1, replication);
				CleanupFile(fileSys, file1);
				secondary = StartSecondaryNameNode(conf);
				secondary.DoCheckpoint();
				secondary.Shutdown();
			}
			finally
			{
				fileSys.Close();
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/*
		* Simulate a namenode crash after uploading new image
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSecondaryNamenodeError2()
		{
			Log.Info("Starting testSecondaryNamenodeError2");
			Configuration conf = new HdfsConfiguration();
			Path file1 = new Path("checkpointyy.dat");
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			SecondaryNameNode secondary = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file1));
				//
				// Make the checkpoint fail after uploading the new fsimage.
				//
				secondary = StartSecondaryNameNode(conf);
				Org.Mockito.Mockito.DoThrow(new IOException("Injecting failure after uploading new image"
					)).When(faultInjector).AfterSecondaryUploadsNewImage();
				try
				{
					secondary.DoCheckpoint();
					// this should fail
					NUnit.Framework.Assert.IsTrue(false);
				}
				catch (IOException)
				{
				}
				// expected
				Org.Mockito.Mockito.Reset(faultInjector);
				//
				// Create a new file
				//
				WriteFile(fileSys, file1, replication);
				CheckFile(fileSys, file1, replication);
			}
			finally
			{
				fileSys.Close();
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
			//
			// Restart cluster and verify that file exists.
			// Then take another checkpoint to verify that the 
			// namenode restart accounted for the rolled edit logs.
			//
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(false
					).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				CheckFile(fileSys, file1, replication);
				CleanupFile(fileSys, file1);
				secondary = StartSecondaryNameNode(conf);
				secondary.DoCheckpoint();
				secondary.Shutdown();
			}
			finally
			{
				fileSys.Close();
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/*
		* Simulate a secondary namenode crash after rolling the edit log.
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSecondaryNamenodeError3()
		{
			Log.Info("Starting testSecondaryNamenodeError3");
			Configuration conf = new HdfsConfiguration();
			Path file1 = new Path("checkpointzz.dat");
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			SecondaryNameNode secondary = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file1));
				//
				// Make the checkpoint fail after rolling the edit log.
				//
				secondary = StartSecondaryNameNode(conf);
				Org.Mockito.Mockito.DoThrow(new IOException("Injecting failure after rolling edit logs"
					)).When(faultInjector).AfterSecondaryCallsRollEditLog();
				try
				{
					secondary.DoCheckpoint();
					// this should fail
					NUnit.Framework.Assert.IsTrue(false);
				}
				catch (IOException)
				{
				}
				// expected
				Org.Mockito.Mockito.Reset(faultInjector);
				secondary.Shutdown();
				// secondary namenode crash!
				// start new instance of secondary and verify that 
				// a new rollEditLog suceedes inspite of the fact that 
				// edits.new already exists.
				//
				secondary = StartSecondaryNameNode(conf);
				secondary.DoCheckpoint();
				// this should work correctly
				//
				// Create a new file
				//
				WriteFile(fileSys, file1, replication);
				CheckFile(fileSys, file1, replication);
			}
			finally
			{
				fileSys.Close();
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
			//
			// Restart cluster and verify that file exists.
			// Then take another checkpoint to verify that the 
			// namenode restart accounted for the twice-rolled edit logs.
			//
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(false
					).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				CheckFile(fileSys, file1, replication);
				CleanupFile(fileSys, file1);
				secondary = StartSecondaryNameNode(conf);
				secondary.DoCheckpoint();
				secondary.Shutdown();
			}
			finally
			{
				fileSys.Close();
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>Simulate a secondary node failure to transfer image.</summary>
		/// <remarks>
		/// Simulate a secondary node failure to transfer image. Uses an unchecked
		/// error and fail transfer before even setting the length header. This used to
		/// cause image truncation. Regression test for HDFS-3330.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSecondaryFailsWithErrorBeforeSettingHeaders()
		{
			Org.Mockito.Mockito.DoThrow(new Error("If this exception is not caught by the " +
				 "name-node, fs image will be truncated.")).When(faultInjector).BeforeGetImageSetsHeaders
				();
			DoSecondaryFailsToReturnImage();
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoSecondaryFailsToReturnImage()
		{
			Log.Info("Starting testSecondaryFailsToReturnImage");
			Configuration conf = new HdfsConfiguration();
			Path file1 = new Path("checkpointRI.dat");
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			FSImage image = null;
			SecondaryNameNode secondary = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				image = cluster.GetNameNode().GetFSImage();
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file1));
				Storage.StorageDirectory sd = image.GetStorage().GetStorageDir(0);
				FilePath latestImageBeforeCheckpoint = FSImageTestUtil.FindLatestImageFile(sd);
				long fsimageLength = latestImageBeforeCheckpoint.Length();
				//
				// Make the checkpoint
				//
				secondary = StartSecondaryNameNode(conf);
				try
				{
					secondary.DoCheckpoint();
					// this should fail
					NUnit.Framework.Assert.Fail("Checkpoint succeeded even though we injected an error!"
						);
				}
				catch (IOException e)
				{
					// check that it's the injected exception
					GenericTestUtils.AssertExceptionContains("If this exception is not caught", e);
				}
				Org.Mockito.Mockito.Reset(faultInjector);
				// Verify that image file sizes did not change.
				foreach (Storage.StorageDirectory sd2 in image.GetStorage().DirIterable(NNStorage.NameNodeDirType
					.Image))
				{
					FilePath thisNewestImage = FSImageTestUtil.FindLatestImageFile(sd2);
					long len = thisNewestImage.Length();
					NUnit.Framework.Assert.AreEqual(fsimageLength, len);
				}
			}
			finally
			{
				fileSys.Close();
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		private FilePath FilePathContaining(string substring)
		{
			return Org.Mockito.Mockito.ArgThat(new _ArgumentMatcher_637(substring));
		}

		private sealed class _ArgumentMatcher_637 : ArgumentMatcher<FilePath>
		{
			public _ArgumentMatcher_637(string substring)
			{
				this.substring = substring;
			}

			public override bool Matches(object argument)
			{
				string path = ((FilePath)argument).GetAbsolutePath();
				return path.Contains(substring);
			}

			private readonly string substring;
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckTempImages(NNStorage storage)
		{
			IList<FilePath> dirs = new AList<FilePath>();
			dirs.AddItem(storage.GetStorageDir(0).GetCurrentDir());
			dirs.AddItem(storage.GetStorageDir(1).GetCurrentDir());
			foreach (FilePath dir in dirs)
			{
				FilePath[] list = dir.ListFiles();
				foreach (FilePath f in list)
				{
					// Throw an exception if a temp image file is found.
					if (f.GetName().Contains(NNStorage.NameNodeFile.ImageNew.GetName()))
					{
						throw new IOException("Found " + f);
					}
				}
			}
		}

		/// <summary>
		/// Simulate 2NN failing to send the whole file (error type 3)
		/// The length header in the HTTP transfer should prevent
		/// this from corrupting the NN.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNameNodeImageSendFailWrongSize()
		{
			Log.Info("Starting testNameNodeImageSendFailWrongSize");
			Org.Mockito.Mockito.DoReturn(true).When(faultInjector).ShouldSendShortFile(FilePathContaining
				("fsimage"));
			DoSendFailTest("is not of the advertised size");
		}

		/// <summary>
		/// Simulate 2NN sending a corrupt image (error type 4)
		/// The digest header in the HTTP transfer should prevent
		/// this from corrupting the NN.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNameNodeImageSendFailWrongDigest()
		{
			Log.Info("Starting testNameNodeImageSendFailWrongDigest");
			Org.Mockito.Mockito.DoReturn(true).When(faultInjector).ShouldCorruptAByte(Org.Mockito.Mockito
				.Any<FilePath>());
			DoSendFailTest("does not match advertised digest");
		}

		/// <summary>
		/// Run a test where the 2NN runs into some kind of error when
		/// sending the checkpoint back to the NN.
		/// </summary>
		/// <param name="exceptionSubstring">an expected substring of the triggered exception
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		private void DoSendFailTest(string exceptionSubstring)
		{
			Configuration conf = new HdfsConfiguration();
			Path file1 = new Path("checkpoint-doSendFailTest-doSendFailTest.dat");
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			SecondaryNameNode secondary = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file1));
				//
				// Make the checkpoint fail after rolling the edit log.
				//
				secondary = StartSecondaryNameNode(conf);
				try
				{
					secondary.DoCheckpoint();
					// this should fail
					NUnit.Framework.Assert.Fail("Did not get expected exception");
				}
				catch (IOException e)
				{
					// We only sent part of the image. Have to trigger this exception
					GenericTestUtils.AssertExceptionContains(exceptionSubstring, e);
				}
				Org.Mockito.Mockito.Reset(faultInjector);
				// Make sure there is no temporary files left around.
				CheckTempImages(cluster.GetNameNode().GetFSImage().GetStorage());
				CheckTempImages(secondary.GetFSImage().GetStorage());
				secondary.Shutdown();
				// secondary namenode crash!
				secondary = null;
				// start new instance of secondary and verify that 
				// a new rollEditLog succedes in spite of the fact that we had
				// a partially failed checkpoint previously.
				//
				secondary = StartSecondaryNameNode(conf);
				secondary.DoCheckpoint();
				// this should work correctly
				//
				// Create a new file
				//
				WriteFile(fileSys, file1, replication);
				CheckFile(fileSys, file1, replication);
			}
			finally
			{
				fileSys.Close();
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>
		/// Test that the NN locks its storage and edits directories, and won't start up
		/// if the directories are already locked
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNameDirLocking()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			// Start a NN, and verify that lock() fails in all of the configured
			// directories
			Storage.StorageDirectory savedSd = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				NNStorage storage = cluster.GetNameNode().GetFSImage().GetStorage();
				foreach (Storage.StorageDirectory sd in storage.DirIterable(null))
				{
					AssertLockFails(sd);
					savedSd = sd;
				}
			}
			finally
			{
				Cleanup(cluster);
				cluster = null;
			}
			NUnit.Framework.Assert.IsNotNull(savedSd);
			// Lock one of the saved directories, then start the NN, and make sure it
			// fails to start
			AssertClusterStartFailsWhenDirLocked(conf, savedSd);
		}

		/// <summary>
		/// Test that, if the edits dir is separate from the name dir, it is
		/// properly locked.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSeparateEditsDirLocking()
		{
			Configuration conf = new HdfsConfiguration();
			FilePath nameDir = new FilePath(MiniDFSCluster.GetBaseDirectory(), "name");
			FilePath editsDir = new FilePath(MiniDFSCluster.GetBaseDirectory(), "testSeparateEditsDirLocking"
				);
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDir.GetAbsolutePath());
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, editsDir.GetAbsolutePath());
			MiniDFSCluster cluster = null;
			// Start a NN, and verify that lock() fails in all of the configured
			// directories
			Storage.StorageDirectory savedSd = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).ManageNameDfsDirs(false).NumDataNodes(
					0).Build();
				NNStorage storage = cluster.GetNameNode().GetFSImage().GetStorage();
				foreach (Storage.StorageDirectory sd in storage.DirIterable(NNStorage.NameNodeDirType
					.Edits))
				{
					NUnit.Framework.Assert.AreEqual(editsDir.GetAbsoluteFile(), sd.GetRoot());
					AssertLockFails(sd);
					savedSd = sd;
				}
			}
			finally
			{
				Cleanup(cluster);
				cluster = null;
			}
			NUnit.Framework.Assert.IsNotNull(savedSd);
			// Lock one of the saved directories, then start the NN, and make sure it
			// fails to start
			AssertClusterStartFailsWhenDirLocked(conf, savedSd);
		}

		/// <summary>Test that the SecondaryNameNode properly locks its storage directories.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSecondaryNameNodeLocking()
		{
			// Start a primary NN so that the secondary will start successfully
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				Storage.StorageDirectory savedSd = null;
				// Start a secondary NN, then make sure that all of its storage
				// dirs got locked.
				secondary = StartSecondaryNameNode(conf);
				NNStorage storage = secondary.GetFSImage().GetStorage();
				foreach (Storage.StorageDirectory sd in storage.DirIterable(null))
				{
					AssertLockFails(sd);
					savedSd = sd;
				}
				Log.Info("===> Shutting down first 2NN");
				secondary.Shutdown();
				secondary = null;
				Log.Info("===> Locking a dir, starting second 2NN");
				// Lock one of its dirs, make sure it fails to start
				Log.Info("Trying to lock" + savedSd);
				savedSd.Lock();
				try
				{
					secondary = StartSecondaryNameNode(conf);
					NUnit.Framework.Assert.IsFalse("Should fail to start 2NN when " + savedSd + " is locked"
						, savedSd.IsLockSupported());
				}
				catch (IOException ioe)
				{
					GenericTestUtils.AssertExceptionContains("already locked", ioe);
				}
				finally
				{
					savedSd.Unlock();
				}
			}
			finally
			{
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>
		/// Test that, an attempt to lock a storage that is already locked by nodename,
		/// logs error message that includes JVM name of the namenode that locked it.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStorageAlreadyLockedErrorMessage()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			Storage.StorageDirectory savedSd = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				NNStorage storage = cluster.GetNameNode().GetFSImage().GetStorage();
				foreach (Storage.StorageDirectory sd in storage.DirIterable(null))
				{
					AssertLockFails(sd);
					savedSd = sd;
				}
				GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer.CaptureLogs(LogFactory
					.GetLog(typeof(Storage)));
				try
				{
					// try to lock the storage that's already locked
					savedSd.Lock();
					NUnit.Framework.Assert.Fail("Namenode should not be able to lock a storage" + " that is already locked"
						);
				}
				catch (IOException)
				{
					// cannot read lock file on Windows, so message cannot get JVM name
					string lockingJvmName = Path.Windows ? string.Empty : " " + ManagementFactory.GetRuntimeMXBean
						().GetName();
					string expectedLogMessage = "It appears that another node " + lockingJvmName + " has already locked the storage directory";
					NUnit.Framework.Assert.IsTrue("Log output does not contain expected log message: "
						 + expectedLogMessage, logs.GetOutput().Contains(expectedLogMessage));
				}
			}
			finally
			{
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>
		/// Assert that the given storage directory can't be locked, because
		/// it's already locked.
		/// </summary>
		private static void AssertLockFails(Storage.StorageDirectory sd)
		{
			try
			{
				sd.Lock();
				// If the above line didn't throw an exception, then
				// locking must not be supported
				NUnit.Framework.Assert.IsFalse(sd.IsLockSupported());
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("already locked", ioe);
			}
		}

		/// <summary>Assert that, if sdToLock is locked, the cluster is not allowed to start up.
		/// 	</summary>
		/// <param name="conf">cluster conf to use</param>
		/// <param name="sdToLock">the storage directory to lock</param>
		/// <exception cref="System.IO.IOException"/>
		private static void AssertClusterStartFailsWhenDirLocked(Configuration conf, Storage.StorageDirectory
			 sdToLock)
		{
			// Lock the edits dir, then start the NN, and make sure it fails to start
			sdToLock.Lock();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Format(false).ManageNameDfsDirs(false)
					.NumDataNodes(0).Build();
				NUnit.Framework.Assert.IsFalse("cluster should fail to start after locking " + sdToLock
					, sdToLock.IsLockSupported());
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("already locked", ioe);
			}
			finally
			{
				Cleanup(cluster);
				cluster = null;
				sdToLock.Unlock();
			}
		}

		/// <summary>Test the importCheckpoint startup option.</summary>
		/// <remarks>
		/// Test the importCheckpoint startup option. Verifies:
		/// 1. if the NN already contains an image, it will not be allowed
		/// to import a checkpoint.
		/// 2. if the NN does not contain an image, importing a checkpoint
		/// succeeds and re-saves the image
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestImportCheckpoint()
		{
			Configuration conf = new HdfsConfiguration();
			Path testPath = new Path("/testfile");
			SecondaryNameNode snn = null;
			MiniDFSCluster cluster = null;
			ICollection<URI> nameDirs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				nameDirs = cluster.GetNameDirs(0);
				// Make an entry in the namespace, used for verifying checkpoint
				// later.
				cluster.GetFileSystem().Mkdirs(testPath);
				// Take a checkpoint
				snn = StartSecondaryNameNode(conf);
				snn.DoCheckpoint();
			}
			finally
			{
				Cleanup(snn);
				Cleanup(cluster);
				cluster = null;
			}
			Log.Info("Trying to import checkpoint when the NameNode already " + "contains an image. This should fail."
				);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).StartupOption
					(HdfsServerConstants.StartupOption.Import).Build();
				NUnit.Framework.Assert.Fail("NameNode did not fail to start when it already contained "
					 + "an image");
			}
			catch (IOException ioe)
			{
				// Expected
				GenericTestUtils.AssertExceptionContains("NameNode already contains an image", ioe
					);
			}
			finally
			{
				Cleanup(cluster);
				cluster = null;
			}
			Log.Info("Removing NN storage contents");
			foreach (URI uri in nameDirs)
			{
				FilePath dir = new FilePath(uri.GetPath());
				Log.Info("Cleaning " + dir);
				RemoveAndRecreateDir(dir);
			}
			Log.Info("Trying to import checkpoint");
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Format(false).NumDataNodes(0).StartupOption
					(HdfsServerConstants.StartupOption.Import).Build();
				NUnit.Framework.Assert.IsTrue("Path from checkpoint should exist after import", cluster
					.GetFileSystem().Exists(testPath));
				// Make sure that the image got saved on import
				FSImageTestUtil.AssertNNHasCheckpoints(cluster, Ints.AsList(3));
			}
			finally
			{
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void RemoveAndRecreateDir(FilePath dir)
		{
			if (dir.Exists())
			{
				if (!(FileUtil.FullyDelete(dir)))
				{
					throw new IOException("Cannot remove directory: " + dir);
				}
			}
			if (!dir.Mkdirs())
			{
				throw new IOException("Cannot create directory " + dir);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual SecondaryNameNode StartSecondaryNameNode(Configuration conf)
		{
			conf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, "0.0.0.0:0");
			return new SecondaryNameNode(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual SecondaryNameNode StartSecondaryNameNode(Configuration conf, int
			 index)
		{
			Configuration snnConf = new Configuration(conf);
			snnConf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, "0.0.0.0:0");
			snnConf.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, MiniDFSCluster.GetBaseDirectory
				() + "/2nn-" + index);
			return new SecondaryNameNode(snnConf);
		}

		/// <summary>Tests checkpoint in HDFS.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckpoint()
		{
			Path file1 = new Path("checkpoint.dat");
			Path file2 = new Path("checkpoint2.dat");
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, "0.0.0.0:0");
			replication = (short)conf.GetInt(DFSConfigKeys.DfsReplicationKey, 3);
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			SecondaryNameNode secondary = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				//
				// verify that 'format' really blew away all pre-existing files
				//
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file1));
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file2));
				//
				// Create file1
				//
				WriteFile(fileSys, file1, replication);
				CheckFile(fileSys, file1, replication);
				//
				// Take a checkpoint
				//
				secondary = StartSecondaryNameNode(conf);
				secondary.DoCheckpoint();
				MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(NnMetrics);
				MetricsAsserts.AssertCounterGt("GetImageNumOps", 0, rb);
				MetricsAsserts.AssertCounterGt("GetEditNumOps", 0, rb);
				MetricsAsserts.AssertCounterGt("PutImageNumOps", 0, rb);
				MetricsAsserts.AssertGaugeGt("GetImageAvgTime", 0.0, rb);
				MetricsAsserts.AssertGaugeGt("GetEditAvgTime", 0.0, rb);
				MetricsAsserts.AssertGaugeGt("PutImageAvgTime", 0.0, rb);
			}
			finally
			{
				fileSys.Close();
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
			//
			// Restart cluster and verify that file1 still exist.
			//
			Path tmpDir = new Path("/tmp_tmp");
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(false
					).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				// check that file1 still exists
				CheckFile(fileSys, file1, replication);
				CleanupFile(fileSys, file1);
				// create new file file2
				WriteFile(fileSys, file2, replication);
				CheckFile(fileSys, file2, replication);
				//
				// Take a checkpoint
				//
				secondary = StartSecondaryNameNode(conf);
				secondary.DoCheckpoint();
				fileSys.Delete(tmpDir, true);
				fileSys.Mkdirs(tmpDir);
				secondary.DoCheckpoint();
			}
			finally
			{
				fileSys.Close();
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
			//
			// Restart cluster and verify that file2 exists and
			// file1 does not exist.
			//
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(false
				).Build();
			cluster.WaitActive();
			fileSys = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file1));
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(tmpDir));
			try
			{
				// verify that file2 exists
				CheckFile(fileSys, file2, replication);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
				cluster = null;
			}
		}

		/// <summary>Tests save namespace.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSaveNamespace()
		{
			MiniDFSCluster cluster = null;
			DistributedFileSystem fs = null;
			FileContext fc;
			try
			{
				Configuration conf = new HdfsConfiguration();
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(true
					).Build();
				cluster.WaitActive();
				fs = (cluster.GetFileSystem());
				fc = FileContext.GetFileContext(cluster.GetURI(0));
				// Saving image without safe mode should fail
				DFSAdmin admin = new DFSAdmin(conf);
				string[] args = new string[] { "-saveNamespace" };
				try
				{
					admin.Run(args);
				}
				catch (IOException eIO)
				{
					NUnit.Framework.Assert.IsTrue(eIO.GetLocalizedMessage().Contains("Safe mode should be turned ON"
						));
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
				// create new file
				Path file = new Path("namespace.dat");
				WriteFile(fs, file, replication);
				CheckFile(fs, file, replication);
				// create new link
				Path symlink = new Path("file.link");
				fc.CreateSymlink(file, symlink, false);
				NUnit.Framework.Assert.IsTrue(fc.GetFileLinkStatus(symlink).IsSymlink());
				// verify that the edits file is NOT empty
				ICollection<URI> editsDirs = cluster.GetNameEditsDirs(0);
				foreach (URI uri in editsDirs)
				{
					FilePath ed = new FilePath(uri.GetPath());
					NUnit.Framework.Assert.IsTrue(new FilePath(ed, "current/" + NNStorage.GetInProgressEditsFileName
						(1)).Length() > int.Size / byte.Size);
				}
				// Saving image in safe mode should succeed
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				try
				{
					admin.Run(args);
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
				// TODO: Fix the test to not require a hard-coded transaction count.
				int ExpectedTxnsFirstSeg = 13;
				// the following steps should have happened:
				//   edits_inprogress_1 -> edits_1-12  (finalized)
				//   fsimage_12 created
				//   edits_inprogress_13 created
				//
				foreach (URI uri_1 in editsDirs)
				{
					FilePath ed = new FilePath(uri_1.GetPath());
					FilePath curDir = new FilePath(ed, "current");
					Log.Info("Files in " + curDir + ":\n  " + Joiner.On("\n  ").Join(curDir.List()));
					// Verify that the first edits file got finalized
					FilePath originalEdits = new FilePath(curDir, NNStorage.GetInProgressEditsFileName
						(1));
					NUnit.Framework.Assert.IsFalse(originalEdits.Exists());
					FilePath finalizedEdits = new FilePath(curDir, NNStorage.GetFinalizedEditsFileName
						(1, ExpectedTxnsFirstSeg));
					GenericTestUtils.AssertExists(finalizedEdits);
					NUnit.Framework.Assert.IsTrue(finalizedEdits.Length() > int.Size / byte.Size);
					GenericTestUtils.AssertExists(new FilePath(ed, "current/" + NNStorage.GetInProgressEditsFileName
						(ExpectedTxnsFirstSeg + 1)));
				}
				ICollection<URI> imageDirs = cluster.GetNameDirs(0);
				foreach (URI uri_2 in imageDirs)
				{
					FilePath imageDir = new FilePath(uri_2.GetPath());
					FilePath savedImage = new FilePath(imageDir, "current/" + NNStorage.GetImageFileName
						(ExpectedTxnsFirstSeg));
					NUnit.Framework.Assert.IsTrue("Should have saved image at " + savedImage, savedImage
						.Exists());
				}
				// restart cluster and verify file exists
				cluster.Shutdown();
				cluster = null;
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(false
					).Build();
				cluster.WaitActive();
				fs = (cluster.GetFileSystem());
				CheckFile(fs, file, replication);
				fc = FileContext.GetFileContext(cluster.GetURI(0));
				NUnit.Framework.Assert.IsTrue(fc.GetFileLinkStatus(symlink).IsSymlink());
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				Cleanup(cluster);
				cluster = null;
			}
		}

		/* Test case to test CheckpointSignature */
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckpointSignature()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = new HdfsConfiguration();
			SecondaryNameNode secondary = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(true
					).Build();
				NameNode nn = cluster.GetNameNode();
				NamenodeProtocols nnRpc = nn.GetRpcServer();
				secondary = StartSecondaryNameNode(conf);
				// prepare checkpoint image
				secondary.DoCheckpoint();
				CheckpointSignature sig = nnRpc.RollEditLog();
				// manipulate the CheckpointSignature fields
				sig.SetBlockpoolID("somerandomebpid");
				sig.clusterID = "somerandomcid";
				try
				{
					sig.ValidateStorageInfo(nn.GetFSImage());
					// this should fail
					NUnit.Framework.Assert.IsTrue("This test is expected to fail.", false);
				}
				catch (Exception)
				{
				}
			}
			finally
			{
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>
		/// Tests the following sequence of events:
		/// - secondary successfully makes a checkpoint
		/// - it then fails while trying to upload it
		/// - it then fails again for the same reason
		/// - it then tries to checkpoint a third time
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckpointAfterTwoFailedUploads()
		{
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			Configuration conf = new HdfsConfiguration();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(true
					).Build();
				secondary = StartSecondaryNameNode(conf);
				Org.Mockito.Mockito.DoThrow(new IOException("Injecting failure after rolling edit logs"
					)).When(faultInjector).AfterSecondaryCallsRollEditLog();
				// Fail to checkpoint once
				try
				{
					secondary.DoCheckpoint();
					NUnit.Framework.Assert.Fail("Should have failed upload");
				}
				catch (IOException ioe)
				{
					Log.Info("Got expected failure", ioe);
					NUnit.Framework.Assert.IsTrue(ioe.ToString().Contains("Injecting failure"));
				}
				// Fail to checkpoint again
				try
				{
					secondary.DoCheckpoint();
					NUnit.Framework.Assert.Fail("Should have failed upload");
				}
				catch (IOException ioe)
				{
					Log.Info("Got expected failure", ioe);
					NUnit.Framework.Assert.IsTrue(ioe.ToString().Contains("Injecting failure"));
				}
				finally
				{
					Org.Mockito.Mockito.Reset(faultInjector);
				}
				// Now with the cleared error simulation, it should succeed
				secondary.DoCheckpoint();
			}
			finally
			{
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>
		/// Starts two namenodes and two secondary namenodes, verifies that secondary
		/// namenodes are configured correctly to talk to their respective namenodes
		/// and can do the checkpoint.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleSecondaryNamenodes()
		{
			Configuration conf = new HdfsConfiguration();
			string nameserviceId1 = "ns1";
			string nameserviceId2 = "ns2";
			conf.Set(DFSConfigKeys.DfsNameservices, nameserviceId1 + "," + nameserviceId2);
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary1 = null;
			SecondaryNameNode secondary2 = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleFederatedTopology
					(conf.Get(DFSConfigKeys.DfsNameservices))).Build();
				Configuration snConf1 = new HdfsConfiguration(cluster.GetConfiguration(0));
				Configuration snConf2 = new HdfsConfiguration(cluster.GetConfiguration(1));
				IPEndPoint nn1RpcAddress = cluster.GetNameNode(0).GetNameNodeAddress();
				IPEndPoint nn2RpcAddress = cluster.GetNameNode(1).GetNameNodeAddress();
				string nn1 = nn1RpcAddress.GetHostName() + ":" + nn1RpcAddress.Port;
				string nn2 = nn2RpcAddress.GetHostName() + ":" + nn2RpcAddress.Port;
				// Set the Service Rpc address to empty to make sure the node specific
				// setting works
				snConf1.Set(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, string.Empty);
				snConf2.Set(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, string.Empty);
				// Set the nameserviceIds
				snConf1.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, 
					nameserviceId1), nn1);
				snConf2.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, 
					nameserviceId2), nn2);
				secondary1 = StartSecondaryNameNode(snConf1);
				secondary2 = StartSecondaryNameNode(snConf2);
				// make sure the two secondary namenodes are talking to correct namenodes.
				NUnit.Framework.Assert.AreEqual(secondary1.GetNameNodeAddress().Port, nn1RpcAddress
					.Port);
				NUnit.Framework.Assert.AreEqual(secondary2.GetNameNodeAddress().Port, nn2RpcAddress
					.Port);
				NUnit.Framework.Assert.IsTrue(secondary1.GetNameNodeAddress().Port != secondary2.
					GetNameNodeAddress().Port);
				// both should checkpoint.
				secondary1.DoCheckpoint();
				secondary2.DoCheckpoint();
			}
			finally
			{
				Cleanup(secondary1);
				secondary1 = null;
				Cleanup(secondary2);
				secondary2 = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>
		/// Test that the secondary doesn't have to re-download image
		/// if it hasn't changed.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSecondaryImageDownload()
		{
			Log.Info("Starting testSecondaryImageDownload");
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, "0.0.0.0:0");
			Path dir = new Path("/checkpoint");
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes
				).Format(true).Build();
			cluster.WaitActive();
			FileSystem fileSys = cluster.GetFileSystem();
			FSImage image = cluster.GetNameNode().GetFSImage();
			SecondaryNameNode secondary = null;
			try
			{
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(dir));
				//
				// Make the checkpoint
				//
				secondary = StartSecondaryNameNode(conf);
				FilePath secondaryDir = new FilePath(MiniDFSCluster.GetBaseDirectory(), "namesecondary1"
					);
				FilePath secondaryCurrent = new FilePath(secondaryDir, "current");
				long expectedTxIdToDownload = cluster.GetNameNode().GetFSImage().GetStorage().GetMostRecentCheckpointTxId
					();
				FilePath secondaryFsImageBefore = new FilePath(secondaryCurrent, NNStorage.GetImageFileName
					(expectedTxIdToDownload));
				FilePath secondaryFsImageAfter = new FilePath(secondaryCurrent, NNStorage.GetImageFileName
					(expectedTxIdToDownload + 2));
				NUnit.Framework.Assert.IsFalse("Secondary should start with empty current/ dir " 
					+ "but " + secondaryFsImageBefore + " exists", secondaryFsImageBefore.Exists());
				NUnit.Framework.Assert.IsTrue("Secondary should have loaded an image", secondary.
					DoCheckpoint());
				NUnit.Framework.Assert.IsTrue("Secondary should have downloaded original image", 
					secondaryFsImageBefore.Exists());
				NUnit.Framework.Assert.IsTrue("Secondary should have created a new image", secondaryFsImageAfter
					.Exists());
				long fsimageLength = secondaryFsImageBefore.Length();
				NUnit.Framework.Assert.AreEqual("Image size should not have changed", fsimageLength
					, secondaryFsImageAfter.Length());
				// change namespace
				fileSys.Mkdirs(dir);
				NUnit.Framework.Assert.IsFalse("Another checkpoint should not have to re-load image"
					, secondary.DoCheckpoint());
				foreach (Storage.StorageDirectory sd in image.GetStorage().DirIterable(NNStorage.NameNodeDirType
					.Image))
				{
					FilePath imageFile = NNStorage.GetImageFile(sd, NNStorage.NameNodeFile.Image, expectedTxIdToDownload
						 + 5);
					NUnit.Framework.Assert.IsTrue("Image size increased", imageFile.Length() > fsimageLength
						);
				}
			}
			finally
			{
				fileSys.Close();
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>
		/// Test NN restart if a failure happens in between creating the fsimage
		/// MD5 file and renaming the fsimage.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureBeforeRename()
		{
			Configuration conf = new HdfsConfiguration();
			FSDataOutputStream fos = null;
			SecondaryNameNode secondary = null;
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				secondary = StartSecondaryNameNode(conf);
				fos = fs.Create(new Path("tmpfile0"));
				fos.Write(new byte[] { 0, 1, 2, 3 });
				secondary.DoCheckpoint();
				fos.Write(new byte[] { 0, 1, 2, 3 });
				fos.Hsync();
				// Cause merge to fail in next checkpoint.
				Org.Mockito.Mockito.DoThrow(new IOException("Injecting failure after MD5Rename"))
					.When(faultInjector).AfterMD5Rename();
				try
				{
					secondary.DoCheckpoint();
					NUnit.Framework.Assert.Fail("Fault injection failed.");
				}
				catch (IOException)
				{
				}
				// This is expected.
				Org.Mockito.Mockito.Reset(faultInjector);
				// Namenode should still restart successfully
				cluster.RestartNameNode();
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
				Org.Mockito.Mockito.Reset(faultInjector);
			}
		}

		/// <summary>
		/// Test that a fault while downloading edits does not prevent future
		/// checkpointing
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestEditFailureBeforeRename()
		{
			Configuration conf = new HdfsConfiguration();
			SecondaryNameNode secondary = null;
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				secondary = StartSecondaryNameNode(conf);
				DFSTestUtil.CreateFile(fs, new Path("tmpfile0"), 1024, (short)1, 0l);
				secondary.DoCheckpoint();
				// Cause edit rename to fail during next checkpoint
				Org.Mockito.Mockito.DoThrow(new IOException("Injecting failure before edit rename"
					)).When(faultInjector).BeforeEditsRename();
				DFSTestUtil.CreateFile(fs, new Path("tmpfile1"), 1024, (short)1, 0l);
				try
				{
					secondary.DoCheckpoint();
					NUnit.Framework.Assert.Fail("Fault injection failed.");
				}
				catch (IOException ioe)
				{
					GenericTestUtils.AssertExceptionContains("Injecting failure before edit rename", 
						ioe);
				}
				Org.Mockito.Mockito.Reset(faultInjector);
				// truncate the tmp edits file to simulate a partial download
				foreach (Storage.StorageDirectory sd in secondary.GetFSImage().GetStorage().DirIterable
					(NNStorage.NameNodeDirType.Edits))
				{
					FilePath[] tmpEdits = sd.GetCurrentDir().ListFiles(tmpEditsFilter);
					NUnit.Framework.Assert.IsTrue("Expected a single tmp edits file in directory " + 
						sd.ToString(), tmpEdits.Length == 1);
					RandomAccessFile randFile = new RandomAccessFile(tmpEdits[0], "rw");
					randFile.SetLength(0);
					randFile.Close();
				}
				// Next checkpoint should succeed
				secondary.DoCheckpoint();
			}
			finally
			{
				if (secondary != null)
				{
					secondary.Shutdown();
				}
				if (fs != null)
				{
					fs.Close();
				}
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
				Org.Mockito.Mockito.Reset(faultInjector);
			}
		}

		/// <summary>
		/// Test that a fault while downloading edits the first time after the 2NN
		/// starts up does not prevent future checkpointing.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestEditFailureOnFirstCheckpoint()
		{
			Configuration conf = new HdfsConfiguration();
			SecondaryNameNode secondary = null;
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				fs.Mkdirs(new Path("test-file-1"));
				// Make sure the on-disk fsimage on the NN has txid > 0.
				FSNamesystem fsns = cluster.GetNamesystem();
				fsns.EnterSafeMode(false);
				fsns.SaveNamespace();
				fsns.LeaveSafeMode();
				secondary = StartSecondaryNameNode(conf);
				// Cause edit rename to fail during next checkpoint
				Org.Mockito.Mockito.DoThrow(new IOException("Injecting failure before edit rename"
					)).When(faultInjector).BeforeEditsRename();
				try
				{
					secondary.DoCheckpoint();
					NUnit.Framework.Assert.Fail("Fault injection failed.");
				}
				catch (IOException ioe)
				{
					GenericTestUtils.AssertExceptionContains("Injecting failure before edit rename", 
						ioe);
				}
				Org.Mockito.Mockito.Reset(faultInjector);
				// Next checkpoint should succeed
				secondary.DoCheckpoint();
			}
			finally
			{
				if (secondary != null)
				{
					secondary.Shutdown();
				}
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				Org.Mockito.Mockito.Reset(faultInjector);
			}
		}

		/// <summary>
		/// Test that the secondary namenode correctly deletes temporary edits
		/// on startup.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDeleteTemporaryEditsOnStartup()
		{
			Configuration conf = new HdfsConfiguration();
			SecondaryNameNode secondary = null;
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				secondary = StartSecondaryNameNode(conf);
				DFSTestUtil.CreateFile(fs, new Path("tmpfile0"), 1024, (short)1, 0l);
				secondary.DoCheckpoint();
				// Cause edit rename to fail during next checkpoint
				Org.Mockito.Mockito.DoThrow(new IOException("Injecting failure before edit rename"
					)).When(faultInjector).BeforeEditsRename();
				DFSTestUtil.CreateFile(fs, new Path("tmpfile1"), 1024, (short)1, 0l);
				try
				{
					secondary.DoCheckpoint();
					NUnit.Framework.Assert.Fail("Fault injection failed.");
				}
				catch (IOException ioe)
				{
					GenericTestUtils.AssertExceptionContains("Injecting failure before edit rename", 
						ioe);
				}
				Org.Mockito.Mockito.Reset(faultInjector);
				// Verify that a temp edits file is present
				foreach (Storage.StorageDirectory sd in secondary.GetFSImage().GetStorage().DirIterable
					(NNStorage.NameNodeDirType.Edits))
				{
					FilePath[] tmpEdits = sd.GetCurrentDir().ListFiles(tmpEditsFilter);
					NUnit.Framework.Assert.IsTrue("Expected a single tmp edits file in directory " + 
						sd.ToString(), tmpEdits.Length == 1);
				}
				// Restart 2NN
				secondary.Shutdown();
				secondary = StartSecondaryNameNode(conf);
				// Verify that tmp files were deleted
				foreach (Storage.StorageDirectory sd_1 in secondary.GetFSImage().GetStorage().DirIterable
					(NNStorage.NameNodeDirType.Edits))
				{
					FilePath[] tmpEdits = sd_1.GetCurrentDir().ListFiles(tmpEditsFilter);
					NUnit.Framework.Assert.IsTrue("Did not expect a tmp edits file in directory " + sd_1
						.ToString(), tmpEdits.Length == 0);
				}
				// Next checkpoint should succeed
				secondary.DoCheckpoint();
			}
			finally
			{
				if (secondary != null)
				{
					secondary.Shutdown();
				}
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				Org.Mockito.Mockito.Reset(faultInjector);
			}
		}

		/// <summary>
		/// Test case where two secondary namenodes are checkpointing the same
		/// NameNode.
		/// </summary>
		/// <remarks>
		/// Test case where two secondary namenodes are checkpointing the same
		/// NameNode. This differs from
		/// <see cref="TestMultipleSecondaryNamenodes()"/>
		/// since that test runs against two distinct NNs.
		/// This case tests the following interleaving:
		/// - 2NN A downloads image (up to txid 2)
		/// - 2NN A about to save its own checkpoint
		/// - 2NN B downloads image (up to txid 4)
		/// - 2NN B uploads checkpoint (txid 4)
		/// - 2NN A uploads checkpoint (txid 2)
		/// It verifies that this works even though the earlier-txid checkpoint gets
		/// uploaded after the later-txid checkpoint.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleSecondaryNNsAgainstSameNN()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary1 = null;
			SecondaryNameNode secondary2 = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(true).Build();
				// Start 2NNs
				secondary1 = StartSecondaryNameNode(conf, 1);
				secondary2 = StartSecondaryNameNode(conf, 2);
				// Make the first 2NN's checkpoint process delayable - we can pause it
				// right before it saves its checkpoint image.
				SecondaryNameNode.CheckpointStorage spyImage1 = SpyOnSecondaryImage(secondary1);
				GenericTestUtils.DelayAnswer delayer = new GenericTestUtils.DelayAnswer(Log);
				Org.Mockito.Mockito.DoAnswer(delayer).When(spyImage1).SaveFSImageInAllDirs(Org.Mockito.Mockito
					.Any<FSNamesystem>(), Org.Mockito.Mockito.AnyLong());
				// Set up a thread to do a checkpoint from the first 2NN
				TestCheckpoint.DoCheckpointThread checkpointThread = new TestCheckpoint.DoCheckpointThread
					(secondary1);
				checkpointThread.Start();
				// Wait for the first checkpointer to get to where it should save its image.
				delayer.WaitForCall();
				// Now make the second checkpointer run an entire checkpoint
				secondary2.DoCheckpoint();
				// Let the first one finish
				delayer.Proceed();
				// It should have succeeded even though another checkpoint raced with it.
				checkpointThread.Join();
				checkpointThread.PropagateExceptions();
				// primary should record "last checkpoint" as the higher txid (even though
				// a checkpoint with a lower txid finished most recently)
				NNStorage storage = cluster.GetNameNode().GetFSImage().GetStorage();
				NUnit.Framework.Assert.AreEqual(4, storage.GetMostRecentCheckpointTxId());
				// Should have accepted both checkpoints
				FSImageTestUtil.AssertNNHasCheckpoints(cluster, ImmutableList.Of(2, 4));
				// Now have second one checkpoint one more time just to make sure that
				// the NN isn't left in a broken state
				secondary2.DoCheckpoint();
				// NN should have received new checkpoint
				NUnit.Framework.Assert.AreEqual(6, storage.GetMostRecentCheckpointTxId());
				// Validate invariant that files named the same are the same.
				AssertParallelFilesInvariant(cluster, ImmutableList.Of(secondary1, secondary2));
				// NN should have removed the checkpoint at txid 2 at this point, but has
				// one at txid 6
				FSImageTestUtil.AssertNNHasCheckpoints(cluster, ImmutableList.Of(4, 6));
			}
			finally
			{
				Cleanup(secondary1);
				secondary1 = null;
				Cleanup(secondary2);
				secondary2 = null;
				if (cluster != null)
				{
					cluster.Shutdown();
					cluster = null;
				}
			}
		}

		/// <summary>
		/// Test case where two secondary namenodes are checkpointing the same
		/// NameNode.
		/// </summary>
		/// <remarks>
		/// Test case where two secondary namenodes are checkpointing the same
		/// NameNode. This differs from
		/// <see cref="TestMultipleSecondaryNamenodes()"/>
		/// since that test runs against two distinct NNs.
		/// This case tests the following interleaving:
		/// - 2NN A) calls rollEdits()
		/// - 2NN B) calls rollEdits()
		/// - 2NN A) paused at getRemoteEditLogManifest()
		/// - 2NN B) calls getRemoteEditLogManifest() (returns up to txid 4)
		/// - 2NN B) uploads checkpoint fsimage_4
		/// - 2NN A) allowed to proceed, also returns up to txid 4
		/// - 2NN A) uploads checkpoint fsimage_4 as well, should fail gracefully
		/// It verifies that one of the two gets an error that it's uploading a
		/// duplicate checkpoint, and the other one succeeds.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleSecondaryNNsAgainstSameNN2()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary1 = null;
			SecondaryNameNode secondary2 = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(true).Build();
				// Start 2NNs
				secondary1 = StartSecondaryNameNode(conf, 1);
				secondary2 = StartSecondaryNameNode(conf, 2);
				// Make the first 2NN's checkpoint process delayable - we can pause it
				// right before it calls getRemoteEditLogManifest.
				// The method to set up a spy on an RPC protocol is a little bit involved
				// since we can't spy directly on a proxy object. This sets up a mock
				// which delegates all its calls to the original object, instead.
				NamenodeProtocol origNN = secondary1.GetNameNode();
				Answer<object> delegator = new GenericTestUtils.DelegateAnswer(origNN);
				NamenodeProtocol spyNN = Org.Mockito.Mockito.Mock<NamenodeProtocol>(delegator);
				GenericTestUtils.DelayAnswer delayer = new _DelayAnswer_1839(delegator, Log);
				secondary1.SetNameNode(spyNN);
				Org.Mockito.Mockito.DoAnswer(delayer).When(spyNN).GetEditLogManifest(Org.Mockito.Mockito
					.AnyLong());
				// Set up a thread to do a checkpoint from the first 2NN
				TestCheckpoint.DoCheckpointThread checkpointThread = new TestCheckpoint.DoCheckpointThread
					(secondary1);
				checkpointThread.Start();
				// Wait for the first checkpointer to be about to call getEditLogManifest
				delayer.WaitForCall();
				// Now make the second checkpointer run an entire checkpoint
				secondary2.DoCheckpoint();
				// NN should have now received fsimage_4
				NNStorage storage = cluster.GetNameNode().GetFSImage().GetStorage();
				NUnit.Framework.Assert.AreEqual(4, storage.GetMostRecentCheckpointTxId());
				// Let the first one finish
				delayer.Proceed();
				// Letting the first node continue, it should try to upload the
				// same image, and gracefully ignore it, while logging an
				// error message.
				checkpointThread.Join();
				checkpointThread.PropagateExceptions();
				// primary should still consider fsimage_4 the latest
				NUnit.Framework.Assert.AreEqual(4, storage.GetMostRecentCheckpointTxId());
				// Now have second one checkpoint one more time just to make sure that
				// the NN isn't left in a broken state
				secondary2.DoCheckpoint();
				NUnit.Framework.Assert.AreEqual(6, storage.GetMostRecentCheckpointTxId());
				// Should have accepted both checkpoints
				FSImageTestUtil.AssertNNHasCheckpoints(cluster, ImmutableList.Of(4, 6));
				// Let the first one also go again on its own to make sure it can
				// continue at next checkpoint
				secondary1.SetNameNode(origNN);
				secondary1.DoCheckpoint();
				// NN should have received new checkpoint
				NUnit.Framework.Assert.AreEqual(8, storage.GetMostRecentCheckpointTxId());
				// Validate invariant that files named the same are the same.
				AssertParallelFilesInvariant(cluster, ImmutableList.Of(secondary1, secondary2));
				// Validate that the NN received checkpoints at expected txids
				// (i.e that both checkpoints went through)
				FSImageTestUtil.AssertNNHasCheckpoints(cluster, ImmutableList.Of(6, 8));
			}
			finally
			{
				Cleanup(secondary1);
				secondary1 = null;
				Cleanup(secondary2);
				secondary2 = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		private sealed class _DelayAnswer_1839 : GenericTestUtils.DelayAnswer
		{
			public _DelayAnswer_1839(Answer<object> delegator, Log baseArg1)
				: base(baseArg1)
			{
				this.delegator = delegator;
			}

			/// <exception cref="System.Exception"/>
			protected override object PassThrough(InvocationOnMock invocation)
			{
				return delegator.Answer(invocation);
			}

			private readonly Answer<object> delegator;
		}

		/// <summary>
		/// Test case where the name node is reformatted while the secondary namenode
		/// is running.
		/// </summary>
		/// <remarks>
		/// Test case where the name node is reformatted while the secondary namenode
		/// is running. The secondary should shut itself down if if talks to a NN
		/// with the wrong namespace.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReformatNNBetweenCheckpoints()
		{
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey, 1);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(true).Build();
				int origPort = cluster.GetNameNodePort();
				int origHttpPort = cluster.GetNameNode().GetHttpAddress().Port;
				Configuration snnConf = new Configuration(conf);
				FilePath checkpointDir = new FilePath(MiniDFSCluster.GetBaseDirectory(), "namesecondary"
					);
				snnConf.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, checkpointDir.GetAbsolutePath
					());
				secondary = StartSecondaryNameNode(snnConf);
				// secondary checkpoints once
				secondary.DoCheckpoint();
				// we reformat primary NN
				cluster.Shutdown();
				cluster = null;
				// Brief sleep to make sure that the 2NN's IPC connection to the NN
				// is dropped.
				try
				{
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception)
				{
				}
				// Start a new NN with the same host/port.
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).NameNodePort(origPort)
					.NameNodeHttpPort(origHttpPort).Format(true).Build();
				try
				{
					secondary.DoCheckpoint();
					NUnit.Framework.Assert.Fail("Should have failed checkpoint against a different namespace"
						);
				}
				catch (IOException ioe)
				{
					Log.Info("Got expected failure", ioe);
					NUnit.Framework.Assert.IsTrue(ioe.ToString().Contains("Inconsistent checkpoint"));
				}
			}
			finally
			{
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>
		/// Test that the primary NN will not serve any files to a 2NN who doesn't
		/// share its namespace ID, and also will not accept any files from one.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNamespaceVerifiedOnFileTransfer()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = new HdfsConfiguration();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(true).Build();
				NamenodeProtocols nn = cluster.GetNameNodeRpc();
				Uri fsName = DFSUtil.GetInfoServer(cluster.GetNameNode().GetServiceRpcAddress(), 
					conf, DFSUtil.GetHttpClientScheme(conf)).ToURL();
				// Make a finalized log on the server side. 
				nn.RollEditLog();
				RemoteEditLogManifest manifest = nn.GetEditLogManifest(1);
				RemoteEditLog log = manifest.GetLogs()[0];
				NNStorage dstImage = Org.Mockito.Mockito.Mock<NNStorage>();
				Org.Mockito.Mockito.DoReturn(Lists.NewArrayList(new FilePath("/wont-be-written"))
					).When(dstImage).GetFiles(Org.Mockito.Mockito.AnyObject<NNStorage.NameNodeDirType
					>(), Org.Mockito.Mockito.AnyString());
				FilePath mockImageFile = FilePath.CreateTempFile("image", string.Empty);
				FileOutputStream imageFile = new FileOutputStream(mockImageFile);
				imageFile.Write(Sharpen.Runtime.GetBytesForString("data"));
				imageFile.Close();
				Org.Mockito.Mockito.DoReturn(mockImageFile).When(dstImage).FindImageFile(Org.Mockito.Mockito
					.Any<NNStorage.NameNodeFile>(), Org.Mockito.Mockito.AnyLong());
				Org.Mockito.Mockito.DoReturn(new StorageInfo(1, 1, "X", 1, HdfsServerConstants.NodeType
					.NameNode).ToColonSeparatedString()).When(dstImage).ToColonSeparatedString();
				try
				{
					TransferFsImage.DownloadImageToStorage(fsName, 0, dstImage, false);
					NUnit.Framework.Assert.Fail("Storage info was not verified");
				}
				catch (IOException ioe)
				{
					string msg = StringUtils.StringifyException(ioe);
					NUnit.Framework.Assert.IsTrue(msg, msg.Contains("but the secondary expected"));
				}
				try
				{
					TransferFsImage.DownloadEditsToStorage(fsName, log, dstImage);
					NUnit.Framework.Assert.Fail("Storage info was not verified");
				}
				catch (IOException ioe)
				{
					string msg = StringUtils.StringifyException(ioe);
					NUnit.Framework.Assert.IsTrue(msg, msg.Contains("but the secondary expected"));
				}
				try
				{
					TransferFsImage.UploadImageFromStorage(fsName, conf, dstImage, NNStorage.NameNodeFile
						.Image, 0);
					NUnit.Framework.Assert.Fail("Storage info was not verified");
				}
				catch (IOException ioe)
				{
					string msg = StringUtils.StringifyException(ioe);
					NUnit.Framework.Assert.IsTrue(msg, msg.Contains("but the secondary expected"));
				}
			}
			finally
			{
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>
		/// Test that, if a storage directory is failed when a checkpoint occurs,
		/// the non-failed storage directory receives the checkpoint.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckpointWithFailedStorageDir()
		{
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			FilePath currentDir = null;
			Configuration conf = new HdfsConfiguration();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(true).Build();
				secondary = StartSecondaryNameNode(conf);
				// Checkpoint once
				secondary.DoCheckpoint();
				// Now primary NN experiences failure of a volume -- fake by
				// setting its current dir to a-x permissions
				NamenodeProtocols nn = cluster.GetNameNodeRpc();
				NNStorage storage = cluster.GetNameNode().GetFSImage().GetStorage();
				Storage.StorageDirectory sd0 = storage.GetStorageDir(0);
				Storage.StorageDirectory sd1 = storage.GetStorageDir(1);
				currentDir = sd0.GetCurrentDir();
				FileUtil.SetExecutable(currentDir, false);
				// Upload checkpoint when NN has a bad storage dir. This should
				// succeed and create the checkpoint in the good dir.
				secondary.DoCheckpoint();
				GenericTestUtils.AssertExists(new FilePath(sd1.GetCurrentDir(), NNStorage.GetImageFileName
					(2)));
				// Restore the good dir
				FileUtil.SetExecutable(currentDir, true);
				nn.RestoreFailedStorage("true");
				nn.RollEditLog();
				// Checkpoint again -- this should upload to both dirs
				secondary.DoCheckpoint();
				FSImageTestUtil.AssertNNHasCheckpoints(cluster, ImmutableList.Of(8));
				AssertParallelFilesInvariant(cluster, ImmutableList.Of(secondary));
			}
			finally
			{
				if (currentDir != null)
				{
					FileUtil.SetExecutable(currentDir, true);
				}
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>
		/// Test case where the NN is configured with a name-only and an edits-only
		/// dir, with storage-restore turned on.
		/// </summary>
		/// <remarks>
		/// Test case where the NN is configured with a name-only and an edits-only
		/// dir, with storage-restore turned on. In this case, if the name-only dir
		/// disappears and comes back, a new checkpoint after it has been restored
		/// should function correctly.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckpointWithSeparateDirsAfterNameFails()
		{
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			FilePath currentDir = null;
			Configuration conf = new HdfsConfiguration();
			FilePath base_dir = new FilePath(MiniDFSCluster.GetBaseDirectory());
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeNameDirRestoreKey, true);
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, MiniDFSCluster.GetBaseDirectory() +
				 "/name-only");
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, MiniDFSCluster.GetBaseDirectory() 
				+ "/edits-only");
			conf.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
				(new FilePath(base_dir, "namesecondary1")).ToString());
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(true).ManageNameDfsDirs
					(false).Build();
				secondary = StartSecondaryNameNode(conf);
				// Checkpoint once
				secondary.DoCheckpoint();
				// Now primary NN experiences failure of its only name dir -- fake by
				// setting its current dir to a-x permissions
				NamenodeProtocols nn = cluster.GetNameNodeRpc();
				NNStorage storage = cluster.GetNameNode().GetFSImage().GetStorage();
				Storage.StorageDirectory sd0 = storage.GetStorageDir(0);
				NUnit.Framework.Assert.AreEqual(NNStorage.NameNodeDirType.Image, sd0.GetStorageDirType
					());
				currentDir = sd0.GetCurrentDir();
				NUnit.Framework.Assert.AreEqual(0, FileUtil.Chmod(currentDir.GetAbsolutePath(), "000"
					));
				// Try to upload checkpoint -- this should fail since there are no
				// valid storage dirs
				try
				{
					secondary.DoCheckpoint();
					NUnit.Framework.Assert.Fail("Did not fail to checkpoint when there are no valid storage dirs"
						);
				}
				catch (IOException ioe)
				{
					GenericTestUtils.AssertExceptionContains("No targets in destination storage", ioe
						);
				}
				// Restore the good dir
				NUnit.Framework.Assert.AreEqual(0, FileUtil.Chmod(currentDir.GetAbsolutePath(), "755"
					));
				nn.RestoreFailedStorage("true");
				nn.RollEditLog();
				// Checkpoint again -- this should upload to the restored name dir
				secondary.DoCheckpoint();
				FSImageTestUtil.AssertNNHasCheckpoints(cluster, ImmutableList.Of(8));
				AssertParallelFilesInvariant(cluster, ImmutableList.Of(secondary));
			}
			finally
			{
				if (currentDir != null)
				{
					FileUtil.Chmod(currentDir.GetAbsolutePath(), "755");
				}
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>Test that the 2NN triggers a checkpoint after the configurable interval</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCheckpointTriggerOnTxnCount()
		{
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckpointTxnsKey, 10);
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckpointCheckPeriodKey, 1);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(true).Build();
				FileSystem fs = cluster.GetFileSystem();
				secondary = StartSecondaryNameNode(conf);
				secondary.StartCheckpointThread();
				NNStorage storage = secondary.GetFSImage().GetStorage();
				// 2NN should checkpoint at startup
				GenericTestUtils.WaitFor(new _Supplier_2191(storage), 200, 15000);
				// If we make 10 transactions, it should checkpoint again
				for (int i = 0; i < 10; i++)
				{
					fs.Mkdirs(new Path("/test" + i));
				}
				GenericTestUtils.WaitFor(new _Supplier_2204(storage), 200, 15000);
			}
			finally
			{
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		private sealed class _Supplier_2191 : Supplier<bool>
		{
			public _Supplier_2191(NNStorage storage)
			{
				this.storage = storage;
			}

			public bool Get()
			{
				Org.Apache.Hadoop.Hdfs.Server.Namenode.TestCheckpoint.Log.Info("Waiting for checkpoint txn id to go to 2"
					);
				return storage.GetMostRecentCheckpointTxId() == 2;
			}

			private readonly NNStorage storage;
		}

		private sealed class _Supplier_2204 : Supplier<bool>
		{
			public _Supplier_2204(NNStorage storage)
			{
				this.storage = storage;
			}

			public bool Get()
			{
				Org.Apache.Hadoop.Hdfs.Server.Namenode.TestCheckpoint.Log.Info("Waiting for checkpoint txn id to go > 2"
					);
				return storage.GetMostRecentCheckpointTxId() > 2;
			}

			private readonly NNStorage storage;
		}

		/// <summary>Test case where the secondary does a checkpoint, then stops for a while.
		/// 	</summary>
		/// <remarks>
		/// Test case where the secondary does a checkpoint, then stops for a while.
		/// In the meantime, the NN saves its image several times, so that the
		/// logs that connect the 2NN's old checkpoint to the current txid
		/// get archived. Then, the 2NN tries to checkpoint again.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSecondaryHasVeryOutOfDateImage()
		{
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			Configuration conf = new HdfsConfiguration();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(true
					).Build();
				secondary = StartSecondaryNameNode(conf);
				// Checkpoint once
				secondary.DoCheckpoint();
				// Now primary NN saves namespace 3 times
				NamenodeProtocols nn = cluster.GetNameNodeRpc();
				nn.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, false);
				for (int i = 0; i < 3; i++)
				{
					nn.SaveNamespace();
				}
				nn.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, false);
				// Now the secondary tries to checkpoint again with its
				// old image in memory.
				secondary.DoCheckpoint();
			}
			finally
			{
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>Regression test for HDFS-3678 "Edit log files are never being purged from 2NN"
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSecondaryPurgesEditLogs()
		{
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeNumExtraEditsRetainedKey, 0);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(true).Build();
				FileSystem fs = cluster.GetFileSystem();
				fs.Mkdirs(new Path("/foo"));
				secondary = StartSecondaryNameNode(conf);
				// Checkpoint a few times. Doing this will cause a log roll, and thus
				// several edit log segments on the 2NN.
				for (int i = 0; i < 5; i++)
				{
					secondary.DoCheckpoint();
				}
				// Make sure there are no more edit log files than there should be.
				IList<FilePath> checkpointDirs = GetCheckpointCurrentDirs(secondary);
				foreach (FilePath checkpointDir in checkpointDirs)
				{
					IList<FileJournalManager.EditLogFile> editsFiles = FileJournalManager.MatchEditLogs
						(checkpointDir);
					NUnit.Framework.Assert.AreEqual("Edit log files were not purged from 2NN", 1, editsFiles
						.Count);
				}
			}
			finally
			{
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>
		/// Regression test for HDFS-3835 - "Long-lived 2NN cannot perform a
		/// checkpoint if security is enabled and the NN restarts without outstanding
		/// delegation tokens"
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSecondaryNameNodeWithDelegationTokens()
		{
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(true
					).Build();
				NUnit.Framework.Assert.IsNotNull(cluster.GetNamesystem().GetDelegationToken(new Text
					("atm")));
				secondary = StartSecondaryNameNode(conf);
				// Checkpoint once, so the 2NN loads the DT into its in-memory sate.
				secondary.DoCheckpoint();
				// Perform a saveNamespace, so that the NN has a new fsimage, and the 2NN
				// therefore needs to download a new fsimage the next time it performs a
				// checkpoint.
				cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, 
					false);
				cluster.GetNameNodeRpc().SaveNamespace();
				cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, 
					false);
				// Ensure that the 2NN can still perform a checkpoint.
				secondary.DoCheckpoint();
			}
			finally
			{
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <summary>Regression test for HDFS-3849.</summary>
		/// <remarks>
		/// Regression test for HDFS-3849.  This makes sure that when we re-load the
		/// FSImage in the 2NN, we clear the existing leases.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSecondaryNameNodeWithSavedLeases()
		{
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			FSDataOutputStream fos = null;
			Configuration conf = new HdfsConfiguration();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(true
					).Build();
				FileSystem fs = cluster.GetFileSystem();
				fos = fs.Create(new Path("tmpfile"));
				fos.Write(new byte[] { 0, 1, 2, 3 });
				fos.Hflush();
				NUnit.Framework.Assert.AreEqual(1, cluster.GetNamesystem().GetLeaseManager().CountLease
					());
				secondary = StartSecondaryNameNode(conf);
				NUnit.Framework.Assert.AreEqual(0, secondary.GetFSNamesystem().GetLeaseManager().
					CountLease());
				// Checkpoint once, so the 2NN loads the lease into its in-memory sate.
				secondary.DoCheckpoint();
				NUnit.Framework.Assert.AreEqual(1, secondary.GetFSNamesystem().GetLeaseManager().
					CountLease());
				fos.Close();
				fos = null;
				// Perform a saveNamespace, so that the NN has a new fsimage, and the 2NN
				// therefore needs to download a new fsimage the next time it performs a
				// checkpoint.
				cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, 
					false);
				cluster.GetNameNodeRpc().SaveNamespace();
				cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, 
					false);
				// Ensure that the 2NN can still perform a checkpoint.
				secondary.DoCheckpoint();
				// And the leases have been cleared...
				NUnit.Framework.Assert.AreEqual(0, secondary.GetFSNamesystem().GetLeaseManager().
					CountLease());
			}
			finally
			{
				if (fos != null)
				{
					fos.Close();
				}
				Cleanup(secondary);
				secondary = null;
				Cleanup(cluster);
				cluster = null;
			}
		}

		/// <exception cref="Org.Apache.Commons.Cli.ParseException"/>
		[NUnit.Framework.Test]
		public virtual void TestCommandLineParsing()
		{
			SecondaryNameNode.CommandLineOpts opts = new SecondaryNameNode.CommandLineOpts();
			opts.Parse();
			NUnit.Framework.Assert.IsNull(opts.GetCommand());
			opts.Parse("-checkpoint");
			NUnit.Framework.Assert.AreEqual(SecondaryNameNode.CommandLineOpts.Command.Checkpoint
				, opts.GetCommand());
			NUnit.Framework.Assert.IsFalse(opts.ShouldForceCheckpoint());
			opts.Parse("-checkpoint", "force");
			NUnit.Framework.Assert.AreEqual(SecondaryNameNode.CommandLineOpts.Command.Checkpoint
				, opts.GetCommand());
			NUnit.Framework.Assert.IsTrue(opts.ShouldForceCheckpoint());
			opts.Parse("-geteditsize");
			NUnit.Framework.Assert.AreEqual(SecondaryNameNode.CommandLineOpts.Command.Geteditsize
				, opts.GetCommand());
			opts.Parse("-format");
			NUnit.Framework.Assert.IsTrue(opts.ShouldFormat());
			try
			{
				opts.Parse("-geteditsize", "-checkpoint");
				NUnit.Framework.Assert.Fail("Should have failed bad parsing for two actions");
			}
			catch (ParseException e)
			{
				Log.Warn("Encountered ", e);
			}
			try
			{
				opts.Parse("-checkpoint", "xx");
				NUnit.Framework.Assert.Fail("Should have failed for bad checkpoint arg");
			}
			catch (ParseException e)
			{
				Log.Warn("Encountered ", e);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLegacyOivImage()
		{
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			FilePath tmpDir = Files.CreateTempDir();
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeLegacyOivImageDirKey, tmpDir.GetAbsolutePath());
			conf.Set(DFSConfigKeys.DfsNamenodeNumCheckpointsRetainedKey, "2");
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(true).Build();
				secondary = StartSecondaryNameNode(conf);
				// Checkpoint once
				secondary.DoCheckpoint();
				string[] files1 = tmpDir.List();
				NUnit.Framework.Assert.AreEqual("Only one file is expected", 1, files1.Length);
				// Perform more checkpointngs and check whether retention management
				// is working.
				secondary.DoCheckpoint();
				secondary.DoCheckpoint();
				string[] files2 = tmpDir.List();
				NUnit.Framework.Assert.AreEqual("Two files are expected", 2, files2.Length);
				// Verify that the first file is deleted.
				foreach (string fName in files2)
				{
					NUnit.Framework.Assert.IsFalse(fName.Equals(files1[0]));
				}
			}
			finally
			{
				Cleanup(secondary);
				Cleanup(cluster);
				tmpDir.Delete();
			}
		}

		private static void Cleanup(SecondaryNameNode snn)
		{
			if (snn != null)
			{
				try
				{
					snn.Shutdown();
				}
				catch (Exception e)
				{
					Log.Warn("Could not shut down secondary namenode", e);
				}
			}
		}

		private static void Cleanup(MiniDFSCluster cluster)
		{
			if (cluster != null)
			{
				try
				{
					cluster.Shutdown();
				}
				catch (Exception e)
				{
					Log.Warn("Could not shutdown MiniDFSCluster ", e);
				}
			}
		}

		/// <summary>
		/// Assert that if any two files have the same name across the 2NNs
		/// and NN, they should have the same content too.
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void AssertParallelFilesInvariant(MiniDFSCluster cluster, ImmutableList<SecondaryNameNode
			> secondaries)
		{
			IList<FilePath> allCurrentDirs = Lists.NewArrayList();
			Sharpen.Collections.AddAll(allCurrentDirs, FSImageTestUtil.GetNameNodeCurrentDirs
				(cluster, 0));
			foreach (SecondaryNameNode snn in secondaries)
			{
				Sharpen.Collections.AddAll(allCurrentDirs, GetCheckpointCurrentDirs(snn));
			}
			FSImageTestUtil.AssertParallelFilesAreIdentical(allCurrentDirs, ImmutableSet.Of("VERSION"
				));
		}

		private static IList<FilePath> GetCheckpointCurrentDirs(SecondaryNameNode secondary
			)
		{
			IList<FilePath> ret = Lists.NewArrayList();
			foreach (string u in secondary.GetCheckpointDirectories())
			{
				FilePath checkpointDir = new FilePath(URI.Create(u).GetPath());
				ret.AddItem(new FilePath(checkpointDir, "current"));
			}
			return ret;
		}

		private static SecondaryNameNode.CheckpointStorage SpyOnSecondaryImage(SecondaryNameNode
			 secondary1)
		{
			SecondaryNameNode.CheckpointStorage spy = Org.Mockito.Mockito.Spy((SecondaryNameNode.CheckpointStorage
				)secondary1.GetFSImage());
			secondary1.SetFSImage(spy);
			return spy;
		}

		/// <summary>A utility class to perform a checkpoint in a different thread.</summary>
		private class DoCheckpointThread : Sharpen.Thread
		{
			private readonly SecondaryNameNode snn;

			private volatile Exception thrown = null;

			internal DoCheckpointThread(SecondaryNameNode snn)
			{
				this.snn = snn;
			}

			public override void Run()
			{
				try
				{
					snn.DoCheckpoint();
				}
				catch (Exception t)
				{
					thrown = t;
				}
			}

			internal virtual void PropagateExceptions()
			{
				if (thrown != null)
				{
					throw new RuntimeException(thrown);
				}
			}
		}
	}
}
