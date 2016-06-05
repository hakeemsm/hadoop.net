using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Javax.Management;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Startup and checkpoint tests</summary>
	public class TestStartup
	{
		public const string NameNodeHost = "localhost:";

		public const string WildcardHttpHost = "0.0.0.0:";

		private static readonly Log Log = LogFactory.GetLog(typeof(TestStartup).FullName);

		private Configuration config;

		private FilePath hdfsDir = null;

		internal const long seed = unchecked((long)(0xAAAAEEFL));

		internal const int blockSize = 4096;

		internal const int fileSize = 8192;

		private long editsLength = 0;

		private long fsimageLength = 0;

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FileSystem fileSys, Path name, int repl)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)repl, blockSize);
			byte[] buffer = new byte[fileSize];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			config = new HdfsConfiguration();
			hdfsDir = new FilePath(MiniDFSCluster.GetBaseDirectory());
			if (hdfsDir.Exists() && !FileUtil.FullyDelete(hdfsDir))
			{
				throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
			}
			Log.Info("--hdfsdir is " + hdfsDir.GetAbsolutePath());
			config.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Util.FileAsURI(new FilePath(hdfsDir
				, "name")).ToString());
			config.Set(DFSConfigKeys.DfsDatanodeDataDirKey, new FilePath(hdfsDir, "data").GetPath
				());
			config.Set(DFSConfigKeys.DfsDatanodeAddressKey, "0.0.0.0:0");
			config.Set(DFSConfigKeys.DfsDatanodeHttpAddressKey, "0.0.0.0:0");
			config.Set(DFSConfigKeys.DfsDatanodeIpcAddressKey, "0.0.0.0:0");
			config.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, Util.FileAsURI(new FilePath
				(hdfsDir, "secondary")).ToString());
			config.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, WildcardHttpHost + "0"
				);
			FileSystem.SetDefaultUri(config, "hdfs://" + NameNodeHost + "0");
		}

		/// <summary>clean up</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (hdfsDir.Exists() && !FileUtil.FullyDelete(hdfsDir))
			{
				throw new IOException("Could not delete hdfs directory in tearDown '" + hdfsDir +
					 "'");
			}
		}

		/// <summary>Create a number of fsimage checkpoints</summary>
		/// <param name="count">number of checkpoints to create</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CreateCheckPoint(int count)
		{
			Log.Info("--starting mini cluster");
			// manage dirs parameter set to false 
			MiniDFSCluster cluster = null;
			SecondaryNameNode sn = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(config).ManageDataDfsDirs(false).ManageNameDfsDirs
					(false).Build();
				cluster.WaitActive();
				Log.Info("--starting Secondary Node");
				// start secondary node
				sn = new SecondaryNameNode(config);
				NUnit.Framework.Assert.IsNotNull(sn);
				// Create count new files and checkpoints
				for (int i = 0; i < count; i++)
				{
					// create a file
					FileSystem fileSys = cluster.GetFileSystem();
					Path p = new Path("t" + i);
					this.WriteFile(fileSys, p, 1);
					Log.Info("--file " + p.ToString() + " created");
					Log.Info("--doing checkpoint");
					sn.DoCheckpoint();
					// this shouldn't fail
					Log.Info("--done checkpoint");
				}
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.Fail(StringUtils.StringifyException(e));
				System.Console.Error.WriteLine("checkpoint failed");
				throw;
			}
			finally
			{
				if (sn != null)
				{
					sn.Shutdown();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				Log.Info("--cluster shutdown");
			}
		}

		/// <summary>Corrupts the MD5 sum of the fsimage.</summary>
		/// <param name="corruptAll">
		/// whether to corrupt one or all of the MD5 sums in the configured
		/// namedirs
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		private void CorruptFSImageMD5(bool corruptAll)
		{
			IList<URI> nameDirs = (IList<URI>)FSNamesystem.GetNamespaceDirs(config);
			// Corrupt the md5 files in all the namedirs
			foreach (URI uri in nameDirs)
			{
				// Directory layout looks like:
				// test/data/dfs/nameN/current/{fsimage,edits,...}
				FilePath nameDir = new FilePath(uri.GetPath());
				FilePath dfsDir = nameDir.GetParentFile();
				NUnit.Framework.Assert.AreEqual(dfsDir.GetName(), "dfs");
				// make sure we got right dir
				// Set the md5 file to all zeros
				FilePath imageFile = new FilePath(nameDir, Storage.StorageDirCurrent + "/" + NNStorage
					.GetImageFileName(0));
				MD5FileUtils.SaveMD5File(imageFile, new MD5Hash(new byte[16]));
				// Only need to corrupt one if !corruptAll
				if (!corruptAll)
				{
					break;
				}
			}
		}

		/*
		* corrupt files by removing and recreating the directory
		*/
		/// <exception cref="System.IO.IOException"/>
		private void CorruptNameNodeFiles()
		{
			// now corrupt/delete the directrory
			IList<URI> nameDirs = (IList<URI>)FSNamesystem.GetNamespaceDirs(config);
			IList<URI> nameEditsDirs = FSNamesystem.GetNamespaceEditsDirs(config);
			// get name dir and its length, then delete and recreate the directory
			FilePath dir = new FilePath(nameDirs[0].GetPath());
			// has only one
			this.fsimageLength = new FilePath(new FilePath(dir, Storage.StorageDirCurrent), NNStorage.NameNodeFile
				.Image.GetName()).Length();
			if (dir.Exists() && !(FileUtil.FullyDelete(dir)))
			{
				throw new IOException("Cannot remove directory: " + dir);
			}
			Log.Info("--removed dir " + dir + ";len was =" + this.fsimageLength);
			if (!dir.Mkdirs())
			{
				throw new IOException("Cannot create directory " + dir);
			}
			dir = new FilePath(nameEditsDirs[0].GetPath());
			//has only one
			this.editsLength = new FilePath(new FilePath(dir, Storage.StorageDirCurrent), NNStorage.NameNodeFile
				.Edits.GetName()).Length();
			if (dir.Exists() && !(FileUtil.FullyDelete(dir)))
			{
				throw new IOException("Cannot remove directory: " + dir);
			}
			if (!dir.Mkdirs())
			{
				throw new IOException("Cannot create directory " + dir);
			}
			Log.Info("--removed dir and recreated " + dir + ";len was =" + this.editsLength);
		}

		/// <summary>start with -importCheckpoint option and verify that the files are in separate directories and of the right length
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		private void CheckNameNodeFiles()
		{
			// start namenode with import option
			Log.Info("-- about to start DFS cluster");
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(config).Format(false).ManageDataDfsDirs(false
					).ManageNameDfsDirs(false).StartupOption(HdfsServerConstants.StartupOption.Import
					).Build();
				cluster.WaitActive();
				Log.Info("--NN started with checkpoint option");
				NameNode nn = cluster.GetNameNode();
				NUnit.Framework.Assert.IsNotNull(nn);
				// Verify that image file sizes did not change.
				FSImage image = nn.GetFSImage();
				VerifyDifferentDirs(image, this.fsimageLength, this.editsLength);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>verify that edits log and fsimage are in different directories and of a correct size
		/// 	</summary>
		private void VerifyDifferentDirs(FSImage img, long expectedImgSize, long expectedEditsSize
			)
		{
			Storage.StorageDirectory sd = null;
			for (IEnumerator<Storage.StorageDirectory> it = img.GetStorage().DirIterator(); it
				.HasNext(); )
			{
				sd = it.Next();
				if (sd.GetStorageDirType().IsOfType(NNStorage.NameNodeDirType.Image))
				{
					img.GetStorage();
					FilePath imf = NNStorage.GetStorageFile(sd, NNStorage.NameNodeFile.Image, 0);
					Log.Info("--image file " + imf.GetAbsolutePath() + "; len = " + imf.Length() + "; expected = "
						 + expectedImgSize);
					NUnit.Framework.Assert.AreEqual(expectedImgSize, imf.Length());
				}
				else
				{
					if (sd.GetStorageDirType().IsOfType(NNStorage.NameNodeDirType.Edits))
					{
						img.GetStorage();
						FilePath edf = NNStorage.GetStorageFile(sd, NNStorage.NameNodeFile.Edits, 0);
						Log.Info("-- edits file " + edf.GetAbsolutePath() + "; len = " + edf.Length() + "; expected = "
							 + expectedEditsSize);
						NUnit.Framework.Assert.AreEqual(expectedEditsSize, edf.Length());
					}
					else
					{
						NUnit.Framework.Assert.Fail("Image/Edits directories are not different");
					}
				}
			}
		}

		/// <summary>
		/// secnn-6
		/// checkpoint for edits and image is the same directory
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestChkpointStartup2()
		{
			Log.Info("--starting checkpointStartup2 - same directory for checkpoint");
			// different name dirs
			config.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
				(new FilePath(hdfsDir, "name")).ToString());
			config.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
				(new FilePath(hdfsDir, "edits")).ToString());
			// same checkpoint dirs
			config.Set(DFSConfigKeys.DfsNamenodeCheckpointEditsDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
				(new FilePath(hdfsDir, "chkpt")).ToString());
			config.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
				(new FilePath(hdfsDir, "chkpt")).ToString());
			CreateCheckPoint(1);
			CorruptNameNodeFiles();
			CheckNameNodeFiles();
		}

		/// <summary>
		/// seccn-8
		/// checkpoint for edits and image are different directories
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestChkpointStartup1()
		{
			//setUpConfig();
			Log.Info("--starting testStartup Recovery");
			// different name dirs
			config.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
				(new FilePath(hdfsDir, "name")).ToString());
			config.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
				(new FilePath(hdfsDir, "edits")).ToString());
			// same checkpoint dirs
			config.Set(DFSConfigKeys.DfsNamenodeCheckpointEditsDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
				(new FilePath(hdfsDir, "chkpt_edits")).ToString());
			config.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
				(new FilePath(hdfsDir, "chkpt")).ToString());
			CreateCheckPoint(1);
			CorruptNameNodeFiles();
			CheckNameNodeFiles();
		}

		/// <summary>
		/// secnn-7
		/// secondary node copies fsimage and edits into correct separate directories.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSNNStartup()
		{
			//setUpConfig();
			Log.Info("--starting SecondNN startup test");
			// different name dirs
			config.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
				(new FilePath(hdfsDir, "name")).ToString());
			config.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
				(new FilePath(hdfsDir, "name")).ToString());
			// same checkpoint dirs
			config.Set(DFSConfigKeys.DfsNamenodeCheckpointEditsDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
				(new FilePath(hdfsDir, "chkpt_edits")).ToString());
			config.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
				(new FilePath(hdfsDir, "chkpt")).ToString());
			Log.Info("--starting NN ");
			MiniDFSCluster cluster = null;
			SecondaryNameNode sn = null;
			NameNode nn = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(config).ManageDataDfsDirs(false).ManageNameDfsDirs
					(false).Build();
				cluster.WaitActive();
				nn = cluster.GetNameNode();
				NUnit.Framework.Assert.IsNotNull(nn);
				// start secondary node
				Log.Info("--starting SecondNN");
				sn = new SecondaryNameNode(config);
				NUnit.Framework.Assert.IsNotNull(sn);
				Log.Info("--doing checkpoint");
				sn.DoCheckpoint();
				// this shouldn't fail
				Log.Info("--done checkpoint");
				// now verify that image and edits are created in the different directories
				FSImage image = nn.GetFSImage();
				Storage.StorageDirectory sd = image.GetStorage().GetStorageDir(0);
				//only one
				NUnit.Framework.Assert.AreEqual(sd.GetStorageDirType(), NNStorage.NameNodeDirType
					.ImageAndEdits);
				image.GetStorage();
				FilePath imf = NNStorage.GetStorageFile(sd, NNStorage.NameNodeFile.Image, 0);
				image.GetStorage();
				FilePath edf = NNStorage.GetStorageFile(sd, NNStorage.NameNodeFile.Edits, 0);
				Log.Info("--image file " + imf.GetAbsolutePath() + "; len = " + imf.Length());
				Log.Info("--edits file " + edf.GetAbsolutePath() + "; len = " + edf.Length());
				FSImage chkpImage = sn.GetFSImage();
				VerifyDifferentDirs(chkpImage, imf.Length(), edf.Length());
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.Fail(StringUtils.StringifyException(e));
				System.Console.Error.WriteLine("checkpoint failed");
				throw;
			}
			finally
			{
				if (sn != null)
				{
					sn.Shutdown();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCompression()
		{
			Log.Info("Test compressing image.");
			Configuration conf = new Configuration();
			FileSystem.SetDefaultUri(conf, "hdfs://localhost:0");
			conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "127.0.0.1:0");
			FilePath base_dir = new FilePath(PathUtils.GetTestDir(GetType()), "dfs/");
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, new FilePath(base_dir, "name").GetPath
				());
			conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, false);
			DFSTestUtil.FormatNameNode(conf);
			// create an uncompressed image
			Log.Info("Create an uncompressed fsimage");
			NameNode namenode = new NameNode(conf);
			namenode.GetNamesystem().Mkdirs("/test", new PermissionStatus("hairong", null, FsPermission
				.GetDefault()), true);
			NamenodeProtocols nnRpc = namenode.GetRpcServer();
			NUnit.Framework.Assert.IsTrue(nnRpc.GetFileInfo("/test").IsDir());
			nnRpc.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, false);
			nnRpc.SaveNamespace();
			namenode.Stop();
			namenode.Join();
			// compress image using default codec
			Log.Info("Read an uncomressed image and store it compressed using default codec."
				);
			conf.SetBoolean(DFSConfigKeys.DfsImageCompressKey, true);
			CheckNameSpace(conf);
			// read image compressed using the default and compress it using Gzip codec
			Log.Info("Read a compressed image and store it using a different codec.");
			conf.Set(DFSConfigKeys.DfsImageCompressionCodecKey, "org.apache.hadoop.io.compress.GzipCodec"
				);
			CheckNameSpace(conf);
			// read an image compressed in Gzip and store it uncompressed
			Log.Info("Read a compressed image and store it as uncompressed.");
			conf.SetBoolean(DFSConfigKeys.DfsImageCompressKey, false);
			CheckNameSpace(conf);
			// read an uncomrpessed image and store it uncompressed
			Log.Info("Read an uncompressed image and store it as uncompressed.");
			CheckNameSpace(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckNameSpace(Configuration conf)
		{
			NameNode namenode = new NameNode(conf);
			NamenodeProtocols nnRpc = namenode.GetRpcServer();
			NUnit.Framework.Assert.IsTrue(nnRpc.GetFileInfo("/test").IsDir());
			nnRpc.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, false);
			nnRpc.SaveNamespace();
			namenode.Stop();
			namenode.Join();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestImageChecksum()
		{
			Log.Info("Test uncompressed image checksum");
			TestImageChecksum(false);
			Log.Info("Test compressed image checksum");
			TestImageChecksum(true);
		}

		/// <exception cref="System.Exception"/>
		private void TestImageChecksum(bool compress)
		{
			MiniDFSCluster cluster = null;
			if (compress)
			{
				config.SetBoolean(DFSConfigKeys.DfsImageCompressionCodecKey, true);
			}
			try
			{
				Log.Info("\n===========================================\n" + "Starting empty cluster"
					);
				cluster = new MiniDFSCluster.Builder(config).NumDataNodes(0).Format(true).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				fs.Mkdirs(new Path("/test"));
				Log.Info("Shutting down cluster #1");
				cluster.Shutdown();
				cluster = null;
				// Corrupt the md5 files in all the namedirs
				CorruptFSImageMD5(true);
				// Attach our own log appender so we can verify output
				LogVerificationAppender appender = new LogVerificationAppender();
				Logger logger = Logger.GetRootLogger();
				logger.AddAppender(appender);
				// Try to start a new cluster
				Log.Info("\n===========================================\n" + "Starting same cluster after simulated crash"
					);
				try
				{
					cluster = new MiniDFSCluster.Builder(config).NumDataNodes(0).Format(false).Build(
						);
					NUnit.Framework.Assert.Fail("Should not have successfully started with corrupt image"
						);
				}
				catch (IOException ioe)
				{
					GenericTestUtils.AssertExceptionContains("Failed to load an FSImage file!", ioe);
					int md5failures = appender.CountExceptionsWithMessage(" is corrupt with MD5 checksum of "
						);
					// Two namedirs, so should have seen two failures
					NUnit.Framework.Assert.AreEqual(2, md5failures);
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
		public virtual void TestCorruptImageFallback()
		{
			// Create two checkpoints
			CreateCheckPoint(2);
			// Delete a single md5sum
			CorruptFSImageMD5(false);
			// Should still be able to start
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(config).Format(false).ManageDataDfsDirs
				(false).ManageNameDfsDirs(false).Build();
			try
			{
				cluster.WaitActive();
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>This test tests hosts include list contains host names.</summary>
		/// <remarks>
		/// This test tests hosts include list contains host names.  After namenode
		/// restarts, the still alive datanodes should not have any trouble in getting
		/// registrant again.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNNRestart()
		{
			MiniDFSCluster cluster = null;
			FileSystem localFileSys;
			Path hostsFile;
			Path excludeFile;
			int HeartbeatInterval = 1;
			// heartbeat interval in seconds
			// Set up the hosts/exclude files.
			localFileSys = FileSystem.GetLocal(config);
			Path workingDir = localFileSys.GetWorkingDirectory();
			Path dir = new Path(workingDir, "build/test/data/work-dir/restartnn");
			hostsFile = new Path(dir, "hosts");
			excludeFile = new Path(dir, "exclude");
			// Setup conf
			config.Set(DFSConfigKeys.DfsHostsExclude, excludeFile.ToUri().GetPath());
			WriteConfigFile(localFileSys, excludeFile, null);
			config.Set(DFSConfigKeys.DfsHosts, hostsFile.ToUri().GetPath());
			// write into hosts file
			AList<string> list = new AList<string>();
			byte[] b = new byte[] { 127, 0, 0, 1 };
			IPAddress inetAddress = IPAddress.GetByAddress(b);
			list.AddItem(inetAddress.GetHostName());
			WriteConfigFile(localFileSys, hostsFile, list);
			int numDatanodes = 1;
			try
			{
				cluster = new MiniDFSCluster.Builder(config).NumDataNodes(numDatanodes).SetupHostsFile
					(true).Build();
				cluster.WaitActive();
				cluster.RestartNameNode();
				NamenodeProtocols nn = cluster.GetNameNodeRpc();
				NUnit.Framework.Assert.IsNotNull(nn);
				NUnit.Framework.Assert.IsTrue(cluster.IsDataNodeUp());
				DatanodeInfo[] info = nn.GetDatanodeReport(HdfsConstants.DatanodeReportType.Live);
				for (int i = 0; i < 5 && info.Length != numDatanodes; i++)
				{
					Sharpen.Thread.Sleep(HeartbeatInterval * 1000);
					info = nn.GetDatanodeReport(HdfsConstants.DatanodeReportType.Live);
				}
				NUnit.Framework.Assert.AreEqual("Number of live nodes should be " + numDatanodes, 
					numDatanodes, info.Length);
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.Fail(StringUtils.StringifyException(e));
				throw;
			}
			finally
			{
				CleanupFile(localFileSys, excludeFile.GetParent());
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteConfigFile(FileSystem localFileSys, Path name, AList<string> nodes
			)
		{
			// delete if it already exists
			if (localFileSys.Exists(name))
			{
				localFileSys.Delete(name, true);
			}
			FSDataOutputStream stm = localFileSys.Create(name);
			if (nodes != null)
			{
				for (IEnumerator<string> it = nodes.GetEnumerator(); it.HasNext(); )
				{
					string node = it.Next();
					stm.WriteBytes(node);
					stm.WriteBytes("\n");
				}
			}
			stm.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestXattrConfiguration()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			try
			{
				conf.SetInt(DFSConfigKeys.DfsNamenodeMaxXattrSizeKey, -1);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(true).Build();
				NUnit.Framework.Assert.Fail("Expected exception with negative xattr size");
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot set a negative value for the maximum size of an xattr"
					, e);
			}
			finally
			{
				conf.SetInt(DFSConfigKeys.DfsNamenodeMaxXattrSizeKey, DFSConfigKeys.DfsNamenodeMaxXattrSizeDefault
					);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			try
			{
				conf.SetInt(DFSConfigKeys.DfsNamenodeMaxXattrsPerInodeKey, -1);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(true).Build();
				NUnit.Framework.Assert.Fail("Expected exception with negative # xattrs per inode"
					);
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot set a negative limit on the number of xattrs per inode"
					, e);
			}
			finally
			{
				conf.SetInt(DFSConfigKeys.DfsNamenodeMaxXattrsPerInodeKey, DFSConfigKeys.DfsNamenodeMaxXattrsPerInodeDefault
					);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			try
			{
				// Set up a logger to check log message
				LogVerificationAppender appender = new LogVerificationAppender();
				Logger logger = Logger.GetRootLogger();
				logger.AddAppender(appender);
				int count = appender.CountLinesWithMessage("Maximum size of an xattr: 0 (unlimited)"
					);
				NUnit.Framework.Assert.AreEqual("Expected no messages about unlimited xattr size"
					, 0, count);
				conf.SetInt(DFSConfigKeys.DfsNamenodeMaxXattrSizeKey, 0);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(true).Build();
				count = appender.CountLinesWithMessage("Maximum size of an xattr: 0 (unlimited)");
				// happens twice because we format then run
				NUnit.Framework.Assert.AreEqual("Expected unlimited xattr size", 2, count);
			}
			finally
			{
				conf.SetInt(DFSConfigKeys.DfsNamenodeMaxXattrSizeKey, DFSConfigKeys.DfsNamenodeMaxXattrSizeDefault
					);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Verify the following scenario.</summary>
		/// <remarks>
		/// Verify the following scenario.
		/// 1. NN restarts.
		/// 2. Heartbeat RPC will retry and succeed. NN asks DN to reregister.
		/// 3. After reregistration completes, DN will send Heartbeat, followed by
		/// Blockreport.
		/// 4. NN will mark DatanodeStorageInfo#blockContentsStale to false.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestStorageBlockContentsStaleAfterNNRestart()
		{
			MiniDFSCluster dfsCluster = null;
			try
			{
				Configuration config = new Configuration();
				dfsCluster = new MiniDFSCluster.Builder(config).NumDataNodes(1).Build();
				dfsCluster.WaitActive();
				dfsCluster.RestartNameNode(true);
				BlockManagerTestUtil.CheckHeartbeat(dfsCluster.GetNamesystem().GetBlockManager());
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanNameFsns = new ObjectName("Hadoop:service=NameNode,name=FSNamesystemState"
					);
				int numStaleStorages = (int)(mbs.GetAttribute(mxbeanNameFsns, "NumStaleStorages")
					);
				NUnit.Framework.Assert.AreEqual(0, numStaleStorages);
			}
			finally
			{
				if (dfsCluster != null)
				{
					dfsCluster.Shutdown();
				}
			}
			return;
		}
	}
}
