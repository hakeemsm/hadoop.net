using System;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests various cases during file creation.</summary>
	public class TestFileCreation
	{
		internal static readonly string Dir = "/" + typeof(Org.Apache.Hadoop.Hdfs.TestFileCreation
			).Name + "/";

		private const string RpcDetailedMetrics = "RpcDetailedActivityForPort";

		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 8192;

		internal const int numBlocks = 2;

		internal const int fileSize = numBlocks * blockSize + 1;

		internal bool simulatedStorage = false;

		private static readonly string[] NonCanonicalPaths = new string[] { "//foo", "///foo2"
			, "//dir//file", "////test2/file", "/dir/./file2", "/dir/../file3" };

		//((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
		// creates a file but does not close it
		/// <exception cref="System.IO.IOException"/>
		public static FSDataOutputStream CreateFile(FileSystem fileSys, Path name, int repl
			)
		{
			System.Console.Out.WriteLine("createFile: Created " + name + " with " + repl + " replica."
				);
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)repl, blockSize);
			return stm;
		}

		/// <exception cref="System.IO.IOException"/>
		public static HdfsDataOutputStream Create(DistributedFileSystem dfs, Path name, int
			 repl)
		{
			return (HdfsDataOutputStream)CreateFile(dfs, name, repl);
		}

		//
		// writes to file but does not close it
		//
		/// <exception cref="System.IO.IOException"/>
		internal static void WriteFile(FSDataOutputStream stm)
		{
			WriteFile(stm, fileSize);
		}

		//
		// writes specified bytes to file.
		//
		/// <exception cref="System.IO.IOException"/>
		public static void WriteFile(FSDataOutputStream stm, int size)
		{
			byte[] buffer = AppendTestUtil.RandomBytes(seed, size);
			stm.Write(buffer, 0, size);
		}

		/// <summary>Test that server default values can be retrieved on the client side</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestServerDefaults()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, DFSConfigKeys.DfsBlockSizeDefault);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, DFSConfigKeys.DfsBytesPerChecksumDefault
				);
			conf.SetInt(DFSConfigKeys.DfsClientWritePacketSizeKey, DFSConfigKeys.DfsClientWritePacketSizeDefault
				);
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, DFSConfigKeys.DfsReplicationDefault 
				+ 1);
			conf.SetInt(CommonConfigurationKeysPublic.IoFileBufferSizeKey, CommonConfigurationKeysPublic
				.IoFileBufferSizeDefault);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(DFSConfigKeys
				.DfsReplicationDefault + 1).Build();
			cluster.WaitActive();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				FsServerDefaults serverDefaults = fs.GetServerDefaults();
				NUnit.Framework.Assert.AreEqual(DFSConfigKeys.DfsBlockSizeDefault, serverDefaults
					.GetBlockSize());
				NUnit.Framework.Assert.AreEqual(DFSConfigKeys.DfsBytesPerChecksumDefault, serverDefaults
					.GetBytesPerChecksum());
				NUnit.Framework.Assert.AreEqual(DFSConfigKeys.DfsClientWritePacketSizeDefault, serverDefaults
					.GetWritePacketSize());
				NUnit.Framework.Assert.AreEqual(DFSConfigKeys.DfsReplicationDefault + 1, serverDefaults
					.GetReplication());
				NUnit.Framework.Assert.AreEqual(CommonConfigurationKeysPublic.IoFileBufferSizeDefault
					, serverDefaults.GetFileBufferSize());
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileCreation()
		{
			CheckFileCreation(null, false);
		}

		/// <summary>Same test but the client should use DN hostnames</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileCreationUsingHostname()
		{
			Assume.AssumeTrue(Runtime.GetProperty("os.name").StartsWith("Linux"));
			CheckFileCreation(null, true);
		}

		/// <summary>Same test but the client should bind to a local interface</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileCreationSetLocalInterface()
		{
			Assume.AssumeTrue(Runtime.GetProperty("os.name").StartsWith("Linux"));
			// The mini cluster listens on the loopback so we can use it here
			CheckFileCreation("lo", false);
			try
			{
				CheckFileCreation("bogus-interface", false);
				NUnit.Framework.Assert.Fail("Able to specify a bogus interface");
			}
			catch (UnknownHostException e)
			{
				NUnit.Framework.Assert.AreEqual("No such interface bogus-interface", e.Message);
			}
		}

		/// <summary>Test if file creation and disk space consumption works right</summary>
		/// <param name="netIf">the local interface, if any, clients should use to access DNs
		/// 	</param>
		/// <param name="useDnHostname">whether the client should contact DNs by hostname</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CheckFileCreation(string netIf, bool useDnHostname)
		{
			Configuration conf = new HdfsConfiguration();
			if (netIf != null)
			{
				conf.Set(DFSConfigKeys.DfsClientLocalInterfaces, netIf);
			}
			conf.SetBoolean(DFSConfigKeys.DfsClientUseDnHostname, useDnHostname);
			if (useDnHostname)
			{
				// Since the mini cluster only listens on the loopback we have to
				// ensure the hostname used to access DNs maps to the loopback. We
				// do this by telling the DN to advertise localhost as its hostname
				// instead of the default hostname.
				conf.Set(DFSConfigKeys.DfsDatanodeHostNameKey, "localhost");
			}
			if (simulatedStorage)
			{
				SimulatedFSDataset.SetFactory(conf);
			}
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).CheckDataNodeHostConfig
				(true).Build();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				//
				// check that / exists
				//
				Path path = new Path("/");
				System.Console.Out.WriteLine("Path : \"" + path.ToString() + "\"");
				System.Console.Out.WriteLine(fs.GetFileStatus(path).IsDirectory());
				NUnit.Framework.Assert.IsTrue("/ should be a directory", fs.GetFileStatus(path).IsDirectory
					());
				//
				// Create a directory inside /, then try to overwrite it
				//
				Path dir1 = new Path("/test_dir");
				fs.Mkdirs(dir1);
				System.Console.Out.WriteLine("createFile: Creating " + dir1.GetName() + " for overwrite of existing directory."
					);
				try
				{
					fs.Create(dir1, true);
					// Create path, overwrite=true
					fs.Close();
					NUnit.Framework.Assert.IsTrue("Did not prevent directory from being overwritten."
						, false);
				}
				catch (FileAlreadyExistsException)
				{
				}
				// expected
				//
				// create a new file in home directory. Do not close it.
				//
				Path file1 = new Path("filestatus.dat");
				Path parent = file1.GetParent();
				fs.Mkdirs(parent);
				DistributedFileSystem dfs = (DistributedFileSystem)fs;
				dfs.SetQuota(file1.GetParent(), 100L, blockSize * 5);
				FSDataOutputStream stm = CreateFile(fs, file1, 1);
				// verify that file exists in FS namespace
				NUnit.Framework.Assert.IsTrue(file1 + " should be a file", fs.GetFileStatus(file1
					).IsFile());
				System.Console.Out.WriteLine("Path : \"" + file1 + "\"");
				// write to file
				WriteFile(stm);
				stm.Close();
				// verify that file size has changed to the full size
				long len = fs.GetFileStatus(file1).GetLen();
				NUnit.Framework.Assert.IsTrue(file1 + " should be of size " + fileSize + " but found to be of size "
					 + len, len == fileSize);
				// verify the disk space the file occupied
				long diskSpace = dfs.GetContentSummary(file1.GetParent()).GetLength();
				NUnit.Framework.Assert.AreEqual(file1 + " should take " + fileSize + " bytes disk space "
					 + "but found to take " + diskSpace + " bytes", fileSize, diskSpace);
				// Check storage usage 
				// can't check capacities for real storage since the OS file system may be changing under us.
				if (simulatedStorage)
				{
					DataNode dn = cluster.GetDataNodes()[0];
					FsDatasetSpi<object> dataset = DataNodeTestUtils.GetFSDataset(dn);
					NUnit.Framework.Assert.AreEqual(fileSize, dataset.GetDfsUsed());
					NUnit.Framework.Assert.AreEqual(SimulatedFSDataset.DefaultCapacity - fileSize, dataset
						.GetRemaining());
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test deleteOnExit</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteOnExit()
		{
			Configuration conf = new HdfsConfiguration();
			if (simulatedStorage)
			{
				SimulatedFSDataset.SetFactory(conf);
			}
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			FileSystem localfs = FileSystem.GetLocal(conf);
			try
			{
				// Creates files in HDFS and local file system.
				//
				Path file1 = new Path("filestatus.dat");
				Path file2 = new Path("filestatus2.dat");
				Path file3 = new Path("filestatus3.dat");
				FSDataOutputStream stm1 = CreateFile(fs, file1, 1);
				FSDataOutputStream stm2 = CreateFile(fs, file2, 1);
				FSDataOutputStream stm3 = CreateFile(localfs, file3, 1);
				System.Console.Out.WriteLine("DeleteOnExit: Created files.");
				// write to files and close. Purposely, do not close file2.
				WriteFile(stm1);
				WriteFile(stm3);
				stm1.Close();
				stm2.Close();
				stm3.Close();
				// set delete on exit flag on files.
				fs.DeleteOnExit(file1);
				fs.DeleteOnExit(file2);
				localfs.DeleteOnExit(file3);
				// close the file system. This should make the above files
				// disappear.
				fs.Close();
				localfs.Close();
				fs = null;
				localfs = null;
				// reopen file system and verify that file does not exist.
				fs = cluster.GetFileSystem();
				localfs = FileSystem.GetLocal(conf);
				NUnit.Framework.Assert.IsTrue(file1 + " still exists inspite of deletOnExit set."
					, !fs.Exists(file1));
				NUnit.Framework.Assert.IsTrue(file2 + " still exists inspite of deletOnExit set."
					, !fs.Exists(file2));
				NUnit.Framework.Assert.IsTrue(file3 + " still exists inspite of deletOnExit set."
					, !localfs.Exists(file3));
				System.Console.Out.WriteLine("DeleteOnExit successful.");
			}
			finally
			{
				IOUtils.CloseStream(fs);
				IOUtils.CloseStream(localfs);
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test that a file which is open for write is overwritten by another
		/// client.
		/// </summary>
		/// <remarks>
		/// Test that a file which is open for write is overwritten by another
		/// client. Regression test for HDFS-3755.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOverwriteOpenForWrite()
		{
			Configuration conf = new HdfsConfiguration();
			SimulatedFSDataset.SetFactory(conf);
			conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, false);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			UserGroupInformation otherUgi = UserGroupInformation.CreateUserForTesting("testuser"
				, new string[] { "testgroup" });
			FileSystem fs2 = otherUgi.DoAs(new _PrivilegedExceptionAction_385(cluster));
			string metricsName = RpcDetailedMetrics + cluster.GetNameNodePort();
			try
			{
				Path p = new Path("/testfile");
				FSDataOutputStream stm1 = fs.Create(p);
				stm1.Write(1);
				MetricsAsserts.AssertCounter("CreateNumOps", 1L, MetricsAsserts.GetMetrics(metricsName
					));
				// Create file again without overwrite
				try
				{
					fs2.Create(p, false);
					NUnit.Framework.Assert.Fail("Did not throw!");
				}
				catch (IOException abce)
				{
					GenericTestUtils.AssertExceptionContains("Failed to CREATE_FILE", abce);
				}
				MetricsAsserts.AssertCounter("AlreadyBeingCreatedExceptionNumOps", 1L, MetricsAsserts.GetMetrics
					(metricsName));
				FSDataOutputStream stm2 = fs2.Create(p, true);
				stm2.Write(2);
				stm2.Close();
				try
				{
					stm1.Close();
					NUnit.Framework.Assert.Fail("Should have exception closing stm1 since it was deleted"
						);
				}
				catch (IOException ioe)
				{
					GenericTestUtils.AssertExceptionContains("No lease on /testfile", ioe);
					GenericTestUtils.AssertExceptionContains("File does not exist.", ioe);
				}
			}
			finally
			{
				IOUtils.CloseStream(fs);
				IOUtils.CloseStream(fs2);
				cluster.Shutdown();
			}
		}

		private sealed class _PrivilegedExceptionAction_385 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_385(MiniDFSCluster cluster)
			{
				this.cluster = cluster;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(cluster.GetConfiguration(0));
			}

			private readonly MiniDFSCluster cluster;
		}

		/// <summary>Test that file data does not become corrupted even in the face of errors.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileCreationError1()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			if (simulatedStorage)
			{
				SimulatedFSDataset.SetFactory(conf);
			}
			// create cluster
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			cluster.WaitActive();
			IPEndPoint addr = new IPEndPoint("localhost", cluster.GetNameNodePort());
			DFSClient client = new DFSClient(addr, conf);
			try
			{
				// create a new file.
				//
				Path file1 = new Path("/filestatus.dat");
				FSDataOutputStream stm = CreateFile(fs, file1, 1);
				// verify that file exists in FS namespace
				NUnit.Framework.Assert.IsTrue(file1 + " should be a file", fs.GetFileStatus(file1
					).IsFile());
				System.Console.Out.WriteLine("Path : \"" + file1 + "\"");
				// kill the datanode
				cluster.ShutdownDataNodes();
				// wait for the datanode to be declared dead
				while (true)
				{
					DatanodeInfo[] info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Live
						);
					if (info.Length == 0)
					{
						break;
					}
					System.Console.Out.WriteLine("testFileCreationError1: waiting for datanode " + " to die."
						);
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
					}
				}
				// write 1 byte to file. 
				// This should fail because all datanodes are dead.
				byte[] buffer = AppendTestUtil.RandomBytes(seed, 1);
				try
				{
					stm.Write(buffer);
					stm.Close();
				}
				catch (Exception)
				{
					System.Console.Out.WriteLine("Encountered expected exception");
				}
				// verify that no blocks are associated with this file
				// bad block allocations were cleaned up earlier.
				LocatedBlocks locations = client.GetNamenode().GetBlockLocations(file1.ToString()
					, 0, long.MaxValue);
				System.Console.Out.WriteLine("locations = " + locations.LocatedBlockCount());
				NUnit.Framework.Assert.IsTrue("Error blocks were not cleaned up", locations.LocatedBlockCount
					() == 0);
			}
			finally
			{
				cluster.Shutdown();
				client.Close();
			}
		}

		/// <summary>
		/// Test that the filesystem removes the last block from a file if its
		/// lease expires.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileCreationError2()
		{
			long leasePeriod = 1000;
			System.Console.Out.WriteLine("testFileCreationError2 start");
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			if (simulatedStorage)
			{
				SimulatedFSDataset.SetFactory(conf);
			}
			// create cluster
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			DistributedFileSystem dfs = null;
			try
			{
				cluster.WaitActive();
				dfs = cluster.GetFileSystem();
				DFSClient client = dfs.dfs;
				// create a new file.
				//
				Path file1 = new Path("/filestatus.dat");
				CreateFile(dfs, file1, 1);
				System.Console.Out.WriteLine("testFileCreationError2: " + "Created file filestatus.dat with one replicas."
					);
				LocatedBlocks locations = client.GetNamenode().GetBlockLocations(file1.ToString()
					, 0, long.MaxValue);
				System.Console.Out.WriteLine("testFileCreationError2: " + "The file has " + locations
					.LocatedBlockCount() + " blocks.");
				// add one block to the file
				LocatedBlock location = client.GetNamenode().AddBlock(file1.ToString(), client.clientName
					, null, null, INodeId.GrandfatherInodeId, null);
				System.Console.Out.WriteLine("testFileCreationError2: " + "Added block " + location
					.GetBlock());
				locations = client.GetNamenode().GetBlockLocations(file1.ToString(), 0, long.MaxValue
					);
				int count = locations.LocatedBlockCount();
				System.Console.Out.WriteLine("testFileCreationError2: " + "The file now has " + count
					 + " blocks.");
				// set the soft and hard limit to be 1 second so that the
				// namenode triggers lease recovery
				cluster.SetLeasePeriod(leasePeriod, leasePeriod);
				// wait for the lease to expire
				try
				{
					Sharpen.Thread.Sleep(5 * leasePeriod);
				}
				catch (Exception)
				{
				}
				// verify that the last block was synchronized.
				locations = client.GetNamenode().GetBlockLocations(file1.ToString(), 0, long.MaxValue
					);
				System.Console.Out.WriteLine("testFileCreationError2: " + "locations = " + locations
					.LocatedBlockCount());
				NUnit.Framework.Assert.AreEqual(0, locations.LocatedBlockCount());
				System.Console.Out.WriteLine("testFileCreationError2 successful");
			}
			finally
			{
				IOUtils.CloseStream(dfs);
				cluster.Shutdown();
			}
		}

		/// <summary>test addBlock(..) when replication&lt;min and excludeNodes==null.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileCreationError3()
		{
			System.Console.Out.WriteLine("testFileCreationError3 start");
			Configuration conf = new HdfsConfiguration();
			// create cluster
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
			DistributedFileSystem dfs = null;
			try
			{
				cluster.WaitActive();
				dfs = cluster.GetFileSystem();
				DFSClient client = dfs.dfs;
				// create a new file.
				Path f = new Path("/foo.txt");
				CreateFile(dfs, f, 3);
				try
				{
					cluster.GetNameNodeRpc().AddBlock(f.ToString(), client.clientName, null, null, INodeId
						.GrandfatherInodeId, null);
					NUnit.Framework.Assert.Fail();
				}
				catch (IOException ioe)
				{
					FileSystem.Log.Info("GOOD!", ioe);
				}
				System.Console.Out.WriteLine("testFileCreationError3 successful");
			}
			finally
			{
				IOUtils.CloseStream(dfs);
				cluster.Shutdown();
			}
		}

		/// <summary>Test that file leases are persisted across namenode restarts.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileCreationNamenodeRestart()
		{
			Configuration conf = new HdfsConfiguration();
			int MaxIdleTime = 2000;
			// 2s
			conf.SetInt("ipc.client.connection.maxidletime", MaxIdleTime);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			if (simulatedStorage)
			{
				SimulatedFSDataset.SetFactory(conf);
			}
			// create cluster
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			DistributedFileSystem fs = null;
			try
			{
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				int nnport = cluster.GetNameNodePort();
				// create a new file.
				Path file1 = new Path("/filestatus.dat");
				HdfsDataOutputStream stm = Create(fs, file1, 1);
				System.Console.Out.WriteLine("testFileCreationNamenodeRestart: " + "Created file "
					 + file1);
				NUnit.Framework.Assert.AreEqual(file1 + " should be replicated to 1 datanode.", 1
					, stm.GetCurrentBlockReplication());
				// write two full blocks.
				WriteFile(stm, numBlocks * blockSize);
				stm.Hflush();
				NUnit.Framework.Assert.AreEqual(file1 + " should still be replicated to 1 datanode."
					, 1, stm.GetCurrentBlockReplication());
				// rename file wile keeping it open.
				Path fileRenamed = new Path("/filestatusRenamed.dat");
				fs.Rename(file1, fileRenamed);
				System.Console.Out.WriteLine("testFileCreationNamenodeRestart: " + "Renamed file "
					 + file1 + " to " + fileRenamed);
				file1 = fileRenamed;
				// create another new file.
				//
				Path file2 = new Path("/filestatus2.dat");
				FSDataOutputStream stm2 = CreateFile(fs, file2, 1);
				System.Console.Out.WriteLine("testFileCreationNamenodeRestart: " + "Created file "
					 + file2);
				// create yet another new file with full path name. 
				// rename it while open
				//
				Path file3 = new Path("/user/home/fullpath.dat");
				FSDataOutputStream stm3 = CreateFile(fs, file3, 1);
				System.Console.Out.WriteLine("testFileCreationNamenodeRestart: " + "Created file "
					 + file3);
				Path file4 = new Path("/user/home/fullpath4.dat");
				FSDataOutputStream stm4 = CreateFile(fs, file4, 1);
				System.Console.Out.WriteLine("testFileCreationNamenodeRestart: " + "Created file "
					 + file4);
				fs.Mkdirs(new Path("/bin"));
				fs.Rename(new Path("/user/home"), new Path("/bin"));
				Path file3new = new Path("/bin/home/fullpath.dat");
				System.Console.Out.WriteLine("testFileCreationNamenodeRestart: " + "Renamed file "
					 + file3 + " to " + file3new);
				Path file4new = new Path("/bin/home/fullpath4.dat");
				System.Console.Out.WriteLine("testFileCreationNamenodeRestart: " + "Renamed file "
					 + file4 + " to " + file4new);
				// restart cluster with the same namenode port as before.
				// This ensures that leases are persisted in fsimage.
				cluster.Shutdown();
				try
				{
					Sharpen.Thread.Sleep(2 * MaxIdleTime);
				}
				catch (Exception)
				{
				}
				cluster = new MiniDFSCluster.Builder(conf).NameNodePort(nnport).Format(false).Build
					();
				cluster.WaitActive();
				// restart cluster yet again. This triggers the code to read in
				// persistent leases from fsimage.
				cluster.Shutdown();
				try
				{
					Sharpen.Thread.Sleep(5000);
				}
				catch (Exception)
				{
				}
				cluster = new MiniDFSCluster.Builder(conf).NameNodePort(nnport).Format(false).Build
					();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				// instruct the dfsclient to use a new filename when it requests
				// new blocks for files that were renamed.
				DFSOutputStream dfstream = (DFSOutputStream)(stm.GetWrappedStream());
				dfstream.SetTestFilename(file1.ToString());
				dfstream = (DFSOutputStream)(stm3.GetWrappedStream());
				dfstream.SetTestFilename(file3new.ToString());
				dfstream = (DFSOutputStream)(stm4.GetWrappedStream());
				dfstream.SetTestFilename(file4new.ToString());
				// write 1 byte to file.  This should succeed because the 
				// namenode should have persisted leases.
				byte[] buffer = AppendTestUtil.RandomBytes(seed, 1);
				stm.Write(buffer);
				stm.Close();
				stm2.Write(buffer);
				stm2.Close();
				stm3.Close();
				stm4.Close();
				// verify that new block is associated with this file
				DFSClient client = fs.dfs;
				LocatedBlocks locations = client.GetNamenode().GetBlockLocations(file1.ToString()
					, 0, long.MaxValue);
				System.Console.Out.WriteLine("locations = " + locations.LocatedBlockCount());
				NUnit.Framework.Assert.IsTrue("Error blocks were not cleaned up for file " + file1
					, locations.LocatedBlockCount() == 3);
				// verify filestatus2.dat
				locations = client.GetNamenode().GetBlockLocations(file2.ToString(), 0, long.MaxValue
					);
				System.Console.Out.WriteLine("locations = " + locations.LocatedBlockCount());
				NUnit.Framework.Assert.IsTrue("Error blocks were not cleaned up for file " + file2
					, locations.LocatedBlockCount() == 1);
			}
			finally
			{
				IOUtils.CloseStream(fs);
				cluster.Shutdown();
			}
		}

		/// <summary>Test that all open files are closed when client dies abnormally.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDFSClientDeath()
		{
			Configuration conf = new HdfsConfiguration();
			System.Console.Out.WriteLine("Testing adbornal client death.");
			if (simulatedStorage)
			{
				SimulatedFSDataset.SetFactory(conf);
			}
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			DistributedFileSystem dfs = (DistributedFileSystem)fs;
			DFSClient dfsclient = dfs.dfs;
			try
			{
				// create a new file in home directory. Do not close it.
				//
				Path file1 = new Path("/clienttest.dat");
				FSDataOutputStream stm = CreateFile(fs, file1, 1);
				System.Console.Out.WriteLine("Created file clienttest.dat");
				// write to file
				WriteFile(stm);
				// close the dfsclient before closing the output stream.
				// This should close all existing file.
				dfsclient.Close();
				// reopen file system and verify that file exists.
				NUnit.Framework.Assert.IsTrue(file1 + " does not exist.", AppendTestUtil.CreateHdfsWithDifferentUsername
					(conf).Exists(file1));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test file creation using createNonRecursive().</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileCreationNonRecursive()
		{
			Configuration conf = new HdfsConfiguration();
			if (simulatedStorage)
			{
				SimulatedFSDataset.SetFactory(conf);
			}
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			Path path = new Path("/" + Time.Now() + "-testFileCreationNonRecursive");
			FSDataOutputStream @out = null;
			try
			{
				IOException expectedException = null;
				string nonExistDir = "/non-exist-" + Time.Now();
				fs.Delete(new Path(nonExistDir), true);
				EnumSet<CreateFlag> createFlag = EnumSet.Of(CreateFlag.Create);
				// Create a new file in root dir, should succeed
				@out = CreateNonRecursive(fs, path, 1, createFlag);
				@out.Close();
				// Create a file when parent dir exists as file, should fail
				try
				{
					CreateNonRecursive(fs, new Path(path, "Create"), 1, createFlag);
				}
				catch (IOException e)
				{
					expectedException = e;
				}
				NUnit.Framework.Assert.IsTrue("Create a file when parent directory exists as a file"
					 + " should throw ParentNotDirectoryException ", expectedException != null && expectedException
					 is ParentNotDirectoryException);
				fs.Delete(path, true);
				// Create a file in a non-exist directory, should fail
				Path path2 = new Path(nonExistDir + "/testCreateNonRecursive");
				expectedException = null;
				try
				{
					CreateNonRecursive(fs, path2, 1, createFlag);
				}
				catch (IOException e)
				{
					expectedException = e;
				}
				NUnit.Framework.Assert.IsTrue("Create a file in a non-exist dir using" + " createNonRecursive() should throw FileNotFoundException "
					, expectedException != null && expectedException is FileNotFoundException);
				EnumSet<CreateFlag> overwriteFlag = EnumSet.Of(CreateFlag.Create, CreateFlag.Overwrite
					);
				// Overwrite a file in root dir, should succeed
				@out = CreateNonRecursive(fs, path, 1, overwriteFlag);
				@out.Close();
				// Overwrite a file when parent dir exists as file, should fail
				expectedException = null;
				try
				{
					CreateNonRecursive(fs, new Path(path, "Overwrite"), 1, overwriteFlag);
				}
				catch (IOException e)
				{
					expectedException = e;
				}
				NUnit.Framework.Assert.IsTrue("Overwrite a file when parent directory exists as a file"
					 + " should throw ParentNotDirectoryException ", expectedException != null && expectedException
					 is ParentNotDirectoryException);
				fs.Delete(path, true);
				// Overwrite a file in a non-exist directory, should fail
				Path path3 = new Path(nonExistDir + "/testOverwriteNonRecursive");
				expectedException = null;
				try
				{
					CreateNonRecursive(fs, path3, 1, overwriteFlag);
				}
				catch (IOException e)
				{
					expectedException = e;
				}
				NUnit.Framework.Assert.IsTrue("Overwrite a file in a non-exist dir using" + " createNonRecursive() should throw FileNotFoundException "
					, expectedException != null && expectedException is FileNotFoundException);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		// creates a file using DistributedFileSystem.createNonRecursive()
		/// <exception cref="System.IO.IOException"/>
		internal static FSDataOutputStream CreateNonRecursive(FileSystem fs, Path name, int
			 repl, EnumSet<CreateFlag> flag)
		{
			System.Console.Out.WriteLine("createNonRecursive: Created " + name + " with " + repl
				 + " replica.");
			FSDataOutputStream stm = ((DistributedFileSystem)fs).CreateNonRecursive(name, FsPermission
				.GetDefault(), flag, fs.GetConf().GetInt(CommonConfigurationKeys.IoFileBufferSizeKey
				, 4096), (short)repl, blockSize, null);
			return stm;
		}

		/// <summary>Test that file data becomes available before file is closed.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileCreationSimulated()
		{
			simulatedStorage = true;
			TestFileCreation();
			simulatedStorage = false;
		}

		/// <summary>Test creating two files at the same time.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestConcurrentFileCreation()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				Path[] p = new Path[] { new Path("/foo"), new Path("/bar") };
				//write 2 files at the same time
				FSDataOutputStream[] @out = new FSDataOutputStream[] { fs.Create(p[0]), fs.Create
					(p[1]) };
				int i = 0;
				for (; i < 100; i++)
				{
					@out[0].Write(i);
					@out[1].Write(i);
				}
				@out[0].Close();
				for (; i < 200; i++)
				{
					@out[1].Write(i);
				}
				@out[1].Close();
				//verify
				FSDataInputStream[] @in = new FSDataInputStream[] { fs.Open(p[0]), fs.Open(p[1]) };
				for (i = 0; i < 100; i++)
				{
					NUnit.Framework.Assert.AreEqual(i, @in[0].Read());
				}
				for (i = 0; i < 200; i++)
				{
					NUnit.Framework.Assert.AreEqual(i, @in[1].Read());
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

		/// <summary>Test creating a file whose data gets sync when closed</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileCreationSyncOnClose()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsDatanodeSynconcloseKey, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				Path[] p = new Path[] { new Path("/foo"), new Path("/bar") };
				//write 2 files at the same time
				FSDataOutputStream[] @out = new FSDataOutputStream[] { fs.Create(p[0]), fs.Create
					(p[1]) };
				int i = 0;
				for (; i < 100; i++)
				{
					@out[0].Write(i);
					@out[1].Write(i);
				}
				@out[0].Close();
				for (; i < 200; i++)
				{
					@out[1].Write(i);
				}
				@out[1].Close();
				//verify
				FSDataInputStream[] @in = new FSDataInputStream[] { fs.Open(p[0]), fs.Open(p[1]) };
				for (i = 0; i < 100; i++)
				{
					NUnit.Framework.Assert.AreEqual(i, @in[0].Read());
				}
				for (i = 0; i < 200; i++)
				{
					NUnit.Framework.Assert.AreEqual(i, @in[1].Read());
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

		/// <summary>Create a file, write something, hflush but not close.</summary>
		/// <remarks>
		/// Create a file, write something, hflush but not close.
		/// Then change lease period and wait for lease recovery.
		/// Finally, read the block directly from each Datanode and verify the content.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLeaseExpireHardLimit()
		{
			System.Console.Out.WriteLine("testLeaseExpireHardLimit start");
			long leasePeriod = 1000;
			int DatanodeNum = 3;
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			// create cluster
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(DatanodeNum
				).Build();
			DistributedFileSystem dfs = null;
			try
			{
				cluster.WaitActive();
				dfs = cluster.GetFileSystem();
				// create a new file.
				string f = Dir + "foo";
				Path fpath = new Path(f);
				HdfsDataOutputStream @out = Create(dfs, fpath, DatanodeNum);
				@out.Write(Sharpen.Runtime.GetBytesForString("something"));
				@out.Hflush();
				int actualRepl = @out.GetCurrentBlockReplication();
				NUnit.Framework.Assert.IsTrue(f + " should be replicated to " + DatanodeNum + " datanodes."
					, actualRepl == DatanodeNum);
				// set the soft and hard limit to be 1 second so that the
				// namenode triggers lease recovery
				cluster.SetLeasePeriod(leasePeriod, leasePeriod);
				// wait for the lease to expire
				try
				{
					Sharpen.Thread.Sleep(5 * leasePeriod);
				}
				catch (Exception)
				{
				}
				LocatedBlocks locations = dfs.dfs.GetNamenode().GetBlockLocations(f, 0, long.MaxValue
					);
				NUnit.Framework.Assert.AreEqual(1, locations.LocatedBlockCount());
				LocatedBlock locatedblock = locations.GetLocatedBlocks()[0];
				int successcount = 0;
				foreach (DatanodeInfo datanodeinfo in locatedblock.GetLocations())
				{
					DataNode datanode = cluster.GetDataNode(datanodeinfo.GetIpcPort());
					ExtendedBlock blk = locatedblock.GetBlock();
					Block b = DataNodeTestUtils.GetFSDataset(datanode).GetStoredBlock(blk.GetBlockPoolId
						(), blk.GetBlockId());
					FilePath blockfile = DataNodeTestUtils.GetFile(datanode, blk.GetBlockPoolId(), b.
						GetBlockId());
					System.Console.Out.WriteLine("blockfile=" + blockfile);
					if (blockfile != null)
					{
						BufferedReader @in = new BufferedReader(new FileReader(blockfile));
						NUnit.Framework.Assert.AreEqual("something", @in.ReadLine());
						@in.Close();
						successcount++;
					}
				}
				System.Console.Out.WriteLine("successcount=" + successcount);
				NUnit.Framework.Assert.IsTrue(successcount > 0);
			}
			finally
			{
				IOUtils.CloseStream(dfs);
				cluster.Shutdown();
			}
			System.Console.Out.WriteLine("testLeaseExpireHardLimit successful");
		}

		// test closing file system before all file handles are closed.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsClose()
		{
			System.Console.Out.WriteLine("test file system close start");
			int DatanodeNum = 3;
			Configuration conf = new HdfsConfiguration();
			// create cluster
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(DatanodeNum
				).Build();
			DistributedFileSystem dfs = null;
			try
			{
				cluster.WaitActive();
				dfs = cluster.GetFileSystem();
				// create a new file.
				string f = Dir + "foofs";
				Path fpath = new Path(f);
				FSDataOutputStream @out = Org.Apache.Hadoop.Hdfs.TestFileCreation.CreateFile(dfs, 
					fpath, DatanodeNum);
				@out.Write(Sharpen.Runtime.GetBytesForString("something"));
				// close file system without closing file
				dfs.Close();
			}
			finally
			{
				System.Console.Out.WriteLine("testFsClose successful");
				cluster.Shutdown();
			}
		}

		// test closing file after cluster is shutdown
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFsCloseAfterClusterShutdown()
		{
			System.Console.Out.WriteLine("test testFsCloseAfterClusterShutdown start");
			int DatanodeNum = 3;
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationMinKey, 3);
			conf.SetBoolean("ipc.client.ping", false);
			// hdfs timeout is default 60 seconds
			conf.SetInt("ipc.ping.interval", 10000);
			// hdfs timeout is now 10 second
			// create cluster
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(DatanodeNum
				).Build();
			DistributedFileSystem dfs = null;
			try
			{
				cluster.WaitActive();
				dfs = cluster.GetFileSystem();
				// create a new file.
				string f = Dir + "testFsCloseAfterClusterShutdown";
				Path fpath = new Path(f);
				FSDataOutputStream @out = Org.Apache.Hadoop.Hdfs.TestFileCreation.CreateFile(dfs, 
					fpath, DatanodeNum);
				@out.Write(Sharpen.Runtime.GetBytesForString("something_test"));
				@out.Hflush();
				// ensure that block is allocated
				// shutdown last datanode in pipeline.
				cluster.StopDataNode(2);
				// close file. Since we have set the minReplcatio to 3 but have killed one
				// of the three datanodes, the close call will loop until the hdfsTimeout is
				// encountered.
				bool hasException = false;
				try
				{
					@out.Close();
					System.Console.Out.WriteLine("testFsCloseAfterClusterShutdown: Error here");
				}
				catch (IOException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue("Failed to close file after cluster shutdown", hasException
					);
			}
			finally
			{
				System.Console.Out.WriteLine("testFsCloseAfterClusterShutdown successful");
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Regression test for HDFS-3626.</summary>
		/// <remarks>
		/// Regression test for HDFS-3626. Creates a file using a non-canonical path
		/// (i.e. with extra slashes between components) and makes sure that the NN
		/// can properly restart.
		/// This test RPCs directly to the NN, to ensure that even an old client
		/// which passes an invalid path won't cause corrupt edits.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateNonCanonicalPathAndRestartRpc()
		{
			DoCreateTest(TestFileCreation.CreationMethod.DirectNnRpc);
		}

		/// <summary>Another regression test for HDFS-3626.</summary>
		/// <remarks>
		/// Another regression test for HDFS-3626. This one creates files using
		/// a Path instantiated from a string object.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateNonCanonicalPathAndRestartFromString()
		{
			DoCreateTest(TestFileCreation.CreationMethod.PathFromString);
		}

		/// <summary>Another regression test for HDFS-3626.</summary>
		/// <remarks>
		/// Another regression test for HDFS-3626. This one creates files using
		/// a Path instantiated from a URI object.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateNonCanonicalPathAndRestartFromUri()
		{
			DoCreateTest(TestFileCreation.CreationMethod.PathFromUri);
		}

		private enum CreationMethod
		{
			DirectNnRpc,
			PathFromUri,
			PathFromString
		}

		/// <exception cref="System.Exception"/>
		private void DoCreateTest(TestFileCreation.CreationMethod method)
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				NamenodeProtocols nnrpc = cluster.GetNameNodeRpc();
				foreach (string pathStr in NonCanonicalPaths)
				{
					System.Console.Out.WriteLine("Creating " + pathStr + " by " + method);
					switch (method)
					{
						case TestFileCreation.CreationMethod.DirectNnRpc:
						{
							try
							{
								nnrpc.Create(pathStr, new FsPermission((short)0x1ed), "client", new EnumSetWritable
									<CreateFlag>(EnumSet.Of(CreateFlag.Create)), true, (short)1, 128 * 1024 * 1024L, 
									null);
								NUnit.Framework.Assert.Fail("Should have thrown exception when creating '" + pathStr
									 + "'" + " by " + method);
							}
							catch (InvalidPathException)
							{
							}
							// When we create by direct NN RPC, the NN just rejects the
							// non-canonical paths, rather than trying to normalize them.
							// So, we expect all of them to fail. 
							break;
						}

						case TestFileCreation.CreationMethod.PathFromUri:
						case TestFileCreation.CreationMethod.PathFromString:
						{
							// Unlike the above direct-to-NN case, we expect these to succeed,
							// since the Path constructor should normalize the path.
							Path p;
							if (method == TestFileCreation.CreationMethod.PathFromUri)
							{
								p = new Path(new URI(fs.GetUri() + pathStr));
							}
							else
							{
								p = new Path(fs.GetUri() + pathStr);
							}
							FSDataOutputStream stm = fs.Create(p);
							IOUtils.CloseStream(stm);
							break;
						}

						default:
						{
							throw new Exception("bad method: " + method);
						}
					}
				}
				cluster.RestartNameNode();
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test complete(..) - verifies that the fileId in the request
		/// matches that of the Inode.
		/// </summary>
		/// <remarks>
		/// Test complete(..) - verifies that the fileId in the request
		/// matches that of the Inode.
		/// This test checks that FileNotFoundException exception is thrown in case
		/// the fileId does not match.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileIdMismatch()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			DistributedFileSystem dfs = null;
			try
			{
				cluster.WaitActive();
				dfs = cluster.GetFileSystem();
				DFSClient client = dfs.dfs;
				Path f = new Path("/testFileIdMismatch.txt");
				CreateFile(dfs, f, 3);
				long someOtherFileId = -1;
				try
				{
					cluster.GetNameNodeRpc().Complete(f.ToString(), client.clientName, null, someOtherFileId
						);
					NUnit.Framework.Assert.Fail();
				}
				catch (LeaseExpiredException e)
				{
					FileSystem.Log.Info("Caught Expected LeaseExpiredException: ", e);
				}
			}
			finally
			{
				IOUtils.CloseStream(dfs);
				cluster.Shutdown();
			}
		}

		/// <summary>1.</summary>
		/// <remarks>
		/// 1. Check the blocks of old file are cleaned after creating with overwrite
		/// 2. Restart NN, check the file
		/// 3. Save new checkpoint and restart NN, check the file
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestFileCreationWithOverwrite()
		{
			Configuration conf = new Configuration();
			conf.SetInt("dfs.blocksize", blockSize);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			DistributedFileSystem dfs = cluster.GetFileSystem();
			try
			{
				dfs.Mkdirs(new Path("/foo/dir"));
				string file = "/foo/dir/file";
				Path filePath = new Path(file);
				// Case 1: Create file with overwrite, check the blocks of old file
				// are cleaned after creating with overwrite
				NameNode nn = cluster.GetNameNode();
				FSNamesystem fsn = NameNodeAdapter.GetNamesystem(nn);
				BlockManager bm = fsn.GetBlockManager();
				FSDataOutputStream @out = dfs.Create(filePath);
				byte[] oldData = AppendTestUtil.RandomBytes(seed, fileSize);
				try
				{
					@out.Write(oldData);
				}
				finally
				{
					@out.Close();
				}
				LocatedBlocks oldBlocks = NameNodeAdapter.GetBlockLocations(nn, file, 0, fileSize
					);
				AssertBlocks(bm, oldBlocks, true);
				@out = dfs.Create(filePath, true);
				byte[] newData = AppendTestUtil.RandomBytes(seed, fileSize);
				try
				{
					@out.Write(newData);
				}
				finally
				{
					@out.Close();
				}
				dfs.DeleteOnExit(filePath);
				LocatedBlocks newBlocks = NameNodeAdapter.GetBlockLocations(nn, file, 0, fileSize
					);
				AssertBlocks(bm, newBlocks, true);
				AssertBlocks(bm, oldBlocks, false);
				FSDataInputStream @in = dfs.Open(filePath);
				byte[] result = null;
				try
				{
					result = ReadAll(@in);
				}
				finally
				{
					@in.Close();
				}
				Assert.AssertArrayEquals(newData, result);
				// Case 2: Restart NN, check the file
				cluster.RestartNameNode();
				nn = cluster.GetNameNode();
				@in = dfs.Open(filePath);
				try
				{
					result = ReadAll(@in);
				}
				finally
				{
					@in.Close();
				}
				Assert.AssertArrayEquals(newData, result);
				// Case 3: Save new checkpoint and restart NN, check the file
				NameNodeAdapter.EnterSafeMode(nn, false);
				NameNodeAdapter.SaveNamespace(nn);
				cluster.RestartNameNode();
				nn = cluster.GetNameNode();
				@in = dfs.Open(filePath);
				try
				{
					result = ReadAll(@in);
				}
				finally
				{
					@in.Close();
				}
				Assert.AssertArrayEquals(newData, result);
			}
			finally
			{
				if (dfs != null)
				{
					dfs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private void AssertBlocks(BlockManager bm, LocatedBlocks lbs, bool exist)
		{
			foreach (LocatedBlock locatedBlock in lbs.GetLocatedBlocks())
			{
				if (exist)
				{
					NUnit.Framework.Assert.IsTrue(bm.GetStoredBlock(locatedBlock.GetBlock().GetLocalBlock
						()) != null);
				}
				else
				{
					NUnit.Framework.Assert.IsTrue(bm.GetStoredBlock(locatedBlock.GetBlock().GetLocalBlock
						()) == null);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private byte[] ReadAll(FSDataInputStream @in)
		{
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			int n = 0;
			while ((n = @in.Read(buffer)) > -1)
			{
				@out.Write(buffer, 0, n);
			}
			return @out.ToByteArray();
		}

		public TestFileCreation()
		{
			{
				((Log4JLogger)LeaseManager.Log).GetLogger().SetLevel(Level.All);
				((Log4JLogger)LogFactory.GetLog(typeof(FSNamesystem))).GetLogger().SetLevel(Level
					.All);
				((Log4JLogger)DFSClient.Log).GetLogger().SetLevel(Level.All);
			}
		}
	}
}
