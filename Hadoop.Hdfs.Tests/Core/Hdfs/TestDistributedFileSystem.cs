using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestDistributedFileSystem
	{
		private static readonly Random Ran = new Random();

		private bool dualPortTesting = false;

		private bool noXmlDefaults = false;

		private HdfsConfiguration GetTestConfiguration()
		{
			HdfsConfiguration conf;
			if (noXmlDefaults)
			{
				conf = new HdfsConfiguration(false);
				string namenodeDir = new FilePath(MiniDFSCluster.GetBaseDirectory(), "name").GetAbsolutePath
					();
				conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, namenodeDir);
				conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, namenodeDir);
			}
			else
			{
				conf = new HdfsConfiguration();
			}
			if (dualPortTesting)
			{
				conf.Set(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, "localhost:0");
			}
			conf.SetLong(DFSConfigKeys.DfsNamenodeMinBlockSizeKey, 0);
			return conf;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEmptyDelegationToken()
		{
			Configuration conf = GetTestConfiguration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				FileSystem fileSys = cluster.GetFileSystem();
				fileSys.GetDelegationToken(string.Empty);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFileSystemCloseAll()
		{
			Configuration conf = GetTestConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
			URI address = FileSystem.GetDefaultUri(conf);
			try
			{
				FileSystem.CloseAll();
				conf = GetTestConfiguration();
				FileSystem.SetDefaultUri(conf, address);
				FileSystem.Get(conf);
				FileSystem.Get(conf);
				FileSystem.CloseAll();
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
		/// Tests DFSClient.close throws no ConcurrentModificationException if
		/// multiple files are open.
		/// </summary>
		/// <remarks>
		/// Tests DFSClient.close throws no ConcurrentModificationException if
		/// multiple files are open.
		/// Also tests that any cached sockets are closed. (HDFS-3359)
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDFSClose()
		{
			Configuration conf = GetTestConfiguration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				FileSystem fileSys = cluster.GetFileSystem();
				// create two files, leaving them open
				fileSys.Create(new Path("/test/dfsclose/file-0"));
				fileSys.Create(new Path("/test/dfsclose/file-1"));
				// create another file, close it, and read it, so
				// the client gets a socket in its SocketCache
				Path p = new Path("/non-empty-file");
				DFSTestUtil.CreateFile(fileSys, p, 1L, (short)1, 0L);
				DFSTestUtil.ReadFile(fileSys, p);
				fileSys.Close();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDFSCloseOrdering()
		{
			DistributedFileSystem fs = new TestDistributedFileSystem.MyDistributedFileSystem(
				);
			Path path = new Path("/a");
			fs.DeleteOnExit(path);
			fs.Close();
			InOrder inOrder = Org.Mockito.Mockito.InOrder(fs.dfs);
			inOrder.Verify(fs.dfs).CloseOutputStreams(Matchers.Eq(false));
			inOrder.Verify(fs.dfs).Delete(Matchers.Eq(path.ToString()), Matchers.Eq(true));
			inOrder.Verify(fs.dfs).Close();
		}

		private class MyDistributedFileSystem : DistributedFileSystem
		{
			internal MyDistributedFileSystem()
			{
				statistics = new FileSystem.Statistics("myhdfs");
				// can't mock finals
				dfs = Org.Mockito.Mockito.Mock<DFSClient>();
			}

			public override bool Exists(Path p)
			{
				return true;
			}
			// trick out deleteOnExit
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDFSSeekExceptions()
		{
			Configuration conf = GetTestConfiguration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				FileSystem fileSys = cluster.GetFileSystem();
				string file = "/test/fileclosethenseek/file-0";
				Path path = new Path(file);
				// create file
				FSDataOutputStream output = fileSys.Create(path);
				output.WriteBytes("Some test data to write longer than 10 bytes");
				output.Close();
				FSDataInputStream input = fileSys.Open(path);
				input.Seek(10);
				bool threw = false;
				try
				{
					input.Seek(100);
				}
				catch (IOException)
				{
					// success
					threw = true;
				}
				NUnit.Framework.Assert.IsTrue("Failed to throw IOE when seeking past end", threw);
				input.Close();
				threw = false;
				try
				{
					input.Seek(1);
				}
				catch (IOException)
				{
					//success
					threw = true;
				}
				NUnit.Framework.Assert.IsTrue("Failed to throw IOE when seeking after close", threw
					);
				fileSys.Close();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDFSClient()
		{
			Configuration conf = GetTestConfiguration();
			long grace = 1000L;
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				string filepathstring = "/test/LeaseChecker/foo";
				Path[] filepaths = new Path[4];
				for (int i = 0; i < filepaths.Length; i++)
				{
					filepaths[i] = new Path(filepathstring + i);
				}
				long millis = Time.Now();
				{
					DistributedFileSystem dfs = cluster.GetFileSystem();
					dfs.dfs.GetLeaseRenewer().SetGraceSleepPeriod(grace);
					NUnit.Framework.Assert.IsFalse(dfs.dfs.GetLeaseRenewer().IsRunning());
					{
						//create a file
						FSDataOutputStream @out = dfs.Create(filepaths[0]);
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
						//write something
						@out.WriteLong(millis);
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
						//close
						@out.Close();
						Sharpen.Thread.Sleep(grace / 4 * 3);
						//within grace period
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
						for (int i_1 = 0; i_1 < 3; i_1++)
						{
							if (dfs.dfs.GetLeaseRenewer().IsRunning())
							{
								Sharpen.Thread.Sleep(grace / 2);
							}
						}
						//passed grace period
						NUnit.Framework.Assert.IsFalse(dfs.dfs.GetLeaseRenewer().IsRunning());
					}
					{
						//create file1
						FSDataOutputStream out1 = dfs.Create(filepaths[1]);
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
						//create file2
						FSDataOutputStream out2 = dfs.Create(filepaths[2]);
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
						//write something to file1
						out1.WriteLong(millis);
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
						//close file1
						out1.Close();
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
						//write something to file2
						out2.WriteLong(millis);
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
						//close file2
						out2.Close();
						Sharpen.Thread.Sleep(grace / 4 * 3);
						//within grace period
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
					}
					{
						//create file3
						FSDataOutputStream out3 = dfs.Create(filepaths[3]);
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
						Sharpen.Thread.Sleep(grace / 4 * 3);
						//passed previous grace period, should still running
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
						//write something to file3
						out3.WriteLong(millis);
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
						//close file3
						out3.Close();
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
						Sharpen.Thread.Sleep(grace / 4 * 3);
						//within grace period
						NUnit.Framework.Assert.IsTrue(dfs.dfs.GetLeaseRenewer().IsRunning());
						for (int i_1 = 0; i_1 < 3; i_1++)
						{
							if (dfs.dfs.GetLeaseRenewer().IsRunning())
							{
								Sharpen.Thread.Sleep(grace / 2);
							}
						}
						//passed grace period
						NUnit.Framework.Assert.IsFalse(dfs.dfs.GetLeaseRenewer().IsRunning());
					}
					dfs.Close();
				}
				{
					// Check to see if opening a non-existent file triggers a FNF
					FileSystem fs = cluster.GetFileSystem();
					Path dir = new Path("/wrwelkj");
					NUnit.Framework.Assert.IsFalse("File should not exist for test.", fs.Exists(dir));
					try
					{
						FSDataInputStream @in = fs.Open(dir);
						try
						{
							@in.Close();
							fs.Close();
						}
						finally
						{
							NUnit.Framework.Assert.IsTrue("Did not get a FileNotFoundException for non-existing"
								 + " file.", false);
						}
					}
					catch (FileNotFoundException)
					{
					}
				}
				{
					// This is the proper exception to catch; move on.
					DistributedFileSystem dfs = cluster.GetFileSystem();
					NUnit.Framework.Assert.IsFalse(dfs.dfs.GetLeaseRenewer().IsRunning());
					//open and check the file
					FSDataInputStream @in = dfs.Open(filepaths[0]);
					NUnit.Framework.Assert.IsFalse(dfs.dfs.GetLeaseRenewer().IsRunning());
					NUnit.Framework.Assert.AreEqual(millis, @in.ReadLong());
					NUnit.Framework.Assert.IsFalse(dfs.dfs.GetLeaseRenewer().IsRunning());
					@in.Close();
					NUnit.Framework.Assert.IsFalse(dfs.dfs.GetLeaseRenewer().IsRunning());
					dfs.Close();
				}
				{
					// test accessing DFS with ip address. should work with any hostname
					// alias or ip address that points to the interface that NameNode
					// is listening on. In this case, it is localhost.
					string uri = "hdfs://127.0.0.1:" + cluster.GetNameNodePort() + "/test/ipAddress/file";
					Path path = new Path(uri);
					FileSystem fs = FileSystem.Get(path.ToUri(), conf);
					FSDataOutputStream @out = fs.Create(path);
					byte[] buf = new byte[1024];
					@out.Write(buf);
					@out.Close();
					FSDataInputStream @in = fs.Open(path);
					@in.ReadFully(buf);
					@in.Close();
					fs.Close();
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

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStatistics()
		{
			int lsLimit = 2;
			Configuration conf = GetTestConfiguration();
			conf.SetInt(DFSConfigKeys.DfsListLimit, lsLimit);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				Path dir = new Path("/test");
				Path file = new Path(dir, "file");
				int readOps = DFSTestUtil.GetStatistics(fs).GetReadOps();
				int writeOps = DFSTestUtil.GetStatistics(fs).GetWriteOps();
				int largeReadOps = DFSTestUtil.GetStatistics(fs).GetLargeReadOps();
				fs.Mkdirs(dir);
				CheckStatistics(fs, readOps, ++writeOps, largeReadOps);
				FSDataOutputStream @out = fs.Create(file, (short)1);
				@out.Close();
				CheckStatistics(fs, readOps, ++writeOps, largeReadOps);
				FileStatus status = fs.GetFileStatus(file);
				CheckStatistics(fs, ++readOps, writeOps, largeReadOps);
				fs.GetFileBlockLocations(file, 0, 0);
				CheckStatistics(fs, ++readOps, writeOps, largeReadOps);
				fs.GetFileBlockLocations(status, 0, 0);
				CheckStatistics(fs, ++readOps, writeOps, largeReadOps);
				FSDataInputStream @in = fs.Open(file);
				@in.Close();
				CheckStatistics(fs, ++readOps, writeOps, largeReadOps);
				fs.SetReplication(file, (short)2);
				CheckStatistics(fs, readOps, ++writeOps, largeReadOps);
				Path file1 = new Path(dir, "file1");
				fs.Rename(file, file1);
				CheckStatistics(fs, readOps, ++writeOps, largeReadOps);
				fs.GetContentSummary(file1);
				CheckStatistics(fs, ++readOps, writeOps, largeReadOps);
				// Iterative ls test
				for (int i = 0; i < 10; i++)
				{
					Path p = new Path(dir, Sharpen.Extensions.ToString(i));
					fs.Mkdirs(p);
					FileStatus[] list = fs.ListStatus(dir);
					if (list.Length > lsLimit)
					{
						// if large directory, then count readOps and largeReadOps by 
						// number times listStatus iterates
						int iterations = (int)Math.Ceil((double)list.Length / lsLimit);
						largeReadOps += iterations;
						readOps += iterations;
					}
					else
					{
						// Single iteration in listStatus - no large read operation done
						readOps++;
					}
					// writeOps incremented by 1 for mkdirs
					// readOps and largeReadOps incremented by 1 or more
					CheckStatistics(fs, readOps, ++writeOps, largeReadOps);
				}
				fs.GetStatus(file1);
				CheckStatistics(fs, ++readOps, writeOps, largeReadOps);
				fs.GetFileChecksum(file1);
				CheckStatistics(fs, ++readOps, writeOps, largeReadOps);
				fs.SetPermission(file1, new FsPermission((short)0x1ff));
				CheckStatistics(fs, readOps, ++writeOps, largeReadOps);
				fs.SetTimes(file1, 0L, 0L);
				CheckStatistics(fs, readOps, ++writeOps, largeReadOps);
				UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
				fs.SetOwner(file1, ugi.GetUserName(), ugi.GetGroupNames()[0]);
				CheckStatistics(fs, readOps, ++writeOps, largeReadOps);
				fs.Delete(dir, true);
				CheckStatistics(fs, readOps, ++writeOps, largeReadOps);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Checks statistics.</summary>
		/// <remarks>Checks statistics. -1 indicates do not check for the operations</remarks>
		private void CheckStatistics(FileSystem fs, int readOps, int writeOps, int largeReadOps
			)
		{
			NUnit.Framework.Assert.AreEqual(readOps, DFSTestUtil.GetStatistics(fs).GetReadOps
				());
			NUnit.Framework.Assert.AreEqual(writeOps, DFSTestUtil.GetStatistics(fs).GetWriteOps
				());
			NUnit.Framework.Assert.AreEqual(largeReadOps, DFSTestUtil.GetStatistics(fs).GetLargeReadOps
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFileChecksum()
		{
			((Log4JLogger)HftpFileSystem.Log).GetLogger().SetLevel(Level.All);
			long seed = Ran.NextLong();
			System.Console.Out.WriteLine("seed=" + seed);
			Ran.SetSeed(seed);
			Configuration conf = GetTestConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			FileSystem hdfs = cluster.GetFileSystem();
			string nnAddr = conf.Get(DFSConfigKeys.DfsNamenodeHttpAddressKey);
			UserGroupInformation current = UserGroupInformation.GetCurrentUser();
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting(current.GetShortUserName
				() + "x", new string[] { "user" });
			try
			{
				hdfs.GetFileChecksum(new Path("/test/TestNonExistingFile"));
				NUnit.Framework.Assert.Fail("Expecting FileNotFoundException");
			}
			catch (FileNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue("Not throwing the intended exception message", e.Message
					.Contains("File does not exist: /test/TestNonExistingFile"));
			}
			try
			{
				Path path = new Path("/test/TestExistingDir/");
				hdfs.Mkdirs(path);
				hdfs.GetFileChecksum(path);
				NUnit.Framework.Assert.Fail("Expecting FileNotFoundException");
			}
			catch (FileNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue("Not throwing the intended exception message", e.Message
					.Contains("Path is not a file: /test/TestExistingDir"));
			}
			//hftp
			string hftpuri = "hftp://" + nnAddr;
			System.Console.Out.WriteLine("hftpuri=" + hftpuri);
			FileSystem hftp = ugi.DoAs(new _PrivilegedExceptionAction_536(hftpuri, conf));
			//webhdfs
			string webhdfsuri = WebHdfsFileSystem.Scheme + "://" + nnAddr;
			System.Console.Out.WriteLine("webhdfsuri=" + webhdfsuri);
			FileSystem webhdfs = ugi.DoAs(new _PrivilegedExceptionAction_547(webhdfsuri, conf
				));
			Path dir = new Path("/filechecksum");
			int block_size = 1024;
			int buffer_size = conf.GetInt(CommonConfigurationKeys.IoFileBufferSizeKey, 4096);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, 512);
			//try different number of blocks
			for (int n = 0; n < 5; n++)
			{
				//generate random data
				byte[] data = new byte[Ran.Next(block_size / 2 - 1) + n * block_size + 1];
				Ran.NextBytes(data);
				System.Console.Out.WriteLine("data.length=" + data.Length);
				//write data to a file
				Path foo = new Path(dir, "foo" + n);
				{
					FSDataOutputStream @out = hdfs.Create(foo, false, buffer_size, (short)2, block_size
						);
					@out.Write(data);
					@out.Close();
				}
				//compute checksum
				FileChecksum hdfsfoocs = hdfs.GetFileChecksum(foo);
				System.Console.Out.WriteLine("hdfsfoocs=" + hdfsfoocs);
				//hftp
				FileChecksum hftpfoocs = hftp.GetFileChecksum(foo);
				System.Console.Out.WriteLine("hftpfoocs=" + hftpfoocs);
				Path qualified = new Path(hftpuri + dir, "foo" + n);
				FileChecksum qfoocs = hftp.GetFileChecksum(qualified);
				System.Console.Out.WriteLine("qfoocs=" + qfoocs);
				//webhdfs
				FileChecksum webhdfsfoocs = webhdfs.GetFileChecksum(foo);
				System.Console.Out.WriteLine("webhdfsfoocs=" + webhdfsfoocs);
				Path webhdfsqualified = new Path(webhdfsuri + dir, "foo" + n);
				FileChecksum webhdfs_qfoocs = webhdfs.GetFileChecksum(webhdfsqualified);
				System.Console.Out.WriteLine("webhdfs_qfoocs=" + webhdfs_qfoocs);
				//create a zero byte file
				Path zeroByteFile = new Path(dir, "zeroByteFile" + n);
				{
					FSDataOutputStream @out = hdfs.Create(zeroByteFile, false, buffer_size, (short)2, 
						block_size);
					@out.Close();
				}
				{
					// verify the magic val for zero byte files
					FileChecksum zeroChecksum = hdfs.GetFileChecksum(zeroByteFile);
					NUnit.Framework.Assert.AreEqual(zeroChecksum.ToString(), "MD5-of-0MD5-of-0CRC32:70bc8f4b72a86921468bf8e8441dce51"
						);
				}
				//write another file
				Path bar = new Path(dir, "bar" + n);
				{
					FSDataOutputStream @out = hdfs.Create(bar, false, buffer_size, (short)2, block_size
						);
					@out.Write(data);
					@out.Close();
				}
				{
					//verify checksum
					FileChecksum barcs = hdfs.GetFileChecksum(bar);
					int barhashcode = barcs.GetHashCode();
					NUnit.Framework.Assert.AreEqual(hdfsfoocs.GetHashCode(), barhashcode);
					NUnit.Framework.Assert.AreEqual(hdfsfoocs, barcs);
					//hftp
					NUnit.Framework.Assert.AreEqual(hftpfoocs.GetHashCode(), barhashcode);
					NUnit.Framework.Assert.AreEqual(hftpfoocs, barcs);
					NUnit.Framework.Assert.AreEqual(qfoocs.GetHashCode(), barhashcode);
					NUnit.Framework.Assert.AreEqual(qfoocs, barcs);
					//webhdfs
					NUnit.Framework.Assert.AreEqual(webhdfsfoocs.GetHashCode(), barhashcode);
					NUnit.Framework.Assert.AreEqual(webhdfsfoocs, barcs);
					NUnit.Framework.Assert.AreEqual(webhdfs_qfoocs.GetHashCode(), barhashcode);
					NUnit.Framework.Assert.AreEqual(webhdfs_qfoocs, barcs);
				}
				hdfs.SetPermission(dir, new FsPermission((short)0));
				{
					//test permission error on hftp 
					try
					{
						hftp.GetFileChecksum(qualified);
						NUnit.Framework.Assert.Fail();
					}
					catch (IOException ioe)
					{
						FileSystem.Log.Info("GOOD: getting an exception", ioe);
					}
				}
				{
					//test permission error on webhdfs 
					try
					{
						webhdfs.GetFileChecksum(webhdfsqualified);
						NUnit.Framework.Assert.Fail();
					}
					catch (IOException ioe)
					{
						FileSystem.Log.Info("GOOD: getting an exception", ioe);
					}
				}
				hdfs.SetPermission(dir, new FsPermission((short)0x1ff));
			}
			cluster.Shutdown();
		}

		private sealed class _PrivilegedExceptionAction_536 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_536(string hftpuri, Configuration conf)
			{
				this.hftpuri = hftpuri;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return new Path(hftpuri).GetFileSystem(conf);
			}

			private readonly string hftpuri;

			private readonly Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_547 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_547(string webhdfsuri, Configuration conf)
			{
				this.webhdfsuri = webhdfsuri;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return new Path(webhdfsuri).GetFileSystem(conf);
			}

			private readonly string webhdfsuri;

			private readonly Configuration conf;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllWithDualPort()
		{
			dualPortTesting = true;
			try
			{
				TestFileSystemCloseAll();
				TestDFSClose();
				TestDFSClient();
				TestFileChecksum();
			}
			finally
			{
				dualPortTesting = false;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllWithNoXmlDefaults()
		{
			// Do all the tests with a configuration that ignores the defaults in
			// the XML files.
			noXmlDefaults = true;
			try
			{
				TestFileSystemCloseAll();
				TestDFSClose();
				TestDFSClient();
				TestFileChecksum();
			}
			finally
			{
				noXmlDefaults = false;
			}
		}

		/// <summary>
		/// Tests the normal path of batching up BlockLocation[]s to be passed to a
		/// single
		/// <see cref="DistributedFileSystem.GetFileBlockStorageLocations(System.Collections.Generic.IList{E})
		/// 	"/>
		/// call
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetFileBlockStorageLocationsBatching()
		{
			Configuration conf = GetTestConfiguration();
			((Log4JLogger)ProtobufRpcEngine.Log).GetLogger().SetLevel(Level.Trace);
			((Log4JLogger)BlockStorageLocationUtil.Log).GetLogger().SetLevel(Level.Trace);
			((Log4JLogger)DFSClient.Log).GetLogger().SetLevel(Level.Trace);
			conf.SetBoolean(DFSConfigKeys.DfsHdfsBlocksMetadataEnabled, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			try
			{
				DistributedFileSystem fs = cluster.GetFileSystem();
				// Create two files
				Path tmpFile1 = new Path("/tmpfile1.dat");
				Path tmpFile2 = new Path("/tmpfile2.dat");
				DFSTestUtil.CreateFile(fs, tmpFile1, 1024, (short)2, unchecked((long)(0xDEADDEADl
					)));
				DFSTestUtil.CreateFile(fs, tmpFile2, 1024, (short)2, unchecked((long)(0xDEADDEADl
					)));
				// Make sure files are fully replicated before continuing
				GenericTestUtils.WaitFor(new _Supplier_719(fs, tmpFile1, tmpFile2), 500, 30000);
				// swallow
				// Get locations of blocks of both files and concat together
				BlockLocation[] blockLocs1 = fs.GetFileBlockLocations(tmpFile1, 0, 1024);
				BlockLocation[] blockLocs2 = fs.GetFileBlockLocations(tmpFile2, 0, 1024);
				BlockLocation[] blockLocs = (BlockLocation[])ArrayUtils.AddAll(blockLocs1, blockLocs2
					);
				// Fetch VolumeBlockLocations in batch
				BlockStorageLocation[] locs = fs.GetFileBlockStorageLocations(Arrays.AsList(blockLocs
					));
				int counter = 0;
				// Print out the list of ids received for each block
				foreach (BlockStorageLocation l in locs)
				{
					for (int i = 0; i < l.GetVolumeIds().Length; i++)
					{
						VolumeId id = l.GetVolumeIds()[i];
						string name = l.GetNames()[i];
						if (id != null)
						{
							System.Console.Out.WriteLine("Datanode " + name + " has block " + counter + " on volume id "
								 + id.ToString());
						}
					}
					counter++;
				}
				NUnit.Framework.Assert.AreEqual("Expected two HdfsBlockLocations for two 1-block files"
					, 2, locs.Length);
				foreach (BlockStorageLocation l_1 in locs)
				{
					NUnit.Framework.Assert.AreEqual("Expected two replicas for each block", 2, l_1.GetVolumeIds
						().Length);
					for (int i = 0; i < l_1.GetVolumeIds().Length; i++)
					{
						VolumeId id = l_1.GetVolumeIds()[i];
						string name = l_1.GetNames()[i];
						NUnit.Framework.Assert.IsTrue("Expected block to be valid on datanode " + name, id
							 != null);
					}
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _Supplier_719 : Supplier<bool>
		{
			public _Supplier_719(DistributedFileSystem fs, Path tmpFile1, Path tmpFile2)
			{
				this.fs = fs;
				this.tmpFile1 = tmpFile1;
				this.tmpFile2 = tmpFile2;
			}

			public bool Get()
			{
				try
				{
					IList<BlockLocation> list = Lists.NewArrayList();
					Sharpen.Collections.AddAll(list, Arrays.AsList(fs.GetFileBlockLocations(tmpFile1, 
						0, 1024)));
					Sharpen.Collections.AddAll(list, Arrays.AsList(fs.GetFileBlockLocations(tmpFile2, 
						0, 1024)));
					int totalRepl = 0;
					foreach (BlockLocation loc in list)
					{
						totalRepl += loc.GetHosts().Length;
					}
					if (totalRepl == 4)
					{
						return true;
					}
				}
				catch (IOException)
				{
				}
				return false;
			}

			private readonly DistributedFileSystem fs;

			private readonly Path tmpFile1;

			private readonly Path tmpFile2;
		}

		/// <summary>
		/// Tests error paths for
		/// <see cref="DistributedFileSystem.GetFileBlockStorageLocations(System.Collections.Generic.IList{E})
		/// 	"/>
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetFileBlockStorageLocationsError()
		{
			Configuration conf = GetTestConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsHdfsBlocksMetadataEnabled, true);
			conf.SetInt(DFSConfigKeys.DfsClientFileBlockStorageLocationsTimeoutMs, 1500);
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesKey, 0);
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				cluster.GetDataNodes();
				DistributedFileSystem fs = cluster.GetFileSystem();
				// Create a few files and add together their block locations into
				// a list.
				Path tmpFile1 = new Path("/errorfile1.dat");
				Path tmpFile2 = new Path("/errorfile2.dat");
				DFSTestUtil.CreateFile(fs, tmpFile1, 1024, (short)2, unchecked((long)(0xDEADDEADl
					)));
				DFSTestUtil.CreateFile(fs, tmpFile2, 1024, (short)2, unchecked((long)(0xDEADDEADl
					)));
				// Make sure files are fully replicated before continuing
				GenericTestUtils.WaitFor(new _Supplier_808(fs, tmpFile1, tmpFile2), 500, 30000);
				// swallow
				BlockLocation[] blockLocs1 = fs.GetFileBlockLocations(tmpFile1, 0, 1024);
				BlockLocation[] blockLocs2 = fs.GetFileBlockLocations(tmpFile2, 0, 1024);
				IList<BlockLocation> allLocs = Lists.NewArrayList();
				Sharpen.Collections.AddAll(allLocs, Arrays.AsList(blockLocs1));
				Sharpen.Collections.AddAll(allLocs, Arrays.AsList(blockLocs2));
				// Stall on the DN to test the timeout
				DataNodeFaultInjector injector = Org.Mockito.Mockito.Mock<DataNodeFaultInjector>(
					);
				Org.Mockito.Mockito.DoAnswer(new _Answer_840()).When(injector).GetHdfsBlocksMetadata
					();
				DataNodeFaultInjector.instance = injector;
				BlockStorageLocation[] locs = fs.GetFileBlockStorageLocations(allLocs);
				foreach (BlockStorageLocation loc in locs)
				{
					NUnit.Framework.Assert.AreEqual("Found more than 0 cached hosts although RPCs supposedly timed out"
						, 0, loc.GetCachedHosts().Length);
				}
				// Restore a default injector
				DataNodeFaultInjector.instance = new DataNodeFaultInjector();
				// Stop a datanode to simulate a failure.
				MiniDFSCluster.DataNodeProperties stoppedNode = cluster.StopDataNode(0);
				// Fetch VolumeBlockLocations
				locs = fs.GetFileBlockStorageLocations(allLocs);
				NUnit.Framework.Assert.AreEqual("Expected two HdfsBlockLocation for two 1-block files"
					, 2, locs.Length);
				foreach (BlockStorageLocation l in locs)
				{
					NUnit.Framework.Assert.AreEqual("Expected two replicas for each block", 2, l.GetHosts
						().Length);
					NUnit.Framework.Assert.AreEqual("Expected two VolumeIDs for each block", 2, l.GetVolumeIds
						().Length);
					NUnit.Framework.Assert.IsTrue("Expected one valid and one invalid volume", (l.GetVolumeIds
						()[0] == null) ^ (l.GetVolumeIds()[1] == null));
				}
				// Start the datanode again, and remove one of the blocks.
				// This is a different type of failure where the block itself
				// is invalid.
				cluster.RestartDataNode(stoppedNode, true);
				/*keepPort*/
				cluster.WaitActive();
				fs.Delete(tmpFile2, true);
				HATestUtil.WaitForNNToIssueDeletions(cluster.GetNameNode());
				cluster.TriggerHeartbeats();
				HATestUtil.WaitForDNDeletions(cluster);
				locs = fs.GetFileBlockStorageLocations(allLocs);
				NUnit.Framework.Assert.AreEqual("Expected two HdfsBlockLocations for two 1-block files"
					, 2, locs.Length);
				NUnit.Framework.Assert.IsNotNull(locs[0].GetVolumeIds()[0]);
				NUnit.Framework.Assert.IsNotNull(locs[0].GetVolumeIds()[1]);
				NUnit.Framework.Assert.IsNull(locs[1].GetVolumeIds()[0]);
				NUnit.Framework.Assert.IsNull(locs[1].GetVolumeIds()[1]);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private sealed class _Supplier_808 : Supplier<bool>
		{
			public _Supplier_808(DistributedFileSystem fs, Path tmpFile1, Path tmpFile2)
			{
				this.fs = fs;
				this.tmpFile1 = tmpFile1;
				this.tmpFile2 = tmpFile2;
			}

			public bool Get()
			{
				try
				{
					IList<BlockLocation> list = Lists.NewArrayList();
					Sharpen.Collections.AddAll(list, Arrays.AsList(fs.GetFileBlockLocations(tmpFile1, 
						0, 1024)));
					Sharpen.Collections.AddAll(list, Arrays.AsList(fs.GetFileBlockLocations(tmpFile2, 
						0, 1024)));
					int totalRepl = 0;
					foreach (BlockLocation loc in list)
					{
						totalRepl += loc.GetHosts().Length;
					}
					if (totalRepl == 4)
					{
						return true;
					}
				}
				catch (IOException)
				{
				}
				return false;
			}

			private readonly DistributedFileSystem fs;

			private readonly Path tmpFile1;

			private readonly Path tmpFile2;
		}

		private sealed class _Answer_840 : Answer<Void>
		{
			public _Answer_840()
			{
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				Sharpen.Thread.Sleep(3000);
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateWithCustomChecksum()
		{
			Configuration conf = GetTestConfiguration();
			MiniDFSCluster cluster = null;
			Path testBasePath = new Path("/test/csum");
			// create args 
			Path path1 = new Path(testBasePath, "file_wtih_crc1");
			Path path2 = new Path(testBasePath, "file_with_crc2");
			Options.ChecksumOpt opt1 = new Options.ChecksumOpt(DataChecksum.Type.Crc32c, 512);
			Options.ChecksumOpt opt2 = new Options.ChecksumOpt(DataChecksum.Type.Crc32, 512);
			// common args
			FsPermission perm = FsPermission.GetDefault().ApplyUMask(FsPermission.GetUMask(conf
				));
			EnumSet<CreateFlag> flags = EnumSet.Of(CreateFlag.Overwrite, CreateFlag.Create);
			short repl = 1;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				FileSystem dfs = cluster.GetFileSystem();
				dfs.Mkdirs(testBasePath);
				// create two files with different checksum types
				FSDataOutputStream out1 = dfs.Create(path1, perm, flags, 4096, repl, 131072L, null
					, opt1);
				FSDataOutputStream out2 = dfs.Create(path2, perm, flags, 4096, repl, 131072L, null
					, opt2);
				for (int i = 0; i < 1024; i++)
				{
					out1.Write(i);
					out2.Write(i);
				}
				out1.Close();
				out2.Close();
				// the two checksums must be different.
				MD5MD5CRC32FileChecksum sum1 = (MD5MD5CRC32FileChecksum)dfs.GetFileChecksum(path1
					);
				MD5MD5CRC32FileChecksum sum2 = (MD5MD5CRC32FileChecksum)dfs.GetFileChecksum(path2
					);
				NUnit.Framework.Assert.IsFalse(sum1.Equals(sum2));
				// check the individual params
				NUnit.Framework.Assert.AreEqual(DataChecksum.Type.Crc32c, sum1.GetCrcType());
				NUnit.Framework.Assert.AreEqual(DataChecksum.Type.Crc32, sum2.GetCrcType());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.GetFileSystem().Delete(testBasePath, true);
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFileCloseStatus()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			DistributedFileSystem fs = cluster.GetFileSystem();
			try
			{
				// create a new file.
				Path file = new Path("/simpleFlush.dat");
				FSDataOutputStream output = fs.Create(file);
				// write to file
				output.WriteBytes("Some test data");
				output.Flush();
				NUnit.Framework.Assert.IsFalse("File status should be open", fs.IsFileClosed(file
					));
				output.Close();
				NUnit.Framework.Assert.IsTrue("File status should be closed", fs.IsFileClosed(file
					));
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
		public virtual void TestListFiles()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				DistributedFileSystem fs = cluster.GetFileSystem();
				Path relative = new Path("relative");
				fs.Create(new Path(relative, "foo")).Close();
				IList<LocatedFileStatus> retVal = new AList<LocatedFileStatus>();
				RemoteIterator<LocatedFileStatus> iter = fs.ListFiles(relative, true);
				while (iter.HasNext())
				{
					retVal.AddItem(iter.Next());
				}
				System.Console.Out.WriteLine("retVal = " + retVal);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDFSClientPeerTimeout()
		{
			int timeout = 1000;
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, timeout);
			// only need cluster to create a dfs client to get a peer
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				// use a dummy socket to ensure the read timesout
				Socket socket = Sharpen.Extensions.CreateServerSocket(0);
				Peer peer = dfs.GetClient().NewConnectedPeer((IPEndPoint)socket.LocalEndPoint, null
					, null);
				long start = Time.Now();
				try
				{
					peer.GetInputStream().Read();
					NUnit.Framework.Assert.Fail("should timeout");
				}
				catch (SocketTimeoutException)
				{
					long delta = Time.Now() - start;
					NUnit.Framework.Assert.IsTrue("timedout too soon", delta >= timeout * 0.9);
					NUnit.Framework.Assert.IsTrue("timedout too late", delta <= timeout * 1.1);
				}
				catch (Exception t)
				{
					NUnit.Framework.Assert.Fail("wrong exception:" + t);
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetServerDefaults()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				FsServerDefaults fsServerDefaults = dfs.GetServerDefaults();
				NUnit.Framework.Assert.IsNotNull(fsServerDefaults);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		public TestDistributedFileSystem()
		{
			{
				((Log4JLogger)DFSClient.Log).GetLogger().SetLevel(Level.All);
			}
		}
	}
}
