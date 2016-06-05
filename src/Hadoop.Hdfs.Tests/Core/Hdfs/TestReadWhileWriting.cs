using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Test reading from hdfs while a file is being written.</summary>
	public class TestReadWhileWriting
	{
		private static readonly string Dir = "/" + typeof(TestReadWhileWriting).Name + "/";

		private const int BlockSize = 8192;

		private const long SoftLeaseLimit = 500;

		private const long HardLeaseLimit = 1000 * 600;

		// soft limit is short and hard limit is long, to test that
		// another thread can lease file after soft limit expired
		/// <summary>Test reading while writing.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_02_03()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			// create cluster
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
			try
			{
				//change the lease limits.
				cluster.SetLeasePeriod(SoftLeaseLimit, HardLeaseLimit);
				//wait for the cluster
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				Path p = new Path(Dir, "file1");
				int half = BlockSize / 2;
				{
					//a. On Machine M1, Create file. Write half block of data.
					//   Invoke DFSOutputStream.hflush() on the dfs file handle.
					//   Do not close file yet.
					FSDataOutputStream @out = fs.Create(p, true, fs.GetConf().GetInt(CommonConfigurationKeys
						.IoFileBufferSizeKey, 4096), (short)3, BlockSize);
					Write(@out, 0, half);
					//hflush
					((DFSOutputStream)@out.GetWrappedStream()).Hflush();
				}
				//b. On another machine M2, open file and verify that the half-block
				//   of data can be read successfully.
				CheckFile(p, half, conf);
				AppendTestUtil.Log.Info("leasechecker.interruptAndJoin()");
				((DistributedFileSystem)fs).dfs.GetLeaseRenewer().InterruptAndJoin();
				{
					//c. On M1, append another half block of data.  Close file on M1.
					//sleep to let the lease is expired.
					Sharpen.Thread.Sleep(2 * SoftLeaseLimit);
					UserGroupInformation current = UserGroupInformation.GetCurrentUser();
					UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting(current.GetShortUserName
						() + "x", new string[] { "supergroup" });
					DistributedFileSystem dfs = ugi.DoAs(new _PrivilegedExceptionAction_102(conf));
					FSDataOutputStream @out = Append(dfs, p);
					Write(@out, 0, half);
					@out.Close();
				}
				//d. On M2, open file and read 1 block of data from it. Close file.
				CheckFile(p, 2 * half, conf);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _PrivilegedExceptionAction_102 : PrivilegedExceptionAction<DistributedFileSystem
			>
		{
			public _PrivilegedExceptionAction_102(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public DistributedFileSystem Run()
			{
				return (DistributedFileSystem)FileSystem.NewInstance(conf);
			}

			private readonly Configuration conf;
		}

		/// <summary>Try openning a file for append.</summary>
		/// <exception cref="System.Exception"/>
		private static FSDataOutputStream Append(FileSystem fs, Path p)
		{
			for (int i = 0; i < 10; i++)
			{
				try
				{
					return fs.Append(p);
				}
				catch (RemoteException re)
				{
					if (re.GetClassName().Equals(typeof(RecoveryInProgressException).FullName))
					{
						AppendTestUtil.Log.Info("Will sleep and retry, i=" + i + ", p=" + p, re);
						Sharpen.Thread.Sleep(1000);
					}
					else
					{
						throw;
					}
				}
			}
			throw new IOException("Cannot append to " + p);
		}

		private static int userCount = 0;

		//check the file
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal static void CheckFile(Path p, int expectedsize, Configuration conf)
		{
			//open the file with another user account
			string username = UserGroupInformation.GetCurrentUser().GetShortUserName() + "_" 
				+ ++userCount;
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting(username, new 
				string[] { "supergroup" });
			FileSystem fs = DFSTestUtil.GetFileSystemAs(ugi, conf);
			HdfsDataInputStream @in = (HdfsDataInputStream)fs.Open(p);
			//Check visible length
			NUnit.Framework.Assert.IsTrue(@in.GetVisibleLength() >= expectedsize);
			//Able to read?
			for (int i = 0; i < expectedsize; i++)
			{
				NUnit.Framework.Assert.AreEqual(unchecked((byte)i), unchecked((byte)@in.Read()));
			}
			@in.Close();
		}

		/// <summary>Write something to a file</summary>
		/// <exception cref="System.IO.IOException"/>
		private static void Write(OutputStream @out, int offset, int length)
		{
			byte[] bytes = new byte[length];
			for (int i = 0; i < length; i++)
			{
				bytes[i] = unchecked((byte)(offset + i));
			}
			@out.Write(bytes);
		}

		public TestReadWhileWriting()
		{
			{
				((Log4JLogger)LogFactory.GetLog(typeof(FSNamesystem))).GetLogger().SetLevel(Level
					.All);
				((Log4JLogger)DFSClient.Log).GetLogger().SetLevel(Level.All);
			}
		}
	}
}
