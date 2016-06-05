using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestCachingStrategy
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestCachingStrategy));

		private const int MaxTestFileLen = 1024 * 1024;

		private const int WritePacketSize = DFSConfigKeys.DfsClientWritePacketSizeDefault;

		private static readonly TestCachingStrategy.TestRecordingCacheTracker tracker = new 
			TestCachingStrategy.TestRecordingCacheTracker();

		[BeforeClass]
		public static void SetupTest()
		{
			EditLogFileOutputStream.SetShouldSkipFsyncForTesting(true);
			// Track calls to posix_fadvise.
			NativeIO.POSIX.SetCacheManipulator(tracker);
			// Normally, we wait for a few megabytes of data to be read or written 
			// before dropping the cache.  This is to avoid an excessive number of
			// JNI calls to the posix_fadvise function.  However, for the purpose
			// of this test, we want to use small files and see all fadvise calls
			// happen.
			BlockSender.CacheDropIntervalBytes = 4096;
			BlockReceiver.CacheDropLagBytes = 4096;
		}

		private class Stats
		{
			private readonly string fileName;

			private readonly bool[] dropped = new bool[MaxTestFileLen];

			internal Stats(string fileName)
			{
				this.fileName = fileName;
			}

			internal virtual void Fadvise(int offset, int len, int flags)
			{
				lock (this)
				{
					Log.Debug("got fadvise(offset=" + offset + ", len=" + len + ",flags=" + flags + ")"
						);
					if (flags == NativeIO.POSIX.PosixFadvDontneed)
					{
						for (int i = 0; i < len; i++)
						{
							dropped[(offset + i)] = true;
						}
					}
				}
			}

			internal virtual void AssertNotDroppedInRange(int start, int end)
			{
				lock (this)
				{
					for (int i = start; i < end; i++)
					{
						if (dropped[i])
						{
							throw new RuntimeException("in file " + fileName + ", we " + "dropped the cache at offset "
								 + i);
						}
					}
				}
			}

			internal virtual void AssertDroppedInRange(int start, int end)
			{
				lock (this)
				{
					for (int i = start; i < end; i++)
					{
						if (!dropped[i])
						{
							throw new RuntimeException("in file " + fileName + ", we " + "did not drop the cache at offset "
								 + i);
						}
					}
				}
			}

			internal virtual void Clear()
			{
				lock (this)
				{
					Arrays.Fill(dropped, false);
				}
			}
		}

		private class TestRecordingCacheTracker : NativeIO.POSIX.CacheManipulator
		{
			private readonly IDictionary<string, TestCachingStrategy.Stats> map = new SortedDictionary
				<string, TestCachingStrategy.Stats>();

			/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
			public override void PosixFadviseIfPossible(string name, FileDescriptor fd, long 
				offset, long len, int flags)
			{
				if ((len < 0) || (len > int.MaxValue))
				{
					throw new RuntimeException("invalid length of " + len + " passed to posixFadviseIfPossible"
						);
				}
				if ((offset < 0) || (offset > int.MaxValue))
				{
					throw new RuntimeException("invalid offset of " + offset + " passed to posixFadviseIfPossible"
						);
				}
				TestCachingStrategy.Stats stats = map[name];
				if (stats == null)
				{
					stats = new TestCachingStrategy.Stats(name);
					map[name] = stats;
				}
				stats.Fadvise((int)offset, (int)len, flags);
				base.PosixFadviseIfPossible(name, fd, offset, len, flags);
			}

			internal virtual void Clear()
			{
				lock (this)
				{
					map.Clear();
				}
			}

			internal virtual TestCachingStrategy.Stats GetStats(string fileName)
			{
				lock (this)
				{
					return map[fileName];
				}
			}

			public override string ToString()
			{
				lock (this)
				{
					StringBuilder bld = new StringBuilder();
					bld.Append("TestRecordingCacheManipulator{");
					string prefix = string.Empty;
					foreach (string fileName in map.Keys)
					{
						bld.Append(prefix);
						prefix = ", ";
						bld.Append(fileName);
					}
					bld.Append("}");
					return bld.ToString();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		internal static void CreateHdfsFile(FileSystem fs, Path p, long length, bool dropBehind
			)
		{
			FSDataOutputStream fos = null;
			try
			{
				// create file with replication factor of 1
				fos = fs.Create(p, (short)1);
				if (dropBehind != null)
				{
					fos.SetDropBehind(dropBehind);
				}
				byte[] buf = new byte[8196];
				while (length > 0)
				{
					int amt = (length > buf.Length) ? buf.Length : (int)length;
					fos.Write(buf, 0, amt);
					length -= amt;
				}
			}
			catch (IOException e)
			{
				Log.Error("ioexception", e);
			}
			finally
			{
				if (fos != null)
				{
					fos.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		internal static long ReadHdfsFile(FileSystem fs, Path p, long length, bool dropBehind
			)
		{
			FSDataInputStream fis = null;
			long totalRead = 0;
			try
			{
				fis = fs.Open(p);
				if (dropBehind != null)
				{
					fis.SetDropBehind(dropBehind);
				}
				byte[] buf = new byte[8196];
				while (length > 0)
				{
					int amt = (length > buf.Length) ? buf.Length : (int)length;
					int ret = fis.Read(buf, 0, amt);
					if (ret == -1)
					{
						return totalRead;
					}
					totalRead += ret;
					length -= ret;
				}
			}
			catch (IOException e)
			{
				Log.Error("ioexception", e);
			}
			finally
			{
				if (fis != null)
				{
					fis.Close();
				}
			}
			throw new RuntimeException("unreachable");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFadviseAfterWriteThenRead()
		{
			// start a cluster
			Log.Info("testFadviseAfterWriteThenRead");
			tracker.Clear();
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			string TestPath = "/test";
			int TestPathLen = MaxTestFileLen;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				// create new file
				CreateHdfsFile(fs, new Path(TestPath), TestPathLen, true);
				// verify that we dropped everything from the cache during file creation.
				ExtendedBlock block = cluster.GetNameNode().GetRpcServer().GetBlockLocations(TestPath
					, 0, long.MaxValue).Get(0).GetBlock();
				string fadvisedFileName = cluster.GetBlockFile(0, block).GetName();
				TestCachingStrategy.Stats stats = tracker.GetStats(fadvisedFileName);
				stats.AssertDroppedInRange(0, TestPathLen - WritePacketSize);
				stats.Clear();
				// read file
				ReadHdfsFile(fs, new Path(TestPath), long.MaxValue, true);
				// verify that we dropped everything from the cache.
				NUnit.Framework.Assert.IsNotNull(stats);
				stats.AssertDroppedInRange(0, TestPathLen - WritePacketSize);
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
		/// Test the scenario where the DataNode defaults to not dropping the cache,
		/// but our client defaults are set.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestClientDefaults()
		{
			// start a cluster
			Log.Info("testClientDefaults");
			tracker.Clear();
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsDatanodeDropCacheBehindReadsKey, false);
			conf.SetBoolean(DFSConfigKeys.DfsDatanodeDropCacheBehindWritesKey, false);
			conf.SetBoolean(DFSConfigKeys.DfsClientCacheDropBehindReads, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientCacheDropBehindWrites, true);
			MiniDFSCluster cluster = null;
			string TestPath = "/test";
			int TestPathLen = MaxTestFileLen;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				// create new file
				CreateHdfsFile(fs, new Path(TestPath), TestPathLen, null);
				// verify that we dropped everything from the cache during file creation.
				ExtendedBlock block = cluster.GetNameNode().GetRpcServer().GetBlockLocations(TestPath
					, 0, long.MaxValue).Get(0).GetBlock();
				string fadvisedFileName = cluster.GetBlockFile(0, block).GetName();
				TestCachingStrategy.Stats stats = tracker.GetStats(fadvisedFileName);
				stats.AssertDroppedInRange(0, TestPathLen - WritePacketSize);
				stats.Clear();
				// read file
				ReadHdfsFile(fs, new Path(TestPath), long.MaxValue, null);
				// verify that we dropped everything from the cache.
				NUnit.Framework.Assert.IsNotNull(stats);
				stats.AssertDroppedInRange(0, TestPathLen - WritePacketSize);
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
		public virtual void TestFadviseSkippedForSmallReads()
		{
			// start a cluster
			Log.Info("testFadviseSkippedForSmallReads");
			tracker.Clear();
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsDatanodeDropCacheBehindReadsKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsDatanodeDropCacheBehindWritesKey, true);
			MiniDFSCluster cluster = null;
			string TestPath = "/test";
			int TestPathLen = MaxTestFileLen;
			FSDataInputStream fis = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				// create new file
				CreateHdfsFile(fs, new Path(TestPath), TestPathLen, null);
				// Since the DataNode was configured with drop-behind, and we didn't
				// specify any policy, we should have done drop-behind.
				ExtendedBlock block = cluster.GetNameNode().GetRpcServer().GetBlockLocations(TestPath
					, 0, long.MaxValue).Get(0).GetBlock();
				string fadvisedFileName = cluster.GetBlockFile(0, block).GetName();
				TestCachingStrategy.Stats stats = tracker.GetStats(fadvisedFileName);
				stats.AssertDroppedInRange(0, TestPathLen - WritePacketSize);
				stats.Clear();
				stats.AssertNotDroppedInRange(0, TestPathLen);
				// read file
				fis = fs.Open(new Path(TestPath));
				byte[] buf = new byte[17];
				fis.ReadFully(4096, buf, 0, buf.Length);
				// we should not have dropped anything because of the small read.
				stats = tracker.GetStats(fadvisedFileName);
				stats.AssertNotDroppedInRange(0, TestPathLen - WritePacketSize);
			}
			finally
			{
				IOUtils.Cleanup(null, fis);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNoFadviseAfterWriteThenRead()
		{
			// start a cluster
			Log.Info("testNoFadviseAfterWriteThenRead");
			tracker.Clear();
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			string TestPath = "/test";
			int TestPathLen = MaxTestFileLen;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				// create new file
				CreateHdfsFile(fs, new Path(TestPath), TestPathLen, false);
				// verify that we did not drop everything from the cache during file creation.
				ExtendedBlock block = cluster.GetNameNode().GetRpcServer().GetBlockLocations(TestPath
					, 0, long.MaxValue).Get(0).GetBlock();
				string fadvisedFileName = cluster.GetBlockFile(0, block).GetName();
				TestCachingStrategy.Stats stats = tracker.GetStats(fadvisedFileName);
				NUnit.Framework.Assert.IsNull(stats);
				// read file
				ReadHdfsFile(fs, new Path(TestPath), long.MaxValue, false);
				// verify that we dropped everything from the cache.
				NUnit.Framework.Assert.IsNull(stats);
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
		public virtual void TestSeekAfterSetDropBehind()
		{
			// start a cluster
			Log.Info("testSeekAfterSetDropBehind");
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			string TestPath = "/test";
			int TestPathLen = MaxTestFileLen;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				CreateHdfsFile(fs, new Path(TestPath), TestPathLen, false);
				// verify that we can seek after setDropBehind
				FSDataInputStream fis = fs.Open(new Path(TestPath));
				try
				{
					NUnit.Framework.Assert.IsTrue(fis.Read() != -1);
					// create BlockReader
					fis.SetDropBehind(false);
					// clear BlockReader
					fis.Seek(2);
				}
				finally
				{
					// seek
					fis.Close();
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
	}
}
