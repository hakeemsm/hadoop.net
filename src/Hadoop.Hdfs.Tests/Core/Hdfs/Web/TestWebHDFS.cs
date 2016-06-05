using System;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Web.Resources;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>Test WebHDFS</summary>
	public class TestWebHDFS
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestWebHDFS));

		internal static readonly Random Random = new Random();

		internal static readonly long systemStartTime = Runtime.NanoTime();

		/// <summary>A timer for measuring performance.</summary>
		internal class Ticker
		{
			internal readonly string name;

			internal readonly long startTime = Runtime.NanoTime();

			private long previousTick;

			internal Ticker(string name, string format, params object[] args)
			{
				previousTick = startTime;
				this.name = name;
				Log.Info(string.Format("\n\n%s START: %s\n", name, string.Format(format, args)));
			}

			internal virtual void Tick(long nBytes, string format, params object[] args)
			{
				long now = Runtime.NanoTime();
				if (now - previousTick > 10000000000L)
				{
					previousTick = now;
					double mintues = (now - systemStartTime) / 60000000000.0;
					Log.Info(string.Format("\n\n%s %.2f min) %s %s\n", name, mintues, string.Format(format
						, args), ToMpsString(nBytes, now)));
				}
			}

			internal virtual void End(long nBytes)
			{
				long now = Runtime.NanoTime();
				double seconds = (now - startTime) / 1000000000.0;
				Log.Info(string.Format("\n\n%s END: duration=%.2fs %s\n", name, seconds, ToMpsString
					(nBytes, now)));
			}

			internal virtual string ToMpsString(long nBytes, long now)
			{
				double mb = nBytes / (double)(1 << 20);
				double mps = mb * 1000000000.0 / (now - startTime);
				return string.Format("[nBytes=%.2fMB, speed=%.2fMB/s]", mb, mps);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLargeFile()
		{
			LargeFileTest(200L << 20);
		}

		//200MB file length
		/// <summary>Test read and write large files.</summary>
		/// <exception cref="System.Exception"/>
		internal static void LargeFileTest(long fileLength)
		{
			Configuration conf = WebHdfsTestUtil.CreateConf();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			try
			{
				cluster.WaitActive();
				FileSystem fs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem.Scheme
					);
				Path dir = new Path("/test/largeFile");
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(dir));
				byte[] data = new byte[1 << 20];
				Random.NextBytes(data);
				byte[] expected = new byte[2 * data.Length];
				System.Array.Copy(data, 0, expected, 0, data.Length);
				System.Array.Copy(data, 0, expected, data.Length, data.Length);
				Path p = new Path(dir, "file");
				TestWebHDFS.Ticker t = new TestWebHDFS.Ticker("WRITE", "fileLength=" + fileLength
					);
				FSDataOutputStream @out = fs.Create(p);
				try
				{
					long remaining = fileLength;
					for (; remaining > 0; )
					{
						t.Tick(fileLength - remaining, "remaining=%d", remaining);
						int n = (int)Math.Min(remaining, data.Length);
						@out.Write(data, 0, n);
						remaining -= n;
					}
				}
				finally
				{
					@out.Close();
				}
				t.End(fileLength);
				NUnit.Framework.Assert.AreEqual(fileLength, fs.GetFileStatus(p).GetLen());
				long smallOffset = Random.Next(1 << 20) + (1 << 20);
				long largeOffset = fileLength - smallOffset;
				byte[] buf = new byte[data.Length];
				VerifySeek(fs, p, largeOffset, fileLength, buf, expected);
				VerifySeek(fs, p, smallOffset, fileLength, buf, expected);
				VerifyPread(fs, p, largeOffset, fileLength, buf, expected);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		internal static void CheckData(long offset, long remaining, int n, byte[] actual, 
			byte[] expected)
		{
			if (Random.Next(100) == 0)
			{
				int j = (int)(offset % actual.Length);
				for (int i = 0; i < n; i++)
				{
					if (expected[j] != actual[i])
					{
						NUnit.Framework.Assert.Fail("expected[" + j + "]=" + expected[j] + " != actual[" 
							+ i + "]=" + actual[i] + ", offset=" + offset + ", remaining=" + remaining + ", n="
							 + n);
					}
					j++;
				}
			}
		}

		/// <summary>test seek</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void VerifySeek(FileSystem fs, Path p, long offset, long length, 
			byte[] buf, byte[] expected)
		{
			long remaining = length - offset;
			long @checked = 0;
			Log.Info("XXX SEEK: offset=" + offset + ", remaining=" + remaining);
			TestWebHDFS.Ticker t = new TestWebHDFS.Ticker("SEEK", "offset=%d, remaining=%d", 
				offset, remaining);
			FSDataInputStream @in = fs.Open(p, 64 << 10);
			@in.Seek(offset);
			for (; remaining > 0; )
			{
				t.Tick(@checked, "offset=%d, remaining=%d", offset, remaining);
				int n = (int)Math.Min(remaining, buf.Length);
				@in.ReadFully(buf, 0, n);
				CheckData(offset, remaining, n, buf, expected);
				offset += n;
				remaining -= n;
				@checked += n;
			}
			@in.Close();
			t.End(@checked);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void VerifyPread(FileSystem fs, Path p, long offset, long length, 
			byte[] buf, byte[] expected)
		{
			long remaining = length - offset;
			long @checked = 0;
			Log.Info("XXX PREAD: offset=" + offset + ", remaining=" + remaining);
			TestWebHDFS.Ticker t = new TestWebHDFS.Ticker("PREAD", "offset=%d, remaining=%d", 
				offset, remaining);
			FSDataInputStream @in = fs.Open(p, 64 << 10);
			for (; remaining > 0; )
			{
				t.Tick(@checked, "offset=%d, remaining=%d", offset, remaining);
				int n = (int)Math.Min(remaining, buf.Length);
				@in.ReadFully(offset, buf, 0, n);
				CheckData(offset, remaining, n, buf, expected);
				offset += n;
				remaining -= n;
				@checked += n;
			}
			@in.Close();
			t.End(@checked);
		}

		/// <summary>Test client retry with namenode restarting.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestNamenodeRestart()
		{
			((Log4JLogger)NamenodeWebHdfsMethods.Log).GetLogger().SetLevel(Level.All);
			Configuration conf = WebHdfsTestUtil.CreateConf();
			TestDFSClientRetries.NamenodeRestartTest(conf, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLargeDirectory()
		{
			Configuration conf = WebHdfsTestUtil.CreateConf();
			int listLimit = 2;
			// force small chunking of directory listing
			conf.SetInt(DFSConfigKeys.DfsListLimit, listLimit);
			// force paths to be only owner-accessible to ensure ugi isn't changing
			// during listStatus
			FsPermission.SetUMask(conf, new FsPermission((short)0x3f));
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			try
			{
				cluster.WaitActive();
				WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem.Scheme).SetPermission
					(new Path("/"), new FsPermission(FsAction.All, FsAction.All, FsAction.All));
				// trick the NN into not believing it's not the superuser so we can
				// tell if the correct user is used by listStatus
				UserGroupInformation.SetLoginUser(UserGroupInformation.CreateUserForTesting("not-superuser"
					, new string[] { "not-supergroup" }));
				UserGroupInformation.CreateUserForTesting("me", new string[] { "my-group" }).DoAs
					(new _PrivilegedExceptionAction_263(conf, listLimit));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _PrivilegedExceptionAction_263 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_263(Configuration conf, int listLimit)
			{
				this.conf = conf;
				this.listLimit = listLimit;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Sharpen.URISyntaxException"/>
			public Void Run()
			{
				FileSystem fs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem.Scheme
					);
				Path d = new Path("/my-dir");
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(d));
				for (int i = 0; i < listLimit * 3; i++)
				{
					Path p = new Path(d, "file-" + i);
					NUnit.Framework.Assert.IsTrue(fs.CreateNewFile(p));
				}
				NUnit.Framework.Assert.AreEqual(listLimit * 3, fs.ListStatus(d).Length);
				return null;
			}

			private readonly Configuration conf;

			private readonly int listLimit;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNumericalUserName()
		{
			Configuration conf = WebHdfsTestUtil.CreateConf();
			conf.Set(DFSConfigKeys.DfsWebhdfsUserPatternKey, "^[A-Za-z0-9_][A-Za-z0-9._-]*[$]?$"
				);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				cluster.WaitActive();
				WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem.Scheme).SetPermission
					(new Path("/"), new FsPermission(FsAction.All, FsAction.All, FsAction.All));
				UserGroupInformation.CreateUserForTesting("123", new string[] { "my-group" }).DoAs
					(new _PrivilegedExceptionAction_296(conf));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _PrivilegedExceptionAction_296 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_296(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Sharpen.URISyntaxException"/>
			public Void Run()
			{
				FileSystem fs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem.Scheme
					);
				Path d = new Path("/my-dir");
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(d));
				return null;
			}

			private readonly Configuration conf;
		}

		/// <summary>
		/// Test for catching "no datanode" IOException, when to create a file
		/// but datanode is not running for some reason.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCreateWithNoDN()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = WebHdfsTestUtil.CreateConf();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				conf.SetInt(DFSConfigKeys.DfsReplicationKey, 1);
				cluster.WaitActive();
				FileSystem fs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem.Scheme
					);
				fs.Create(new Path("/testnodatanode"));
				NUnit.Framework.Assert.Fail("No exception was thrown");
			}
			catch (IOException ex)
			{
				GenericTestUtils.AssertExceptionContains("Failed to find datanode", ex);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>WebHdfs should be enabled by default after HDFS-5532</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWebHdfsEnabledByDefault()
		{
			Configuration conf = new HdfsConfiguration();
			NUnit.Framework.Assert.IsTrue(conf.GetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, 
				false));
		}

		/// <summary>Test snapshot creation through WebHdfs</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWebHdfsCreateSnapshot()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = WebHdfsTestUtil.CreateConf();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				FileSystem webHdfs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem
					.Scheme);
				Path foo = new Path("/foo");
				dfs.Mkdirs(foo);
				try
				{
					webHdfs.CreateSnapshot(foo);
					NUnit.Framework.Assert.Fail("Cannot create snapshot on a non-snapshottable directory"
						);
				}
				catch (Exception e)
				{
					GenericTestUtils.AssertExceptionContains("Directory is not a snapshottable directory"
						, e);
				}
				// allow snapshots on /foo
				dfs.AllowSnapshot(foo);
				// create snapshots on foo using WebHdfs
				webHdfs.CreateSnapshot(foo, "s1");
				// create snapshot without specifying name
				Path spath = webHdfs.CreateSnapshot(foo, null);
				NUnit.Framework.Assert.IsTrue(webHdfs.Exists(spath));
				Path s1path = SnapshotTestHelper.GetSnapshotRoot(foo, "s1");
				NUnit.Framework.Assert.IsTrue(webHdfs.Exists(s1path));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test snapshot deletion through WebHdfs</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWebHdfsDeleteSnapshot()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = WebHdfsTestUtil.CreateConf();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				FileSystem webHdfs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem
					.Scheme);
				Path foo = new Path("/foo");
				dfs.Mkdirs(foo);
				dfs.AllowSnapshot(foo);
				webHdfs.CreateSnapshot(foo, "s1");
				Path spath = webHdfs.CreateSnapshot(foo, null);
				NUnit.Framework.Assert.IsTrue(webHdfs.Exists(spath));
				Path s1path = SnapshotTestHelper.GetSnapshotRoot(foo, "s1");
				NUnit.Framework.Assert.IsTrue(webHdfs.Exists(s1path));
				// delete the two snapshots
				webHdfs.DeleteSnapshot(foo, "s1");
				NUnit.Framework.Assert.IsFalse(webHdfs.Exists(s1path));
				webHdfs.DeleteSnapshot(foo, spath.GetName());
				NUnit.Framework.Assert.IsFalse(webHdfs.Exists(spath));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test snapshot rename through WebHdfs</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWebHdfsRenameSnapshot()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = WebHdfsTestUtil.CreateConf();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				FileSystem webHdfs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem
					.Scheme);
				Path foo = new Path("/foo");
				dfs.Mkdirs(foo);
				dfs.AllowSnapshot(foo);
				webHdfs.CreateSnapshot(foo, "s1");
				Path s1path = SnapshotTestHelper.GetSnapshotRoot(foo, "s1");
				NUnit.Framework.Assert.IsTrue(webHdfs.Exists(s1path));
				// rename s1 to s2
				webHdfs.RenameSnapshot(foo, "s1", "s2");
				NUnit.Framework.Assert.IsFalse(webHdfs.Exists(s1path));
				Path s2path = SnapshotTestHelper.GetSnapshotRoot(foo, "s2");
				NUnit.Framework.Assert.IsTrue(webHdfs.Exists(s2path));
				webHdfs.DeleteSnapshot(foo, "s2");
				NUnit.Framework.Assert.IsFalse(webHdfs.Exists(s2path));
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
		/// Make sure a RetriableException is thrown when rpcServer is null in
		/// NamenodeWebHdfsMethods.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRaceWhileNNStartup()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = WebHdfsTestUtil.CreateConf();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				NameNode namenode = cluster.GetNameNode();
				NamenodeProtocols rpcServer = namenode.GetRpcServer();
				Whitebox.SetInternalState(namenode, "rpcServer", null);
				Path foo = new Path("/foo");
				FileSystem webHdfs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem
					.Scheme);
				try
				{
					webHdfs.Mkdirs(foo);
					NUnit.Framework.Assert.Fail("Expected RetriableException");
				}
				catch (RetriableException e)
				{
					GenericTestUtils.AssertExceptionContains("Namenode is in startup mode", e);
				}
				Whitebox.SetInternalState(namenode, "rpcServer", rpcServer);
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
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestDTInInsecureClusterWithFallback()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = WebHdfsTestUtil.CreateConf();
			conf.SetBoolean(CommonConfigurationKeys.IpcClientFallbackToSimpleAuthAllowedKey, 
				true);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				FileSystem webHdfs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem
					.Scheme);
				NUnit.Framework.Assert.IsNull(webHdfs.GetDelegationToken(null));
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
		public virtual void TestDTInInsecureCluster()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = WebHdfsTestUtil.CreateConf();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				FileSystem webHdfs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem
					.Scheme);
				webHdfs.GetDelegationToken(null);
				NUnit.Framework.Assert.Fail("No exception is thrown.");
			}
			catch (AccessControlException ace)
			{
				NUnit.Framework.Assert.IsTrue(ace.Message.StartsWith(WebHdfsFileSystem.CantFallbackToInsecureMsg
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

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWebHdfsOffsetAndLength()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = WebHdfsTestUtil.CreateConf();
			int Offset = 42;
			int Length = 512;
			string Path = "/foo";
			byte[] Contents = new byte[1024];
			Random.NextBytes(Contents);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				WebHdfsFileSystem fs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem
					.Scheme);
				using (OutputStream os = fs.Create(new Path(Path)))
				{
					os.Write(Contents);
				}
				IPEndPoint addr = cluster.GetNameNode().GetHttpAddress();
				Uri url = new Uri("http", addr.GetHostString(), addr.Port, WebHdfsFileSystem.PathPrefix
					 + Path + "?op=OPEN" + Param.ToSortedString("&", new OffsetParam((long)Offset), 
					new LengthParam((long)Length)));
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.SetInstanceFollowRedirects(true);
				NUnit.Framework.Assert.AreEqual(Length, conn.GetContentLength());
				byte[] subContents = new byte[Length];
				byte[] realContents = new byte[Length];
				System.Array.Copy(Contents, Offset, subContents, 0, Length);
				IOUtils.ReadFully(conn.GetInputStream(), realContents);
				Assert.AssertArrayEquals(subContents, realContents);
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
