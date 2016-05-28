using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.Security;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestFSMainOperationsWebHdfs : FSMainOperationsBaseTest
	{
		private static MiniDFSCluster cluster = null;

		private static Path defaultWorkingDirectory;

		private static FileSystem fileSystem;

		public TestFSMainOperationsWebHdfs()
			: base("/tmp/TestFSMainOperationsWebHdfs")
		{
			{
				((Log4JLogger)ExceptionHandler.Log).GetLogger().SetLevel(Level.All);
			}
		}

		/// <exception cref="System.Exception"/>
		protected override FileSystem CreateFileSystem()
		{
			return fileSystem;
		}

		[BeforeClass]
		public static void SetupCluster()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 1024);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				cluster.WaitActive();
				//change root permission to 777
				cluster.GetFileSystem().SetPermission(new Path("/"), new FsPermission((short)0x1ff
					));
				string uri = WebHdfsFileSystem.Scheme + "://" + conf.Get(DFSConfigKeys.DfsNamenodeHttpAddressKey
					);
				//get file system as a non-superuser
				UserGroupInformation current = UserGroupInformation.GetCurrentUser();
				UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting(current.GetShortUserName
					() + "x", new string[] { "user" });
				fileSystem = ugi.DoAs(new _PrivilegedExceptionAction_91(uri, conf));
				defaultWorkingDirectory = fileSystem.GetWorkingDirectory();
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}

		private sealed class _PrivilegedExceptionAction_91 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_91(string uri, Configuration conf)
			{
				this.uri = uri;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(new URI(uri), conf);
			}

			private readonly string uri;

			private readonly Configuration conf;
		}

		[AfterClass]
		public static void ShutdownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
				cluster = null;
			}
		}

		protected override Path GetDefaultWorkingDirectory()
		{
			return defaultWorkingDirectory;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConcat()
		{
			Path[] paths = new Path[] { new Path("/test/hadoop/file1"), new Path("/test/hadoop/file2"
				), new Path("/test/hadoop/file3") };
			DFSTestUtil.CreateFile(fSys, paths[0], 1024, (short)3, 0);
			DFSTestUtil.CreateFile(fSys, paths[1], 1024, (short)3, 0);
			DFSTestUtil.CreateFile(fSys, paths[2], 1024, (short)3, 0);
			Path catPath = new Path("/test/hadoop/catFile");
			DFSTestUtil.CreateFile(fSys, catPath, 1024, (short)3, 0);
			NUnit.Framework.Assert.IsTrue(Exists(fSys, catPath));
			fSys.Concat(catPath, paths);
			NUnit.Framework.Assert.IsFalse(Exists(fSys, paths[0]));
			NUnit.Framework.Assert.IsFalse(Exists(fSys, paths[1]));
			NUnit.Framework.Assert.IsFalse(Exists(fSys, paths[2]));
			FileStatus fileStatus = fSys.GetFileStatus(catPath);
			NUnit.Framework.Assert.AreEqual(1024 * 4, fileStatus.GetLen());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncate()
		{
			short repl = 3;
			int blockSize = 1024;
			int numOfBlocks = 2;
			Path dir = GetTestRootPath(fSys, "test/hadoop");
			Path file = GetTestRootPath(fSys, "test/hadoop/file");
			byte[] data = GetFileData(numOfBlocks, blockSize);
			CreateFile(fSys, file, data, blockSize, repl);
			int newLength = blockSize;
			bool isReady = fSys.Truncate(file, newLength);
			NUnit.Framework.Assert.IsTrue("Recovery is not expected.", isReady);
			FileStatus fileStatus = fSys.GetFileStatus(file);
			NUnit.Framework.Assert.AreEqual(fileStatus.GetLen(), newLength);
			AppendTestUtil.CheckFullFile(fSys, file, newLength, data, file.ToString());
			ContentSummary cs = fSys.GetContentSummary(dir);
			NUnit.Framework.Assert.AreEqual("Bad disk space usage", cs.GetSpaceConsumed(), newLength
				 * repl);
			NUnit.Framework.Assert.IsTrue("Deleted", fSys.Delete(dir, true));
		}

		internal bool closedInputStream = false;

		// Test that WebHdfsFileSystem.jsonParse() closes the connection's input
		// stream.
		// Closing the inputstream in jsonParse will allow WebHDFS to reuse
		// connections to the namenode rather than needing to always open new ones.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJsonParseClosesInputStream()
		{
			WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)fileSystem;
			Path file = GetTestRootPath(fSys, "test/hadoop/file");
			CreateFile(file);
			HttpOpParam.OP op = GetOpParam.OP.Gethomedirectory;
			Uri url = webhdfs.ToUrl(op, file);
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			conn.SetRequestMethod(op.GetType().ToString());
			conn.Connect();
			InputStream myIn = new _InputStream_184(this, conn);
			HttpURLConnection spyConn = Org.Mockito.Mockito.Spy(conn);
			Org.Mockito.Mockito.DoReturn(myIn).When(spyConn).GetInputStream();
			try
			{
				NUnit.Framework.Assert.IsFalse(closedInputStream);
				WebHdfsFileSystem.JsonParse(spyConn, false);
				NUnit.Framework.Assert.IsTrue(closedInputStream);
			}
			catch (IOException)
			{
				TestCase.Fail();
			}
			conn.Disconnect();
		}

		private sealed class _InputStream_184 : InputStream
		{
			public _InputStream_184(TestFSMainOperationsWebHdfs _enclosing, HttpURLConnection
				 conn)
			{
				this._enclosing = _enclosing;
				this.conn = conn;
				this.localConn = conn;
			}

			private HttpURLConnection localConn;

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				this._enclosing.closedInputStream = true;
				this.localConn.GetInputStream().Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				return this.localConn.GetInputStream().Read();
			}

			private readonly TestFSMainOperationsWebHdfs _enclosing;

			private readonly HttpURLConnection conn;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public override void TestMkdirsFailsForSubdirectoryOfExistingFile()
		{
			Path testDir = GetTestRootPath(fSys, "test/hadoop");
			NUnit.Framework.Assert.IsFalse(Exists(fSys, testDir));
			fSys.Mkdirs(testDir);
			NUnit.Framework.Assert.IsTrue(Exists(fSys, testDir));
			CreateFile(GetTestRootPath(fSys, "test/hadoop/file"));
			Path testSubDir = GetTestRootPath(fSys, "test/hadoop/file/subdir");
			try
			{
				fSys.Mkdirs(testSubDir);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			try
			{
				NUnit.Framework.Assert.IsFalse(Exists(fSys, testSubDir));
			}
			catch (AccessControlException)
			{
			}
			// also okay for HDFS.
			Path testDeepSubDir = GetTestRootPath(fSys, "test/hadoop/file/deep/sub/dir");
			try
			{
				fSys.Mkdirs(testDeepSubDir);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			try
			{
				NUnit.Framework.Assert.IsFalse(Exists(fSys, testDeepSubDir));
			}
			catch (AccessControlException)
			{
			}
		}
		// also okay for HDFS.
	}
}
