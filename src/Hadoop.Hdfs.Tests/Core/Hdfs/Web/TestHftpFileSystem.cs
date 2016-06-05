using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestHftpFileSystem
	{
		private static readonly string Basedir = Runtime.GetProperty("test.build.dir", "target/test-dir"
			) + "/" + typeof(TestHftpFileSystem).Name;

		private static string keystoresDir;

		private static string sslConfDir;

		private static Configuration config = null;

		private static MiniDFSCluster cluster = null;

		private static string blockPoolId = null;

		private static string hftpUri = null;

		private FileSystem hdfs = null;

		private HftpFileSystem hftpFs = null;

		private static readonly Path[] TestPaths = new Path[] { new Path("/foo;bar"), new 
			Path("/foo+"), new Path("/foo+bar/foo+bar"), new Path("/foo=bar/foo=bar"), new Path
			("/foo,bar/foo,bar"), new Path("/foo@bar/foo@bar"), new Path("/foo&bar/foo&bar")
			, new Path("/foo$bar/foo$bar"), new Path("/foo_bar/foo_bar"), new Path("/foo~bar/foo~bar"
			), new Path("/foo.bar/foo.bar"), new Path("/foo../bar/foo../bar"), new Path("/foo.../bar/foo.../bar"
			), new Path("/foo'bar/foo'bar"), new Path("/foo#bar/foo#bar"), new Path("/foo!bar/foo!bar"
			), new Path("/foo bar/foo bar"), new Path("/foo?bar/foo?bar"), new Path("/foo\">bar/foo\">bar"
			) };

		// URI does not encode, Request#getPathInfo returns /foo
		// URI does not encode, Request#getPathInfo returns verbatim
		// HDFS file names may not contain ":"
		// URI percent encodes, Request#getPathInfo decodes
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			config = new Configuration();
			cluster = new MiniDFSCluster.Builder(config).NumDataNodes(2).Build();
			blockPoolId = cluster.GetNamesystem().GetBlockPoolId();
			hftpUri = "hftp://" + config.Get(DFSConfigKeys.DfsNamenodeHttpAddressKey);
			FilePath @base = new FilePath(Basedir);
			FileUtil.FullyDelete(@base);
			@base.Mkdirs();
			keystoresDir = new FilePath(Basedir).GetAbsolutePath();
			sslConfDir = KeyStoreTestUtil.GetClasspathDir(typeof(TestHftpFileSystem));
			KeyStoreTestUtil.SetupSSLConfig(keystoresDir, sslConfDir, config, false);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
			FileUtil.FullyDelete(new FilePath(Basedir));
			KeyStoreTestUtil.CleanupSSLConfig(keystoresDir, sslConfDir);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void InitFileSystems()
		{
			hdfs = cluster.GetFileSystem();
			hftpFs = (HftpFileSystem)new Path(hftpUri).GetFileSystem(config);
			// clear out the namespace
			foreach (FileStatus stat in hdfs.ListStatus(new Path("/")))
			{
				hdfs.Delete(stat.GetPath(), true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void ResetFileSystems()
		{
			FileSystem.CloseAll();
		}

		/// <summary>Test file creation and access with file names that need encoding.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileNameEncoding()
		{
			foreach (Path p in TestPaths)
			{
				// Create and access the path (data and streamFile servlets)
				FSDataOutputStream @out = hdfs.Create(p, true);
				@out.WriteBytes("0123456789");
				@out.Close();
				FSDataInputStream @in = hftpFs.Open(p);
				NUnit.Framework.Assert.AreEqual('0', @in.Read());
				@in.Close();
				// Check the file status matches the path. Hftp returns a FileStatus
				// with the entire URI, extract the path part.
				NUnit.Framework.Assert.AreEqual(p, new Path(hftpFs.GetFileStatus(p).GetPath().ToUri
					().GetPath()));
				// Test list status (listPath servlet)
				NUnit.Framework.Assert.AreEqual(1, hftpFs.ListStatus(p).Length);
				// Test content summary (contentSummary servlet)
				NUnit.Framework.Assert.IsNotNull("No content summary", hftpFs.GetContentSummary(p
					));
				// Test checksums (fileChecksum and getFileChecksum servlets)
				NUnit.Framework.Assert.IsNotNull("No file checksum", hftpFs.GetFileChecksum(p));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestDataNodeRedirect(Path path)
		{
			// Create the file
			if (hdfs.Exists(path))
			{
				hdfs.Delete(path, true);
			}
			FSDataOutputStream @out = hdfs.Create(path, (short)1);
			@out.WriteBytes("0123456789");
			@out.Close();
			// Get the path's block location so we can determine
			// if we were redirected to the right DN.
			BlockLocation[] locations = hdfs.GetFileBlockLocations(path, 0, 10);
			string xferAddr = locations[0].GetNames()[0];
			// Connect to the NN to get redirected
			Uri u = hftpFs.GetNamenodeURL("/data" + ServletUtil.EncodePath(path.ToUri().GetPath
				()), "ugi=userx,groupy");
			HttpURLConnection conn = (HttpURLConnection)u.OpenConnection();
			HttpURLConnection.SetFollowRedirects(true);
			conn.Connect();
			conn.GetInputStream();
			bool @checked = false;
			// Find the datanode that has the block according to locations
			// and check that the URL was redirected to this DN's info port
			foreach (DataNode node in cluster.GetDataNodes())
			{
				DatanodeRegistration dnR = DataNodeTestUtils.GetDNRegistrationForBP(node, blockPoolId
					);
				if (dnR.GetXferAddr().Equals(xferAddr))
				{
					@checked = true;
					NUnit.Framework.Assert.AreEqual(dnR.GetInfoPort(), conn.GetURL().Port);
				}
			}
			NUnit.Framework.Assert.IsTrue("The test never checked that location of " + "the block and hftp desitnation are the same"
				, @checked);
		}

		/// <summary>Test that clients are redirected to the appropriate DN.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDataNodeRedirect()
		{
			foreach (Path p in TestPaths)
			{
				TestDataNodeRedirect(p);
			}
		}

		/// <summary>Tests getPos() functionality.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetPos()
		{
			Path testFile = new Path("/testfile+1");
			// Write a test file.
			FSDataOutputStream @out = hdfs.Create(testFile, true);
			@out.WriteBytes("0123456789");
			@out.Close();
			FSDataInputStream @in = hftpFs.Open(testFile);
			// Test read().
			for (int i = 0; i < 5; ++i)
			{
				NUnit.Framework.Assert.AreEqual(i, @in.GetPos());
				@in.Read();
			}
			// Test read(b, off, len).
			NUnit.Framework.Assert.AreEqual(5, @in.GetPos());
			byte[] buffer = new byte[10];
			NUnit.Framework.Assert.AreEqual(2, @in.Read(buffer, 0, 2));
			NUnit.Framework.Assert.AreEqual(7, @in.GetPos());
			// Test read(b).
			int bytesRead = @in.Read(buffer);
			NUnit.Framework.Assert.AreEqual(7 + bytesRead, @in.GetPos());
			// Test EOF.
			for (int i_1 = 0; i_1 < 100; ++i_1)
			{
				@in.Read();
			}
			NUnit.Framework.Assert.AreEqual(10, @in.GetPos());
			@in.Close();
		}

		/// <summary>Tests seek().</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSeek()
		{
			Path testFile = new Path("/testfile+1");
			FSDataOutputStream @out = hdfs.Create(testFile, true);
			@out.WriteBytes("0123456789");
			@out.Close();
			FSDataInputStream @in = hftpFs.Open(testFile);
			@in.Seek(7);
			NUnit.Framework.Assert.AreEqual('7', @in.Read());
			@in.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReadClosedStream()
		{
			Path testFile = new Path("/testfile+2");
			FSDataOutputStream os = hdfs.Create(testFile, true);
			os.WriteBytes("0123456789");
			os.Close();
			// ByteRangeInputStream delays opens until reads. Make sure it doesn't
			// open a closed stream that has never been opened
			FSDataInputStream @in = hftpFs.Open(testFile);
			@in.Close();
			CheckClosedStream(@in);
			CheckClosedStream(@in.GetWrappedStream());
			// force the stream to connect and then close it
			@in = hftpFs.Open(testFile);
			int ch = @in.Read();
			NUnit.Framework.Assert.AreEqual('0', ch);
			@in.Close();
			CheckClosedStream(@in);
			CheckClosedStream(@in.GetWrappedStream());
			// make sure seeking doesn't automagically reopen the stream
			@in.Seek(4);
			CheckClosedStream(@in);
			CheckClosedStream(@in.GetWrappedStream());
		}

		private void CheckClosedStream(InputStream @is)
		{
			IOException ioe = null;
			try
			{
				@is.Read();
			}
			catch (IOException e)
			{
				ioe = e;
			}
			NUnit.Framework.Assert.IsNotNull("No exception on closed read", ioe);
			NUnit.Framework.Assert.AreEqual("Stream closed", ioe.Message);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHftpDefaultPorts()
		{
			Configuration conf = new Configuration();
			URI uri = URI.Create("hftp://localhost");
			HftpFileSystem fs = (HftpFileSystem)FileSystem.Get(uri, conf);
			NUnit.Framework.Assert.AreEqual(DFSConfigKeys.DfsNamenodeHttpPortDefault, fs.GetDefaultPort
				());
			NUnit.Framework.Assert.AreEqual(uri, fs.GetUri());
			// HFTP uses http to get the token so canonical service name should
			// return the http port.
			NUnit.Framework.Assert.AreEqual("127.0.0.1:" + DFSConfigKeys.DfsNamenodeHttpPortDefault
				, fs.GetCanonicalServiceName());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHftpCustomDefaultPorts()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeHttpPortKey, 123);
			URI uri = URI.Create("hftp://localhost");
			HftpFileSystem fs = (HftpFileSystem)FileSystem.Get(uri, conf);
			NUnit.Framework.Assert.AreEqual(123, fs.GetDefaultPort());
			NUnit.Framework.Assert.AreEqual(uri, fs.GetUri());
			// HFTP uses http to get the token so canonical service name should
			// return the http port.
			NUnit.Framework.Assert.AreEqual("127.0.0.1:123", fs.GetCanonicalServiceName());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHftpCustomUriPortWithDefaultPorts()
		{
			Configuration conf = new Configuration();
			URI uri = URI.Create("hftp://localhost:123");
			HftpFileSystem fs = (HftpFileSystem)FileSystem.Get(uri, conf);
			NUnit.Framework.Assert.AreEqual(DFSConfigKeys.DfsNamenodeHttpPortDefault, fs.GetDefaultPort
				());
			NUnit.Framework.Assert.AreEqual(uri, fs.GetUri());
			NUnit.Framework.Assert.AreEqual("127.0.0.1:123", fs.GetCanonicalServiceName());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHftpCustomUriPortWithCustomDefaultPorts()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeHttpPortKey, 123);
			URI uri = URI.Create("hftp://localhost:789");
			HftpFileSystem fs = (HftpFileSystem)FileSystem.Get(uri, conf);
			NUnit.Framework.Assert.AreEqual(123, fs.GetDefaultPort());
			NUnit.Framework.Assert.AreEqual(uri, fs.GetUri());
			NUnit.Framework.Assert.AreEqual("127.0.0.1:789", fs.GetCanonicalServiceName());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTimeout()
		{
			Configuration conf = new Configuration();
			URI uri = URI.Create("hftp://localhost");
			HftpFileSystem fs = (HftpFileSystem)FileSystem.Get(uri, conf);
			URLConnection conn = fs.connectionFactory.OpenConnection(new Uri("http://localhost"
				));
			NUnit.Framework.Assert.AreEqual(URLConnectionFactory.DefaultSocketTimeout, conn.GetConnectTimeout
				());
			NUnit.Framework.Assert.AreEqual(URLConnectionFactory.DefaultSocketTimeout, conn.GetReadTimeout
				());
		}

		// /
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHsftpDefaultPorts()
		{
			Configuration conf = new Configuration();
			URI uri = URI.Create("hsftp://localhost");
			HsftpFileSystem fs = (HsftpFileSystem)FileSystem.Get(uri, conf);
			NUnit.Framework.Assert.AreEqual(DFSConfigKeys.DfsNamenodeHttpsPortDefault, fs.GetDefaultPort
				());
			NUnit.Framework.Assert.AreEqual(uri, fs.GetUri());
			NUnit.Framework.Assert.AreEqual("127.0.0.1:" + DFSConfigKeys.DfsNamenodeHttpsPortDefault
				, fs.GetCanonicalServiceName());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHsftpCustomDefaultPorts()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeHttpPortKey, 123);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHttpsPortKey, 456);
			URI uri = URI.Create("hsftp://localhost");
			HsftpFileSystem fs = (HsftpFileSystem)FileSystem.Get(uri, conf);
			NUnit.Framework.Assert.AreEqual(456, fs.GetDefaultPort());
			NUnit.Framework.Assert.AreEqual(uri, fs.GetUri());
			NUnit.Framework.Assert.AreEqual("127.0.0.1:456", fs.GetCanonicalServiceName());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHsftpCustomUriPortWithDefaultPorts()
		{
			Configuration conf = new Configuration();
			URI uri = URI.Create("hsftp://localhost:123");
			HsftpFileSystem fs = (HsftpFileSystem)FileSystem.Get(uri, conf);
			NUnit.Framework.Assert.AreEqual(DFSConfigKeys.DfsNamenodeHttpsPortDefault, fs.GetDefaultPort
				());
			NUnit.Framework.Assert.AreEqual(uri, fs.GetUri());
			NUnit.Framework.Assert.AreEqual("127.0.0.1:123", fs.GetCanonicalServiceName());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHsftpCustomUriPortWithCustomDefaultPorts()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeHttpPortKey, 123);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHttpsPortKey, 456);
			URI uri = URI.Create("hsftp://localhost:789");
			HsftpFileSystem fs = (HsftpFileSystem)FileSystem.Get(uri, conf);
			NUnit.Framework.Assert.AreEqual(456, fs.GetDefaultPort());
			NUnit.Framework.Assert.AreEqual(uri, fs.GetUri());
			NUnit.Framework.Assert.AreEqual("127.0.0.1:789", fs.GetCanonicalServiceName());
		}
	}
}
