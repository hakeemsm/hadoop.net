using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Test of the URL stream handler.</summary>
	public class TestUrlStreamHandler
	{
		private static readonly FilePath TestRootDir = PathUtils.GetTestDir(typeof(TestUrlStreamHandler
			));

		/// <summary>Test opening and reading from an InputStream through a hdfs:// URL.</summary>
		/// <remarks>
		/// Test opening and reading from an InputStream through a hdfs:// URL.
		/// <p>
		/// First generate a file with some content through the FileSystem API, then
		/// try to open and read the file through the URL stream API.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDfsUrls()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			FileSystem fs = cluster.GetFileSystem();
			// Setup our own factory
			// setURLSteramHandlerFactor is can be set at most once in the JVM
			// the new URLStreamHandler is valid for all tests cases 
			// in TestStreamHandler
			FsUrlStreamHandlerFactory factory = new FsUrlStreamHandlerFactory();
			Uri.SetURLStreamHandlerFactory(factory);
			Path filePath = new Path("/thefile");
			try
			{
				byte[] fileContent = new byte[1024];
				for (int i = 0; i < fileContent.Length; ++i)
				{
					fileContent[i] = unchecked((byte)i);
				}
				// First create the file through the FileSystem API
				OutputStream os = fs.Create(filePath);
				os.Write(fileContent);
				os.Close();
				// Second, open and read the file content through the URL API
				URI uri = fs.GetUri();
				Uri fileURL = new Uri(uri.GetScheme(), uri.GetHost(), uri.GetPort(), filePath.ToString
					());
				InputStream @is = fileURL.OpenStream();
				NUnit.Framework.Assert.IsNotNull(@is);
				byte[] bytes = new byte[4096];
				NUnit.Framework.Assert.AreEqual(1024, @is.Read(bytes));
				@is.Close();
				for (int i_1 = 0; i_1 < fileContent.Length; ++i_1)
				{
					NUnit.Framework.Assert.AreEqual(fileContent[i_1], bytes[i_1]);
				}
				// Cleanup: delete the file
				fs.Delete(filePath, false);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Test opening and reading from an InputStream through a file:// URL.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileUrls()
		{
			// URLStreamHandler is already set in JVM by testDfsUrls() 
			Configuration conf = new HdfsConfiguration();
			// Locate the test temporary directory.
			if (!TestRootDir.Exists())
			{
				if (!TestRootDir.Mkdirs())
				{
					throw new IOException("Cannot create temporary directory: " + TestRootDir);
				}
			}
			FilePath tmpFile = new FilePath(TestRootDir, "thefile");
			URI uri = tmpFile.ToURI();
			FileSystem fs = FileSystem.Get(uri, conf);
			try
			{
				byte[] fileContent = new byte[1024];
				for (int i = 0; i < fileContent.Length; ++i)
				{
					fileContent[i] = unchecked((byte)i);
				}
				// First create the file through the FileSystem API
				OutputStream os = fs.Create(new Path(uri.GetPath()));
				os.Write(fileContent);
				os.Close();
				// Second, open and read the file content through the URL API.
				Uri fileURL = uri.ToURL();
				InputStream @is = fileURL.OpenStream();
				NUnit.Framework.Assert.IsNotNull(@is);
				byte[] bytes = new byte[4096];
				NUnit.Framework.Assert.AreEqual(1024, @is.Read(bytes));
				@is.Close();
				for (int i_1 = 0; i_1 < fileContent.Length; ++i_1)
				{
					NUnit.Framework.Assert.AreEqual(fileContent[i_1], bytes[i_1]);
				}
				// Cleanup: delete the file
				fs.Delete(new Path(uri.GetPath()), false);
			}
			finally
			{
				fs.Close();
			}
		}
	}
}
