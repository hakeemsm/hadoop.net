using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestHttpFSPorts
	{
		private static readonly Configuration conf = new Configuration();

		[SetUp]
		public virtual void SetupConfig()
		{
			conf.SetInt(DFSConfigKeys.DfsNamenodeHttpPortKey, 123);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHttpsPortKey, 456);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWebHdfsCustomDefaultPorts()
		{
			URI uri = URI.Create("webhdfs://localhost");
			WebHdfsFileSystem fs = (WebHdfsFileSystem)FileSystem.Get(uri, conf);
			NUnit.Framework.Assert.AreEqual(123, fs.GetDefaultPort());
			NUnit.Framework.Assert.AreEqual(uri, fs.GetUri());
			NUnit.Framework.Assert.AreEqual("127.0.0.1:123", fs.GetCanonicalServiceName());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWebHdfsCustomUriPortWithCustomDefaultPorts()
		{
			URI uri = URI.Create("webhdfs://localhost:789");
			WebHdfsFileSystem fs = (WebHdfsFileSystem)FileSystem.Get(uri, conf);
			NUnit.Framework.Assert.AreEqual(123, fs.GetDefaultPort());
			NUnit.Framework.Assert.AreEqual(uri, fs.GetUri());
			NUnit.Framework.Assert.AreEqual("127.0.0.1:789", fs.GetCanonicalServiceName());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSWebHdfsCustomDefaultPorts()
		{
			URI uri = URI.Create("swebhdfs://localhost");
			SWebHdfsFileSystem fs = (SWebHdfsFileSystem)FileSystem.Get(uri, conf);
			NUnit.Framework.Assert.AreEqual(456, fs.GetDefaultPort());
			NUnit.Framework.Assert.AreEqual(uri, fs.GetUri());
			NUnit.Framework.Assert.AreEqual("127.0.0.1:456", fs.GetCanonicalServiceName());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSwebHdfsCustomUriPortWithCustomDefaultPorts()
		{
			URI uri = URI.Create("swebhdfs://localhost:789");
			SWebHdfsFileSystem fs = (SWebHdfsFileSystem)FileSystem.Get(uri, conf);
			NUnit.Framework.Assert.AreEqual(456, fs.GetDefaultPort());
			NUnit.Framework.Assert.AreEqual(uri, fs.GetUri());
			NUnit.Framework.Assert.AreEqual("127.0.0.1:789", fs.GetCanonicalServiceName());
		}
	}
}
