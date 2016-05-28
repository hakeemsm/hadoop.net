using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Test NameNode port defaulting code.</summary>
	public class TestDefaultNameNodePort
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAddressFromString()
		{
			NUnit.Framework.Assert.AreEqual(NameNode.GetAddress("foo").Port, NameNode.DefaultPort
				);
			NUnit.Framework.Assert.AreEqual(NameNode.GetAddress("hdfs://foo/").Port, NameNode
				.DefaultPort);
			NUnit.Framework.Assert.AreEqual(NameNode.GetAddress("hdfs://foo:555").Port, 555);
			NUnit.Framework.Assert.AreEqual(NameNode.GetAddress("foo:555").Port, 555);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAddressFromConf()
		{
			Configuration conf = new HdfsConfiguration();
			FileSystem.SetDefaultUri(conf, "hdfs://foo/");
			NUnit.Framework.Assert.AreEqual(NameNode.GetAddress(conf).Port, NameNode.DefaultPort
				);
			FileSystem.SetDefaultUri(conf, "hdfs://foo:555/");
			NUnit.Framework.Assert.AreEqual(NameNode.GetAddress(conf).Port, 555);
			FileSystem.SetDefaultUri(conf, "foo");
			NUnit.Framework.Assert.AreEqual(NameNode.GetAddress(conf).Port, NameNode.DefaultPort
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetUri()
		{
			NUnit.Framework.Assert.AreEqual(NameNode.GetUri(new IPEndPoint("foo", 555)), URI.
				Create("hdfs://foo:555"));
			NUnit.Framework.Assert.AreEqual(NameNode.GetUri(new IPEndPoint("foo", NameNode.DefaultPort
				)), URI.Create("hdfs://foo"));
		}
	}
}
