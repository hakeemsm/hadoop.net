using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Server.Jobtracker;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class TestClientProtocolProviderImpls
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterWithLocalClientProvider()
		{
			Configuration conf = new Configuration();
			conf.Set(MRConfig.FrameworkName, "local");
			Cluster cluster = new Cluster(conf);
			NUnit.Framework.Assert.IsTrue(cluster.GetClient() is LocalJobRunner);
			cluster.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterWithJTClientProvider()
		{
			Configuration conf = new Configuration();
			try
			{
				conf.Set(MRConfig.FrameworkName, "classic");
				conf.Set(JTConfig.JtIpcAddress, "local");
				new Cluster(conf);
				NUnit.Framework.Assert.Fail("Cluster with classic Framework name should not use "
					 + "local JT address");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Cannot initialize Cluster. Please check"
					));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterWithYarnClientProvider()
		{
			Configuration conf = new Configuration();
			conf.Set(MRConfig.FrameworkName, "yarn");
			Cluster cluster = new Cluster(conf);
			NUnit.Framework.Assert.IsTrue(cluster.GetClient() is YARNRunner);
			cluster.Close();
		}

		[NUnit.Framework.Test]
		public virtual void TestClusterException()
		{
			Configuration conf = new Configuration();
			try
			{
				conf.Set(MRConfig.FrameworkName, "incorrect");
				new Cluster(conf);
				NUnit.Framework.Assert.Fail("Cluster should not be initialized with incorrect framework name"
					);
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Cannot initialize Cluster. Please check"
					));
			}
		}
	}
}
