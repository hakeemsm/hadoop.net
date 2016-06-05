using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestHAFsck
	{
		static TestHAFsck()
		{
			((Log4JLogger)LogFactory.GetLog(typeof(DFSUtil))).GetLogger().SetLevel(Level.All);
		}

		/// <summary>Test that fsck still works with HA enabled.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHaFsck()
		{
			Configuration conf = new Configuration();
			// need some HTTP ports
			MiniDFSNNTopology topology = new MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf
				("ha-nn-uri-0").AddNN(new MiniDFSNNTopology.NNConf("nn1").SetHttpPort(10051)).AddNN
				(new MiniDFSNNTopology.NNConf("nn2").SetHttpPort(10052)));
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(topology).NumDataNodes
				(0).Build();
			FileSystem fs = null;
			try
			{
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				// Make sure conf has the relevant HA configs.
				HATestUtil.SetFailoverConfigurations(cluster, conf, "ha-nn-uri-0", 0);
				fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				fs.Mkdirs(new Path("/test1"));
				fs.Mkdirs(new Path("/test2"));
				RunFsck(conf);
				cluster.TransitionToStandby(0);
				cluster.TransitionToActive(1);
				RunFsck(conf);
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		internal static void RunFsck(Configuration conf)
		{
			ByteArrayOutputStream bStream = new ByteArrayOutputStream();
			TextWriter @out = new TextWriter(bStream, true);
			int errCode = ToolRunner.Run(new DFSck(conf, @out), new string[] { "/", "-files" }
				);
			string result = bStream.ToString();
			System.Console.Out.WriteLine("output from fsck:\n" + result);
			NUnit.Framework.Assert.AreEqual(0, errCode);
			NUnit.Framework.Assert.IsTrue(result.Contains("/test1"));
			NUnit.Framework.Assert.IsTrue(result.Contains("/test2"));
		}
	}
}
