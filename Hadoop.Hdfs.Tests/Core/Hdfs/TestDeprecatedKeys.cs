using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestDeprecatedKeys
	{
		//Tests a deprecated key
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeprecatedKeys()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set("topology.script.file.name", "xyz");
			string scriptFile = conf.Get(DFSConfigKeys.NetTopologyScriptFileNameKey);
			NUnit.Framework.Assert.IsTrue(scriptFile.Equals("xyz"));
			conf.SetInt("dfs.replication.interval", 1);
			string alpha = DFSConfigKeys.DfsNamenodeReplicationIntervalKey;
			int repInterval = conf.GetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 3);
			NUnit.Framework.Assert.IsTrue(repInterval == 1);
		}
	}
}
