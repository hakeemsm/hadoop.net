using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestDatanodeLayoutUpgrade
	{
		private const string HadoopDatanodeDirTxt = "hadoop-datanode-dir.txt";

		private const string Hadoop24Datanode = "hadoop-24-datanode-dir.tgz";

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgradeToIdBasedLayout()
		{
			// Upgrade from LDir-based layout to block ID-based layout -- change described
			// in HDFS-6482
			TestDFSUpgradeFromImage upgrade = new TestDFSUpgradeFromImage();
			upgrade.UnpackStorage(Hadoop24Datanode, HadoopDatanodeDirTxt);
			Configuration conf = new Configuration(TestDFSUpgradeFromImage.upgradeConf);
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, Runtime.GetProperty("test.build.data"
				) + FilePath.separator + "dfs" + FilePath.separator + "data");
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Runtime.GetProperty("test.build.data"
				) + FilePath.separator + "dfs" + FilePath.separator + "name");
			upgrade.UpgradeAndVerify(new MiniDFSCluster.Builder(conf).NumDataNodes(1).ManageDataDfsDirs
				(false).ManageNameDfsDirs(false), null);
		}
	}
}
