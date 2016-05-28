using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Qjournal.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal
{
	public class TestMiniJournalCluster
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStartStop()
		{
			Configuration conf = new Configuration();
			MiniJournalCluster c = new MiniJournalCluster.Builder(conf).Build();
			try
			{
				URI uri = c.GetQuorumJournalURI("myjournal");
				string[] addrs = uri.GetAuthority().Split(";");
				NUnit.Framework.Assert.AreEqual(3, addrs.Length);
				JournalNode node = c.GetJournalNode(0);
				string dir = node.GetConf().Get(DFSConfigKeys.DfsJournalnodeEditsDirKey);
				NUnit.Framework.Assert.AreEqual(new FilePath(MiniDFSCluster.GetBaseDirectory() + 
					"journalnode-0").GetAbsolutePath(), dir);
			}
			finally
			{
				c.Shutdown();
			}
		}
	}
}
