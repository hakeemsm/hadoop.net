using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestGetSplitHosts : TestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestGetSplitHosts()
		{
			int numBlocks = 3;
			int block1Size = 100;
			int block2Size = 150;
			int block3Size = 75;
			int fileSize = block1Size + block2Size + block3Size;
			int replicationFactor = 3;
			NetworkTopology clusterMap = new NetworkTopology();
			BlockLocation[] bs = new BlockLocation[numBlocks];
			string[] block1Hosts = new string[] { "host1", "host2", "host3" };
			string[] block1Names = new string[] { "host1:100", "host2:100", "host3:100" };
			string[] block1Racks = new string[] { "/rack1/", "/rack1/", "/rack2/" };
			string[] block1Paths = new string[replicationFactor];
			for (int i = 0; i < replicationFactor; i++)
			{
				block1Paths[i] = block1Racks[i] + block1Names[i];
			}
			bs[0] = new BlockLocation(block1Names, block1Hosts, block1Paths, 0, block1Size);
			string[] block2Hosts = new string[] { "host4", "host5", "host6" };
			string[] block2Names = new string[] { "host4:100", "host5:100", "host6:100" };
			string[] block2Racks = new string[] { "/rack2/", "/rack3/", "/rack3/" };
			string[] block2Paths = new string[replicationFactor];
			for (int i_1 = 0; i_1 < replicationFactor; i_1++)
			{
				block2Paths[i_1] = block2Racks[i_1] + block2Names[i_1];
			}
			bs[1] = new BlockLocation(block2Names, block2Hosts, block2Paths, block1Size, block2Size
				);
			string[] block3Hosts = new string[] { "host1", "host7", "host8" };
			string[] block3Names = new string[] { "host1:100", "host7:100", "host8:100" };
			string[] block3Racks = new string[] { "/rack1/", "/rack4/", "/rack4/" };
			string[] block3Paths = new string[replicationFactor];
			for (int i_2 = 0; i_2 < replicationFactor; i_2++)
			{
				block3Paths[i_2] = block3Racks[i_2] + block3Names[i_2];
			}
			bs[2] = new BlockLocation(block3Names, block3Hosts, block3Paths, block1Size + block2Size
				, block3Size);
			SequenceFileInputFormat<string, string> sif = new SequenceFileInputFormat<string, 
				string>();
			string[] hosts = sif.GetSplitHosts(bs, 0, fileSize, clusterMap);
			// Contributions By Racks are
			// Rack1   175       
			// Rack2   275       
			// Rack3   150       
			// So, Rack2 hosts, host4 and host 3 should be returned
			// even if their individual contribution is not the highest
			NUnit.Framework.Assert.IsTrue(hosts.Length == replicationFactor);
			NUnit.Framework.Assert.IsTrue(Sharpen.Runtime.EqualsIgnoreCase(hosts[0], "host4")
				);
			NUnit.Framework.Assert.IsTrue(Sharpen.Runtime.EqualsIgnoreCase(hosts[1], "host3")
				);
			NUnit.Framework.Assert.IsTrue(Sharpen.Runtime.EqualsIgnoreCase(hosts[2], "host1")
				);
			// Now Create the blocks without topology information
			bs[0] = new BlockLocation(block1Names, block1Hosts, 0, block1Size);
			bs[1] = new BlockLocation(block2Names, block2Hosts, block1Size, block2Size);
			bs[2] = new BlockLocation(block3Names, block3Hosts, block1Size + block2Size, block3Size
				);
			hosts = sif.GetSplitHosts(bs, 0, fileSize, clusterMap);
			// host1 makes the highest contribution among all hosts
			// So, that should be returned before others
			NUnit.Framework.Assert.IsTrue(hosts.Length == replicationFactor);
			NUnit.Framework.Assert.IsTrue(Sharpen.Runtime.EqualsIgnoreCase(hosts[0], "host1")
				);
		}
	}
}
