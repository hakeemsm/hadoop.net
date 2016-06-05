using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Tests NameNode interaction for all ACL modification APIs.</summary>
	/// <remarks>
	/// Tests NameNode interaction for all ACL modification APIs.  This test suite
	/// also covers interaction of setPermission with inodes that have ACLs.
	/// </remarks>
	public class TestNameNodeAcl : FSAclBaseTest
	{
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Init()
		{
			conf = new Configuration();
			StartCluster();
		}
	}
}
