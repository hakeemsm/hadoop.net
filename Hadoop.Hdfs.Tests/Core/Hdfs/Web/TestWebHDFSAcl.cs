using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>Tests ACL APIs via WebHDFS.</summary>
	public class TestWebHDFSAcl : FSAclBaseTest
	{
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Init()
		{
			conf = WebHdfsTestUtil.CreateConf();
			StartCluster();
		}

		/// <summary>
		/// We need to skip this test on WebHDFS, because WebHDFS currently cannot
		/// resolve symlinks.
		/// </summary>
		[NUnit.Framework.Test]
		[Ignore]
		public override void TestDefaultAclNewSymlinkIntermediate()
		{
		}

		/// <summary>Overridden to provide a WebHdfsFileSystem wrapper for the super-user.</summary>
		/// <returns>WebHdfsFileSystem for super-user</returns>
		/// <exception cref="System.Exception">if creation fails</exception>
		protected internal override FileSystem CreateFileSystem()
		{
			return WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem.Scheme);
		}

		/// <summary>Overridden to provide a WebHdfsFileSystem wrapper for a specific user.</summary>
		/// <param name="user">UserGroupInformation specific user</param>
		/// <returns>WebHdfsFileSystem for specific user</returns>
		/// <exception cref="System.Exception">if creation fails</exception>
		protected internal override FileSystem CreateFileSystem(UserGroupInformation user
			)
		{
			return WebHdfsTestUtil.GetWebHdfsFileSystemAs(user, conf, WebHdfsFileSystem.Scheme
				);
		}
	}
}
