using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>Tests XAttr APIs via WebHDFS.</summary>
	public class TestWebHDFSXAttr : FSXAttrBaseTest
	{
		/// <summary>Overridden to provide a WebHdfsFileSystem wrapper for the super-user.</summary>
		/// <returns>WebHdfsFileSystem for super-user</returns>
		/// <exception cref="System.Exception">if creation fails</exception>
		protected internal override FileSystem CreateFileSystem()
		{
			return WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem.Scheme);
		}
	}
}
