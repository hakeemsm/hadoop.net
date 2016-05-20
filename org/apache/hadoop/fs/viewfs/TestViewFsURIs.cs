using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	public class TestViewFsURIs
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testURIEmptyPath()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/user", new java.net.URI("file://foo"
				));
			org.apache.hadoop.fs.FileContext.getFileContext(org.apache.hadoop.fs.FsConstants.
				VIEWFS_URI, conf);
		}
	}
}
