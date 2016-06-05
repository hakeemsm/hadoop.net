using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	public class TestViewFsURIs
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestURIEmptyPath()
		{
			Configuration conf = new Configuration();
			ConfigUtil.AddLink(conf, "/user", new URI("file://foo"));
			FileContext.GetFileContext(FsConstants.ViewfsUri, conf);
		}
	}
}
