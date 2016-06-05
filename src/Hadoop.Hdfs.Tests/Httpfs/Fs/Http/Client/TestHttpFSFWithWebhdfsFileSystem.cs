using System;
using Org.Apache.Hadoop.Hdfs.Web;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Client
{
	public class TestHttpFSFWithWebhdfsFileSystem : TestHttpFSWithHttpFSFileSystem
	{
		public TestHttpFSFWithWebhdfsFileSystem(BaseTestHttpFSWith.Operation operation)
			: base(operation)
		{
		}

		protected internal override Type GetFileSystemClass()
		{
			return typeof(WebHdfsFileSystem);
		}
	}
}
