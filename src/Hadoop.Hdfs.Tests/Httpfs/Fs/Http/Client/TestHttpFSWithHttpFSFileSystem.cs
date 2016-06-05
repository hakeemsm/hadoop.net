using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Client
{
	public class TestHttpFSWithHttpFSFileSystem : BaseTestHttpFSWith
	{
		public TestHttpFSWithHttpFSFileSystem(BaseTestHttpFSWith.Operation operation)
			: base(operation)
		{
		}

		protected internal override Type GetFileSystemClass()
		{
			return typeof(HttpFSFileSystem);
		}

		protected internal override Path GetProxiedFSTestDir()
		{
			return TestHdfsHelper.GetHdfsTestDir();
		}

		protected internal override string GetProxiedFSURI()
		{
			return TestHdfsHelper.GetHdfsConf().Get(CommonConfigurationKeysPublic.FsDefaultNameKey
				);
		}

		protected internal override Configuration GetProxiedFSConf()
		{
			return TestHdfsHelper.GetHdfsConf();
		}
	}
}
