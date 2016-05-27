using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestS3_LocalFileContextURI : FileContextURIBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			Configuration localConf = new Configuration();
			fc2 = FileContext.GetFileContext(localConf);
			Configuration s3conf = new Configuration();
			s3conf.Set(CommonConfigurationKeysPublic.FsDefaultNameDefault, s3conf.Get("test.fs.s3.name"
				));
			fc1 = FileContext.GetFileContext(s3conf);
		}
	}
}
