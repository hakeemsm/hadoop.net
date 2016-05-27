using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestLocal_S3FileContextURI : FileContextURIBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			Configuration S3Conf = new Configuration();
			Configuration localConf = new Configuration();
			S3Conf.Set(CommonConfigurationKeysPublic.FsDefaultNameDefault, S3Conf.Get("test.fs.s3.name"
				));
			fc1 = FileContext.GetFileContext(S3Conf);
			fc2 = FileContext.GetFileContext(localConf);
		}
	}
}
