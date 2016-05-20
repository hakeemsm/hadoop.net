using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestLocal_S3FileContextURI : org.apache.hadoop.fs.FileContextURIBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			org.apache.hadoop.conf.Configuration S3Conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.conf.Configuration localConf = new org.apache.hadoop.conf.Configuration
				();
			S3Conf.set(org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT
				, S3Conf.get("test.fs.s3.name"));
			fc1 = org.apache.hadoop.fs.FileContext.getFileContext(S3Conf);
			fc2 = org.apache.hadoop.fs.FileContext.getFileContext(localConf);
		}
	}
}
