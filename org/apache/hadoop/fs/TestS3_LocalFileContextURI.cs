using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestS3_LocalFileContextURI : org.apache.hadoop.fs.FileContextURIBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			org.apache.hadoop.conf.Configuration localConf = new org.apache.hadoop.conf.Configuration
				();
			fc2 = org.apache.hadoop.fs.FileContext.getFileContext(localConf);
			org.apache.hadoop.conf.Configuration s3conf = new org.apache.hadoop.conf.Configuration
				();
			s3conf.set(org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT
				, s3conf.get("test.fs.s3.name"));
			fc1 = org.apache.hadoop.fs.FileContext.getFileContext(s3conf);
		}
	}
}
