using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Sharedcache
{
	public class TestSharedCacheUploadService
	{
		[NUnit.Framework.Test]
		public virtual void TestInitDisabled()
		{
			TestInit(false);
		}

		[NUnit.Framework.Test]
		public virtual void TestInitEnabled()
		{
			TestInit(true);
		}

		public virtual void TestInit(bool enabled)
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.SharedCacheEnabled, enabled);
			SharedCacheUploadService service = new SharedCacheUploadService();
			service.Init(conf);
			NUnit.Framework.Assert.AreSame(enabled, service.IsEnabled());
			service.Stop();
		}
	}
}
