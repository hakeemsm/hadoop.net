using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Lib.Service.Instrumentation;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service.Scheduler
{
	public class TestSchedulerService : HTestCase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void Service()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Configuration conf = new Configuration(false);
			conf.Set("server.services", StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName)));
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
			NUnit.Framework.Assert.IsNotNull(server.Get<Org.Apache.Hadoop.Lib.Service.Scheduler
				>());
			server.Destroy();
		}
	}
}
