using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Applications.Distributedshell
{
	public class TestDSAppMaster
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTimelineClientInDSAppMaster()
		{
			ApplicationMaster appMaster = new ApplicationMaster();
			appMaster.appSubmitterUgi = UserGroupInformation.CreateUserForTesting("foo", new 
				string[] { "bar" });
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			appMaster.StartTimelineClient(conf);
			NUnit.Framework.Assert.AreEqual(appMaster.appSubmitterUgi, ((TimelineClientImpl)appMaster
				.timelineClient).GetUgi());
		}
	}
}
