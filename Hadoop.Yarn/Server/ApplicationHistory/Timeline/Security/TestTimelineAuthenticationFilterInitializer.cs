using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Security
{
	public class TestTimelineAuthenticationFilterInitializer
	{
		[NUnit.Framework.Test]
		public virtual void TestProxyUserConfiguration()
		{
			FilterContainer container = Org.Mockito.Mockito.Mock<FilterContainer>();
			for (int i = 0; i < 3; ++i)
			{
				Configuration conf = new YarnConfiguration();
				switch (i)
				{
					case 0:
					{
						// hadoop.proxyuser prefix
						conf.Set("hadoop.proxyuser.foo.hosts", "*");
						conf.Set("hadoop.proxyuser.foo.users", "*");
						conf.Set("hadoop.proxyuser.foo.groups", "*");
						break;
					}

					case 1:
					{
						// yarn.timeline-service.http-authentication.proxyuser prefix
						conf.Set("yarn.timeline-service.http-authentication.proxyuser.foo.hosts", "*");
						conf.Set("yarn.timeline-service.http-authentication.proxyuser.foo.users", "*");
						conf.Set("yarn.timeline-service.http-authentication.proxyuser.foo.groups", "*");
						break;
					}

					case 2:
					{
						// hadoop.proxyuser prefix has been overwritten by
						// yarn.timeline-service.http-authentication.proxyuser prefix
						conf.Set("hadoop.proxyuser.foo.hosts", "bar");
						conf.Set("hadoop.proxyuser.foo.users", "bar");
						conf.Set("hadoop.proxyuser.foo.groups", "bar");
						conf.Set("yarn.timeline-service.http-authentication.proxyuser.foo.hosts", "*");
						conf.Set("yarn.timeline-service.http-authentication.proxyuser.foo.users", "*");
						conf.Set("yarn.timeline-service.http-authentication.proxyuser.foo.groups", "*");
						break;
					}

					default:
					{
						break;
					}
				}
				TimelineAuthenticationFilterInitializer initializer = new TimelineAuthenticationFilterInitializer
					();
				initializer.InitFilter(container, conf);
				NUnit.Framework.Assert.AreEqual("*", initializer.filterConfig["proxyuser.foo.hosts"
					]);
				NUnit.Framework.Assert.AreEqual("*", initializer.filterConfig["proxyuser.foo.users"
					]);
				NUnit.Framework.Assert.AreEqual("*", initializer.filterConfig["proxyuser.foo.groups"
					]);
			}
		}
	}
}
