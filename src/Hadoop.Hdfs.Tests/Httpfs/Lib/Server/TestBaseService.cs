using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Server
{
	public class TestBaseService : HTestCase
	{
		public class MyService : BaseService
		{
			internal static bool Init;

			public MyService()
				: base("myservice")
			{
			}

			/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
			protected internal override void Init()
			{
				Init = true;
			}

			public override Type GetInterface()
			{
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void BaseService()
		{
			BaseService service = new TestBaseService.MyService();
			NUnit.Framework.Assert.IsNull(service.GetInterface());
			NUnit.Framework.Assert.AreEqual(service.GetPrefix(), "myservice");
			NUnit.Framework.Assert.AreEqual(service.GetServiceDependencies().Length, 0);
			Org.Apache.Hadoop.Lib.Server.Server server = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Lib.Server.Server
				>();
			Configuration conf = new Configuration(false);
			conf.Set("server.myservice.foo", "FOO");
			conf.Set("server.myservice1.bar", "BAR");
			Org.Mockito.Mockito.When(server.GetConfig()).ThenReturn(conf);
			Org.Mockito.Mockito.When(server.GetPrefixedName("myservice.foo")).ThenReturn("server.myservice.foo"
				);
			Org.Mockito.Mockito.When(server.GetPrefixedName("myservice.")).ThenReturn("server.myservice."
				);
			service.Init(server);
			NUnit.Framework.Assert.AreEqual(service.GetPrefixedName("foo"), "server.myservice.foo"
				);
			NUnit.Framework.Assert.AreEqual(service.GetServiceConfig().Size(), 1);
			NUnit.Framework.Assert.AreEqual(service.GetServiceConfig().Get("foo"), "FOO");
			NUnit.Framework.Assert.IsTrue(TestBaseService.MyService.Init);
		}
	}
}
