using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Test.Framework;
using Javax.WS.RS.Core;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestRMWebServicesFairScheduler : JerseyTestBase
	{
		private static MockRM rm;

		private YarnConfiguration conf;

		private sealed class _ServletModule_49 : ServletModule
		{
			public _ServletModule_49(TestRMWebServicesFairScheduler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected override void ConfigureServlets()
			{
				this.Bind<JAXBContextResolver>();
				this.Bind<RMWebServices>();
				this.Bind<GenericExceptionHandler>();
				this._enclosing.conf = new YarnConfiguration();
				this._enclosing.conf.SetClass(YarnConfiguration.RmScheduler, typeof(FairScheduler
					), typeof(ResourceScheduler));
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesFairScheduler
					.rm = new MockRM(this._enclosing.conf);
				this.Bind<ResourceManager>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesFairScheduler
					.rm);
				this.Serve("/*").With(typeof(GuiceContainer));
			}

			private readonly TestRMWebServicesFairScheduler _enclosing;
		}

		private Injector injector;

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestRMWebServicesFairScheduler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMWebServicesFairScheduler _enclosing;
		}

		public TestRMWebServicesFairScheduler()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.yarn.server.resourcemanager.webapp"
				).ContextListenerClass(typeof(TestRMWebServicesFairScheduler.GuiceServletConfig)
				).FilterClass(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath
				("/").Build())
		{
			injector = Guice.CreateInjector(new _ServletModule_49(this));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterScheduler()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("scheduler"
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterScheduler(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterSchedulerSlash()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("scheduler/"
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterScheduler(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		private void VerifyClusterScheduler(JSONObject json)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject info = json.GetJSONObject("scheduler");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, info.Length());
			info = info.GetJSONObject("schedulerInfo");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, info.Length());
			JSONObject rootQueue = info.GetJSONObject("rootQueue");
			NUnit.Framework.Assert.AreEqual("root", rootQueue.GetString("queueName"));
		}
	}
}
