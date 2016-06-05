using System;
using System.IO;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	public class TestWebApp
	{
		internal static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestWebApp));

		internal class FooController : Controller
		{
			internal readonly TestWebApp test;

			[Com.Google.Inject.Inject]
			internal FooController(TestWebApp test)
			{
				this.test = test;
			}

			public override void Index()
			{
				Set("key", test.Echo("foo"));
			}

			public virtual void Bar()
			{
				Set("key", "bar");
			}

			public virtual void Names()
			{
				for (int i = 0; i < 20; ++i)
				{
					RenderText(MockApps.NewAppName() + "\n");
				}
			}

			public virtual void Ex()
			{
				bool err = $("clear").IsEmpty();
				RenderText(err ? "Should redirect to an error page." : "No error!");
				if (err)
				{
					throw new RuntimeException("exception test");
				}
			}

			public virtual void Tables()
			{
				Render(typeof(TestWebApp.TablesView));
			}
		}

		internal class FooView : TextPage
		{
			public override void Render()
			{
				Puts($("key"), $("foo"));
			}
		}

		internal class DefaultController : Controller
		{
			public override void Index()
			{
				Set("key", "default");
				Render(typeof(TestWebApp.FooView));
			}
		}

		internal class TablesView : HtmlPage
		{
			protected internal override void Render(Hamlet.HTML<HtmlPage._> html)
			{
				Set(JQueryUI.DatatablesId, "t1 t2 t3 t4");
				Set(JQueryUI.InitID(JQueryUI.Datatables, "t1"), JQueryUI.TableInit().Append("}").
					ToString());
				Set(JQueryUI.InitID(JQueryUI.Datatables, "t2"), StringHelper.Join("{bJQueryUI:true, sDom:'t',"
					, "aoColumns:[null, {bSortable:false, bSearchable:false}]}"));
				Set(JQueryUI.InitID(JQueryUI.Datatables, "t3"), "{bJQueryUI:true, sDom:'t'}");
				Set(JQueryUI.InitID(JQueryUI.Datatables, "t4"), "{bJQueryUI:true, sDom:'t'}");
				html.Title("Test DataTables").Link("/static/yarn.css").(typeof(JQueryUI)).Style(".wrapper { padding: 1em }"
					, ".wrapper h2 { margin: 0.5em 0 }", ".dataTables_wrapper { min-height: 1em }").
					Div(".wrapper").H2("Default table init").Table("#t1").Thead().Tr().Th("Column1")
					.Th("Column2").().().Tbody().Tr().Td("c1r1").Td("c2r1").().Tr().Td("c1r2").Td("c2r2"
					).().().().H2("Nested tables").Div(JQueryUI.InfoWrap).Table("#t2").Thead().Tr().
					Th(JQueryUI.Th, "Column1").Th(JQueryUI.Th, "Column2").().().Tbody().Tr().Td("r1"
					).Td().$class(JQueryUI.CTable).Table("#t3").Thead().Tr().Th("SubColumn1").Th("SubColumn2"
					).().().Tbody().Tr().Td("subc1r1").Td("subc2r1").().Tr().Td("subc1r2").Td("subc2r2"
					).().().().().().Tr().Td("r2").Td().$class(JQueryUI.CTable).Table("#t4").Thead()
					.Tr().Th("SubColumn1").Th("SubColumn2").().().Tbody().Tr().Td("subc1r1").Td("subc2r1"
					).().Tr().Td("subc1r2").Td("subc2r2").().().().().().().().().().();
			}
			// th wouldn't work as of dt 1.7.5
			// ditto
		}

		internal virtual string Echo(string s)
		{
			return s;
		}

		[NUnit.Framework.Test]
		public virtual void TestCreate()
		{
			WebApp app = WebApps.$for(this).Start();
			app.Stop();
		}

		[NUnit.Framework.Test]
		public virtual void TestCreateWithPort()
		{
			// see if the ephemeral port is updated
			WebApp app = WebApps.$for(this).At(0).Start();
			int port = app.GetListenerAddress().Port;
			NUnit.Framework.Assert.IsTrue(port > 0);
			app.Stop();
			// try to reuse the port
			app = WebApps.$for(this).At(port).Start();
			NUnit.Framework.Assert.AreEqual(port, app.GetListenerAddress().Port);
			app.Stop();
		}

		public virtual void TestCreateWithBindAddressNonZeroPort()
		{
			WebApp app = WebApps.$for(this).At("0.0.0.0:50000").Start();
			int port = app.GetListenerAddress().Port;
			NUnit.Framework.Assert.AreEqual(50000, port);
			// start another WebApp with same NonZero port
			WebApp app2 = WebApps.$for(this).At("0.0.0.0:50000").Start();
			// An exception occurs (findPort disabled)
			app.Stop();
			app2.Stop();
		}

		public virtual void TestCreateWithNonZeroPort()
		{
			WebApp app = WebApps.$for(this).At(50000).Start();
			int port = app.GetListenerAddress().Port;
			NUnit.Framework.Assert.AreEqual(50000, port);
			// start another WebApp with same NonZero port
			WebApp app2 = WebApps.$for(this).At(50000).Start();
			// An exception occurs (findPort disabled)
			app.Stop();
			app2.Stop();
		}

		[NUnit.Framework.Test]
		public virtual void TestServePaths()
		{
			WebApp app = WebApps.$for("test", this).Start();
			NUnit.Framework.Assert.AreEqual("/test", app.GetRedirectPath());
			string[] expectedPaths = new string[] { "/test", "/test/*" };
			string[] pathSpecs = app.GetServePathSpecs();
			NUnit.Framework.Assert.AreEqual(2, pathSpecs.Length);
			for (int i = 0; i < expectedPaths.Length; i++)
			{
				NUnit.Framework.Assert.IsTrue(ArrayUtils.Contains(pathSpecs, expectedPaths[i]));
			}
			app.Stop();
		}

		[NUnit.Framework.Test]
		public virtual void TestServePathsNoName()
		{
			WebApp app = WebApps.$for(string.Empty, this).Start();
			NUnit.Framework.Assert.AreEqual("/", app.GetRedirectPath());
			string[] expectedPaths = new string[] { "/*" };
			string[] pathSpecs = app.GetServePathSpecs();
			NUnit.Framework.Assert.AreEqual(1, pathSpecs.Length);
			for (int i = 0; i < expectedPaths.Length; i++)
			{
				NUnit.Framework.Assert.IsTrue(ArrayUtils.Contains(pathSpecs, expectedPaths[i]));
			}
			app.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultRoutes()
		{
			WebApp app = WebApps.$for("test", this).Start();
			string baseUrl = BaseUrl(app);
			try
			{
				NUnit.Framework.Assert.AreEqual("foo", GetContent(baseUrl + "test/foo").Trim());
				NUnit.Framework.Assert.AreEqual("foo", GetContent(baseUrl + "test/foo/index").Trim
					());
				NUnit.Framework.Assert.AreEqual("bar", GetContent(baseUrl + "test/foo/bar").Trim(
					));
				NUnit.Framework.Assert.AreEqual("default", GetContent(baseUrl + "test").Trim());
				NUnit.Framework.Assert.AreEqual("default", GetContent(baseUrl + "test/").Trim());
				NUnit.Framework.Assert.AreEqual("default", GetContent(baseUrl).Trim());
			}
			finally
			{
				app.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCustomRoutes()
		{
			WebApp app = WebApps.$for<TestWebApp>("test", this, "ws").Start(new _WebApp_234()
				);
			string baseUrl = BaseUrl(app);
			try
			{
				NUnit.Framework.Assert.AreEqual("foo", GetContent(baseUrl).Trim());
				NUnit.Framework.Assert.AreEqual("foo", GetContent(baseUrl + "test").Trim());
				NUnit.Framework.Assert.AreEqual("foo1", GetContent(baseUrl + "test/1").Trim());
				NUnit.Framework.Assert.AreEqual("bar", GetContent(baseUrl + "test/bar/foo").Trim(
					));
				NUnit.Framework.Assert.AreEqual("default", GetContent(baseUrl + "test/foo/bar").Trim
					());
				NUnit.Framework.Assert.AreEqual("default1", GetContent(baseUrl + "test/foo/1").Trim
					());
				NUnit.Framework.Assert.AreEqual("default2", GetContent(baseUrl + "test/foo/bar/2"
					).Trim());
				NUnit.Framework.Assert.AreEqual(404, GetResponseCode(baseUrl + "test/goo"));
				NUnit.Framework.Assert.AreEqual(200, GetResponseCode(baseUrl + "ws/v1/test"));
				NUnit.Framework.Assert.IsTrue(GetContent(baseUrl + "ws/v1/test").Contains("myInfo"
					));
			}
			finally
			{
				app.Stop();
			}
		}

		private sealed class _WebApp_234 : WebApp
		{
			public _WebApp_234()
			{
			}

			public override void Setup()
			{
				this.Bind<MyTestJAXBContextResolver>();
				this.Bind<MyTestWebService>();
				this.Route("/:foo", typeof(TestWebApp.FooController));
				this.Route("/bar/foo", typeof(TestWebApp.FooController), "bar");
				this.Route("/foo/:foo", typeof(TestWebApp.DefaultController));
				this.Route("/foo/bar/:foo", typeof(TestWebApp.DefaultController), "index");
			}
		}

		// This is to test the GuiceFilter should only be applied to webAppContext,
		// not to staticContext  and logContext;
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestYARNWebAppContext()
		{
			// setting up the log context
			Runtime.SetProperty("hadoop.log.dir", "/Not/Existing/dir");
			WebApp app = WebApps.$for("test", this).Start(new _WebApp_268());
			string baseUrl = BaseUrl(app);
			try
			{
				// should not redirect to foo
				NUnit.Framework.Assert.IsFalse("foo".Equals(GetContent(baseUrl + "static").Trim()
					));
				// Not able to access a non-existing dir, should not redirect to foo.
				NUnit.Framework.Assert.AreEqual(404, GetResponseCode(baseUrl + "logs"));
				// should be able to redirect to foo.
				NUnit.Framework.Assert.AreEqual("foo", GetContent(baseUrl).Trim());
			}
			finally
			{
				app.Stop();
			}
		}

		private sealed class _WebApp_268 : WebApp
		{
			public _WebApp_268()
			{
			}

			public override void Setup()
			{
				this.Route("/", typeof(TestWebApp.FooController));
			}
		}

		internal static string BaseUrl(WebApp app)
		{
			return "http://localhost:" + app.Port() + "/";
		}

		internal static string GetContent(string url)
		{
			try
			{
				StringBuilder @out = new StringBuilder();
				InputStream @in = new Uri(url).OpenConnection().GetInputStream();
				byte[] buffer = new byte[64 * 1024];
				int len = @in.Read(buffer);
				while (len > 0)
				{
					@out.Append(Sharpen.Runtime.GetStringForBytes(buffer, 0, len));
					len = @in.Read(buffer);
				}
				return @out.ToString();
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}

		internal static int GetResponseCode(string url)
		{
			try
			{
				HttpURLConnection c = (HttpURLConnection)new Uri(url).OpenConnection();
				return c.GetResponseCode();
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			// For manual controller/view testing.
			WebApps.$for("test", new TestWebApp()).At(8888).InDevMode().Start().JoinThread();
		}
		//        start(new WebApp() {
		//          @Override public void setup() {
		//            route("/:foo", FooController.class);
		//            route("/foo/:foo", FooController.class);
		//            route("/bar", FooController.class);
		//          }
		//        }).join();
	}
}
