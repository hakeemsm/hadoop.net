using System.IO;
using Com.Google.Inject;
using Javax.Servlet.Http;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Test
{
	public class TestWebAppTests
	{
		internal static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestWebAppTests
			));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInstances()
		{
			Injector injector = WebAppTests.CreateMockInjector(this);
			HttpServletRequest req = injector.GetInstance<HttpServletRequest>();
			HttpServletResponse res = injector.GetInstance<HttpServletResponse>();
			string val = req.GetParameter("foo");
			PrintWriter @out = res.GetWriter();
			@out.WriteLine("Hello world!");
			LogInstances(req, res, @out);
			NUnit.Framework.Assert.AreSame(req, injector.GetInstance<HttpServletRequest>());
			NUnit.Framework.Assert.AreSame(res, injector.GetInstance<HttpServletResponse>());
			NUnit.Framework.Assert.AreSame(this, injector.GetInstance<TestWebAppTests>());
			Org.Mockito.Mockito.Verify(req).GetParameter("foo");
			Org.Mockito.Mockito.Verify(res).GetWriter();
			Org.Mockito.Mockito.Verify(@out).WriteLine("Hello world!");
		}

		internal interface Foo
		{
		}

		internal class Bar : TestWebAppTests.Foo
		{
		}

		internal class FooBar : TestWebAppTests.Bar
		{
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateInjector()
		{
			TestWebAppTests.Bar bar = new TestWebAppTests.Bar();
			Injector injector = WebAppTests.CreateMockInjector<TestWebAppTests.Foo>(bar);
			LogInstances(injector.GetInstance<HttpServletRequest>(), injector.GetInstance<HttpServletResponse
				>(), injector.GetInstance<HttpServletResponse>().GetWriter());
			NUnit.Framework.Assert.AreSame(bar, injector.GetInstance<TestWebAppTests.Foo>());
		}

		[NUnit.Framework.Test]
		public virtual void TestCreateInjector2()
		{
			TestWebAppTests.FooBar foobar = new TestWebAppTests.FooBar();
			TestWebAppTests.Bar bar = new TestWebAppTests.Bar();
			Injector injector = WebAppTests.CreateMockInjector<TestWebAppTests.Foo>(bar, new 
				_AbstractModule_77(foobar));
			NUnit.Framework.Assert.AreNotSame(bar, injector.GetInstance<TestWebAppTests.Bar>(
				));
			NUnit.Framework.Assert.AreSame(foobar, injector.GetInstance<TestWebAppTests.Bar>(
				));
		}

		private sealed class _AbstractModule_77 : AbstractModule
		{
			public _AbstractModule_77(TestWebAppTests.FooBar foobar)
			{
				this.foobar = foobar;
			}

			protected override void Configure()
			{
				this.Bind<TestWebAppTests.Bar>().ToInstance(foobar);
			}

			private readonly TestWebAppTests.FooBar foobar;
		}

		internal class ScopeTest
		{
		}

		[NUnit.Framework.Test]
		public virtual void TestRequestScope()
		{
			Injector injector = WebAppTests.CreateMockInjector(this);
			NUnit.Framework.Assert.AreSame(injector.GetInstance<TestWebAppTests.ScopeTest>(), 
				injector.GetInstance<TestWebAppTests.ScopeTest>());
		}

		private void LogInstances(HttpServletRequest req, HttpServletResponse res, PrintWriter
			 @out)
		{
			Log.Info("request: {}", req);
			Log.Info("response: {}", res);
			Log.Info("writer: {}", @out);
		}
	}
}
