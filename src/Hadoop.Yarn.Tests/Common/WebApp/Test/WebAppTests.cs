using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Test
{
	public class WebAppTests
	{
		/// <summary>Create a mock injector for tests</summary>
		/// <?/>
		/// <param name="api">the interface class of the object to inject</param>
		/// <param name="impl">the implementation object to inject</param>
		/// <param name="modules">additional guice modules</param>
		/// <returns>an injector</returns>
		public static Injector CreateMockInjector<T>(T impl, params Module[] modules)
		{
			System.Type api = typeof(T);
			return Guice.CreateInjector(new _AbstractModule_59(api, impl, modules));
		}

		private sealed class _AbstractModule_59 : AbstractModule
		{
			public _AbstractModule_59(Type api, T impl, Module[] modules)
			{
				this.api = api;
				this.impl = impl;
				this.modules = modules;
				this.writer = Org.Mockito.Mockito.Spy(new PrintWriter(System.Console.Out));
				this.request = this.CreateRequest();
				this.response = this.CreateResponse();
			}

			internal readonly PrintWriter writer;

			internal readonly HttpServletRequest request;

			internal readonly HttpServletResponse response;

			protected override void Configure()
			{
				if (api != null)
				{
					this.Bind(api).ToInstance(impl);
				}
				this.BindScope(typeof(RequestScoped), Scopes.Singleton);
				if (modules != null)
				{
					foreach (Module module in modules)
					{
						this.Install(module);
					}
				}
			}

			[Provides]
			internal HttpServletRequest Request()
			{
				return this.request;
			}

			[Provides]
			internal HttpServletResponse Response()
			{
				return this.response;
			}

			[Provides]
			internal PrintWriter Writer()
			{
				return this.writer;
			}

			internal HttpServletRequest CreateRequest()
			{
				// the default suffices for now
				return Org.Mockito.Mockito.Mock<HttpServletRequest>();
			}

			internal HttpServletResponse CreateResponse()
			{
				try
				{
					HttpServletResponse res = Org.Mockito.Mockito.Mock<HttpServletResponse>();
					Org.Mockito.Mockito.When(res.GetWriter()).ThenReturn(this.writer);
					return res;
				}
				catch (Exception e)
				{
					throw new WebAppException(e);
				}
			}

			private readonly Type api;

			private readonly T impl;

			private readonly Module[] modules;
		}

		// convenience
		public static Injector CreateMockInjector<T>(T impl)
		{
			return CreateMockInjector((Type)impl.GetType(), impl);
		}

		public static void FlushOutput(Injector injector)
		{
			HttpServletResponse res = injector.GetInstance<HttpServletResponse>();
			try
			{
				res.GetWriter().Flush();
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}

		public static Injector TestController<T>(Type ctrlr, string methodName, T impl, params 
			Module[] modules)
		{
			System.Type api = typeof(T);
			try
			{
				Injector injector = CreateMockInjector(api, impl, modules);
				MethodInfo method = ctrlr.GetMethod(methodName, (Type[])null);
				method.Invoke(injector.GetInstance(ctrlr), (object[])null);
				return injector;
			}
			catch (Exception e)
			{
				throw new WebAppException(e);
			}
		}

		public static Injector TestController<T>(Type ctrlr, string methodName)
		{
			return TestController(ctrlr, methodName, null, null);
		}

		public static Injector TestPage<T>(Type page, T impl, IDictionary<string, string>
			 @params, params Module[] modules)
		{
			System.Type api = typeof(T);
			Injector injector = CreateMockInjector(api, impl, modules);
			View view = injector.GetInstance(page);
			if (@params != null)
			{
				foreach (KeyValuePair<string, string> entry in @params)
				{
					view.Set(entry.Key, entry.Value);
				}
			}
			view.Render();
			FlushOutput(injector);
			return injector;
		}

		public static Injector TestPage<T>(Type page, T impl, params Module[] modules)
		{
			System.Type api = typeof(T);
			return TestPage(page, api, impl, null, modules);
		}

		// convenience
		public static Injector TestPage<T>(Type page)
		{
			return TestPage(page, null, null);
		}

		public static Injector TestBlock<T>(Type block, T impl, params Module[] modules)
		{
			System.Type api = typeof(T);
			Injector injector = CreateMockInjector(api, impl, modules);
			injector.GetInstance(block).RenderPartial();
			FlushOutput(injector);
			return injector;
		}

		// convenience
		public static Injector TestBlock<T>(Type block)
		{
			return TestBlock(block, null, null);
		}

		/// <summary>Convenience method to get the spy writer.</summary>
		/// <param name="injector">the injector used for the test.</param>
		/// <returns>The Spy writer.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static PrintWriter GetPrintWriter(Injector injector)
		{
			HttpServletResponse res = injector.GetInstance<HttpServletResponse>();
			return res.GetWriter();
		}
	}
}
