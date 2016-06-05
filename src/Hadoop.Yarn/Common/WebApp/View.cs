using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Inject;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	/// <summary>Base class for all views</summary>
	public abstract class View : Params
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Webapp.View
			));

		public class ViewContext
		{
			internal readonly Controller.RequestContext rc;

			internal int nestLevel = 0;

			internal bool wasInline;

			[Com.Google.Inject.Inject]
			internal ViewContext(Controller.RequestContext ctx)
			{
				rc = ctx;
			}

			public virtual int NestLevel()
			{
				return nestLevel;
			}

			public virtual bool WasInline()
			{
				return wasInline;
			}

			public virtual void Set(int nestLevel, bool wasInline)
			{
				this.nestLevel = nestLevel;
				this.wasInline = wasInline;
			}

			public virtual Controller.RequestContext RequestContext()
			{
				return rc;
			}
		}

		private View.ViewContext vc;

		[Com.Google.Inject.Inject]
		internal Injector injector;

		public View()
		{
		}

		public View(View.ViewContext ctx)
		{
			// Makes injection in subclasses optional.
			// Time will tell if this buy us more than the NPEs :)
			vc = ctx;
		}

		/// <summary>The API to render the view</summary>
		public abstract void Render();

		public virtual View.ViewContext Context()
		{
			if (vc == null)
			{
				if (injector == null)
				{
					// One downside of making the injection in subclasses optional
					throw new WebAppException(StringHelper.Join("Error accessing ViewContext from a\n"
						, "child constructor, either move the usage of the View methods\n", "out of the constructor or inject the ViewContext into the\n"
						, "constructor"));
				}
				vc = injector.GetInstance<View.ViewContext>();
			}
			return vc;
		}

		public virtual Exception Error()
		{
			return Context().rc.error;
		}

		public virtual int Status()
		{
			return Context().rc.status;
		}

		public virtual bool InDevMode()
		{
			return Context().rc.devMode;
		}

		public virtual Injector Injector()
		{
			return Context().rc.injector;
		}

		public virtual T GetInstance<T>()
		{
			System.Type cls = typeof(T);
			return Injector().GetInstance(cls);
		}

		public virtual HttpServletRequest Request()
		{
			return Context().rc.request;
		}

		public virtual HttpServletResponse Response()
		{
			return Context().rc.response;
		}

		public virtual IDictionary<string, string> MoreParams()
		{
			return Context().rc.MoreParams();
		}

		/// <summary>Get the cookies</summary>
		/// <returns>the cookies map</returns>
		public virtual IDictionary<string, Cookie> Cookies()
		{
			return Context().rc.Cookies();
		}

		public virtual ServletOutputStream OutputStream()
		{
			try
			{
				return Response().GetOutputStream();
			}
			catch (IOException e)
			{
				throw new WebAppException(e);
			}
		}

		public virtual PrintWriter Writer()
		{
			try
			{
				return Response().GetWriter();
			}
			catch (IOException e)
			{
				throw new WebAppException(e);
			}
		}

		/// <summary>Lookup a value from the current context.</summary>
		/// <param name="key">to lookup</param>
		/// <param name="defaultValue">if key is missing</param>
		/// <returns>the value of the key or the default value</returns>
		public virtual string $(string key, string defaultValue)
		{
			// moreParams take precedence
			string value = MoreParams()[key];
			if (value == null)
			{
				value = Request().GetParameter(key);
			}
			return value == null ? defaultValue : value;
		}

		/// <summary>Lookup a value from the current context</summary>
		/// <param name="key">to lookup</param>
		/// <returns>the value of the key or empty string</returns>
		public virtual string $(string key)
		{
			return $(key, string.Empty);
		}

		/// <summary>Set a context value.</summary>
		/// <remarks>
		/// Set a context value. (e.g. UI properties for sub views.)
		/// Try to avoid any application (vs view/ui) logic.
		/// </remarks>
		/// <param name="key">to set</param>
		/// <param name="value">to set</param>
		public virtual void Set(string key, string value)
		{
			MoreParams()[key] = value;
		}

		public virtual string Root()
		{
			string root = Runtime.Getenv(ApplicationConstants.ApplicationWebProxyBaseEnv);
			if (root == null || root.IsEmpty())
			{
				root = "/";
			}
			return root;
		}

		public virtual string Prefix()
		{
			if (Context().rc.prefix == null)
			{
				return Root();
			}
			else
			{
				return StringHelper.Ujoin(Root(), Context().rc.prefix);
			}
		}

		public virtual void SetTitle(string title)
		{
			Set(Title, title);
		}

		public virtual void SetTitle(string title, string url)
		{
			SetTitle(title);
			Set(TitleLink, url);
		}

		/// <summary>Create an url from url components</summary>
		/// <param name="parts">components to join</param>
		/// <returns>an url string</returns>
		public virtual string Root_url(params string[] parts)
		{
			return StringHelper.Ujoin(Root(), parts);
		}

		/// <summary>Create an url from url components</summary>
		/// <param name="parts">components to join</param>
		/// <returns>an url string</returns>
		public virtual string Url(params string[] parts)
		{
			return StringHelper.Ujoin(Prefix(), parts);
		}

		public virtual ResponseInfo Info(string about)
		{
			return GetInstance<ResponseInfo>().About(about);
		}

		/// <summary>Render a sub-view</summary>
		/// <param name="cls">the class of the sub-view</param>
		public virtual void Render(Type cls)
		{
			int saved = Context().nestLevel;
			GetInstance(cls).RenderPartial();
			if (Context().nestLevel != saved)
			{
				throw new WebAppException("View " + cls.Name + " not complete");
			}
		}
	}
}
