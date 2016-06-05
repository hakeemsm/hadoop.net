using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Com.Google.Inject;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Org.Codehaus.Jackson.Map;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	public abstract class Controller : Params
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Webapp.Controller
			));

		internal static readonly ObjectMapper jsonMapper = new ObjectMapper();

		public class RequestContext
		{
			internal readonly Injector injector;

			internal readonly HttpServletRequest request;

			internal readonly HttpServletResponse response;

			private IDictionary<string, string> moreParams;

			private IDictionary<string, Cookie> cookies;

			internal int status = 200;

			internal bool rendered = false;

			internal Exception error;

			internal bool devMode = false;

			internal string prefix;

			[Com.Google.Inject.Inject]
			internal RequestContext(Injector injector, HttpServletRequest request, HttpServletResponse
				 response)
			{
				// pre 3.0 servlet-api doesn't have getStatus
				this.injector = injector;
				this.request = request;
				this.response = response;
			}

			public virtual int Status()
			{
				return status;
			}

			public virtual void SetStatus(int status)
			{
				this.status = status;
				response.SetStatus(status);
			}

			public virtual void SetRendered(bool rendered)
			{
				this.rendered = rendered;
			}

			public virtual IDictionary<string, string> MoreParams()
			{
				if (moreParams == null)
				{
					moreParams = Maps.NewHashMap();
				}
				return moreParams;
			}

			// OK
			public virtual IDictionary<string, Cookie> Cookies()
			{
				if (cookies == null)
				{
					cookies = Maps.NewHashMap();
					Cookie[] rcookies = request.GetCookies();
					if (rcookies != null)
					{
						foreach (Cookie cookie in rcookies)
						{
							cookies[cookie.GetName()] = cookie;
						}
					}
				}
				return cookies;
			}

			// OK
			public virtual void Set(string key, string value)
			{
				MoreParams()[key] = value;
			}

			public virtual string Get(string key, string defaultValue)
			{
				string value = MoreParams()[key];
				if (value == null)
				{
					value = request.GetParameter(key);
				}
				return value == null ? defaultValue : value;
			}

			public virtual string Prefix()
			{
				return prefix;
			}
		}

		private Controller.RequestContext context;

		[Com.Google.Inject.Inject]
		internal Injector injector;

		public Controller()
		{
		}

		public Controller(Controller.RequestContext ctx)
		{
			// Makes injection in subclasses optional.
			// Time will tell if this buy us more than the NPEs :)
			context = ctx;
		}

		public virtual Controller.RequestContext Context()
		{
			if (context == null)
			{
				if (injector == null)
				{
					// One of the downsides of making injection in subclasses optional.
					throw new WebAppException(StringHelper.Join("Error accessing RequestContext from\n"
						, "a child constructor, either move the usage of the Controller\n", "methods out of the constructor or inject the RequestContext\n"
						, "into the constructor"));
				}
				context = injector.GetInstance<Controller.RequestContext>();
			}
			return context;
		}

		public virtual Exception Error()
		{
			return Context().error;
		}

		public virtual int Status()
		{
			return Context().status;
		}

		public virtual void SetStatus(int status)
		{
			Context().SetStatus(status);
		}

		public virtual bool InDevMode()
		{
			return Context().devMode;
		}

		public virtual Injector Injector()
		{
			return Context().injector;
		}

		public virtual T GetInstance<T>()
		{
			System.Type cls = typeof(T);
			return injector.GetInstance(cls);
		}

		public virtual HttpServletRequest Request()
		{
			return Context().request;
		}

		public virtual HttpServletResponse Response()
		{
			return Context().response;
		}

		public virtual void Set(string key, string value)
		{
			Context().Set(key, value);
		}

		public virtual string Get(string key, string defaultValue)
		{
			return Context().Get(key, defaultValue);
		}

		public virtual string $(string key)
		{
			return Get(key, string.Empty);
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

		public virtual ResponseInfo Info(string about)
		{
			return GetInstance<ResponseInfo>().About(about);
		}

		/// <summary>Get the cookies</summary>
		/// <returns>the cookies map</returns>
		public virtual IDictionary<string, Cookie> Cookies()
		{
			return Context().Cookies();
		}

		/// <summary>Create an url from url components</summary>
		/// <param name="parts">components to join</param>
		/// <returns>an url string</returns>
		public virtual string Url(params string[] parts)
		{
			return StringHelper.Ujoin(Context().prefix, parts);
		}

		/// <summary>The default action.</summary>
		public abstract void Index();

		public virtual void Echo()
		{
			Render(typeof(DefaultPage));
		}

		protected internal virtual void Render(Type cls)
		{
			Context().rendered = true;
			GetInstance(cls).Render();
		}

		/// <summary>Convenience method for REST APIs (without explicit views)</summary>
		/// <param name="object">- the object as the response (in JSON)</param>
		protected internal virtual void RenderJSON(object @object)
		{
			Log.Debug("{}: {}", MimeType.Json, @object);
			Context().rendered = true;
			Context().response.SetContentType(MimeType.Json);
			try
			{
				jsonMapper.WriteValue(Writer(), @object);
			}
			catch (Exception e)
			{
				throw new WebAppException(e);
			}
		}

		protected internal virtual void RenderJSON(Type cls)
		{
			Context().rendered = true;
			Response().SetContentType(MimeType.Json);
			GetInstance(cls).ToJSON(Writer());
		}

		/// <summary>Convenience method for hello world :)</summary>
		/// <param name="s">- the content to render as plain text</param>
		protected internal virtual void RenderText(string s)
		{
			Log.Debug("{}: {}", MimeType.Text, s);
			Context().rendered = true;
			Response().SetContentType(MimeType.Text);
			Writer().Write(s);
		}

		protected internal virtual PrintWriter Writer()
		{
			try
			{
				return Response().GetWriter();
			}
			catch (Exception e)
			{
				throw new WebAppException(e);
			}
		}
	}
}
