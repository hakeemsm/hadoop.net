using System;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Inject;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	/// <summary>
	/// The servlet that dispatch request to various controllers
	/// according to the user defined routes in the router.
	/// </summary>
	[System.Serializable]
	public class Dispatcher : HttpServlet
	{
		private const long serialVersionUID = 1L;

		internal static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Webapp.Dispatcher
			));

		internal const string ErrorCookie = "last-error";

		internal const string StatusCookie = "last-status";

		[System.NonSerialized]
		private readonly Injector injector;

		[System.NonSerialized]
		private readonly Router router;

		[System.NonSerialized]
		private readonly WebApp webApp;

		private volatile bool devMode = false;

		[Com.Google.Inject.Inject]
		internal Dispatcher(WebApp webApp, Injector injector, Router router)
		{
			this.webApp = webApp;
			this.injector = injector;
			this.router = router;
		}

		protected override void DoOptions(HttpServletRequest req, HttpServletResponse res
			)
		{
			// for simplicity
			res.SetHeader("Allow", "GET, POST");
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void Service(HttpServletRequest req, HttpServletResponse res)
		{
			res.SetCharacterEncoding("UTF-8");
			string uri = HtmlQuoting.QuoteHtmlChars(req.GetRequestURI());
			if (uri == null)
			{
				uri = "/";
			}
			if (devMode && uri.Equals("/__stop"))
			{
				// quick hack to restart servers in dev mode without OS commands
				res.SetStatus(res.ScNoContent);
				Log.Info("dev mode restart requested");
				PrepareToExit();
				return;
			}
			// if they provide a redirectPath go there instead of going to
			// "/" so that filters can differentiate the webapps.
			if (uri.Equals("/"))
			{
				string redirectPath = webApp.GetRedirectPath();
				if (redirectPath != null && !redirectPath.IsEmpty())
				{
					res.SendRedirect(redirectPath);
					return;
				}
			}
			string method = req.GetMethod();
			if (method.Equals("OPTIONS"))
			{
				DoOptions(req, res);
				return;
			}
			if (method.Equals("TRACE"))
			{
				DoTrace(req, res);
				return;
			}
			if (method.Equals("HEAD"))
			{
				DoGet(req, res);
				// default to bad request
				return;
			}
			string pathInfo = req.GetPathInfo();
			if (pathInfo == null)
			{
				pathInfo = "/";
			}
			Controller.RequestContext rc = injector.GetInstance<Controller.RequestContext>();
			if (SetCookieParams(rc, req) > 0)
			{
				Cookie ec = rc.Cookies()[ErrorCookie];
				if (ec != null)
				{
					rc.SetStatus(System.Convert.ToInt32(rc.Cookies()[StatusCookie].GetValue()));
					RemoveErrorCookies(res, uri);
					rc.Set(Params.ErrorDetails, ec.GetValue());
					Render(typeof(ErrorPage));
					return;
				}
			}
			rc.prefix = webApp.Name();
			Router.Dest dest = null;
			try
			{
				dest = router.Resolve(method, pathInfo);
			}
			catch (WebAppException e)
			{
				rc.error = e;
				if (!e.Message.Contains("not found"))
				{
					rc.SetStatus(res.ScInternalServerError);
					Render(typeof(ErrorPage));
					return;
				}
			}
			if (dest == null)
			{
				rc.SetStatus(res.ScNotFound);
				Render(typeof(ErrorPage));
				return;
			}
			rc.devMode = devMode;
			SetMoreParams(rc, pathInfo, dest);
			Controller controller = injector.GetInstance(dest.controllerClass);
			try
			{
				// TODO: support args converted from /path/:arg1/...
				dest.action.Invoke(controller, (object[])null);
				if (!rc.rendered)
				{
					if (dest.defaultViewClass != null)
					{
						Render(dest.defaultViewClass);
					}
					else
					{
						if (rc.status == 200)
						{
							throw new InvalidOperationException("No view rendered for 200");
						}
					}
				}
			}
			catch (Exception e)
			{
				Log.Error("error handling URI: " + uri, e);
				// Page could be half rendered (but still not flushed). So redirect.
				RedirectToErrorPage(res, e, uri, devMode);
			}
		}

		public static void RedirectToErrorPage(HttpServletResponse res, Exception e, string
			 path, bool devMode)
		{
			string st = devMode ? ErrorPage.ToStackTrace(e, 1024 * 3) : "See logs for stack trace";
			// spec: min 4KB
			res.SetStatus(res.ScFound);
			Cookie cookie = new Cookie(StatusCookie, 500.ToString());
			cookie.SetPath(path);
			res.AddCookie(cookie);
			cookie = new Cookie(ErrorCookie, st);
			cookie.SetPath(path);
			res.AddCookie(cookie);
			res.SetHeader("Location", path);
		}

		public static void RemoveErrorCookies(HttpServletResponse res, string path)
		{
			RemoveCookie(res, ErrorCookie, path);
			RemoveCookie(res, StatusCookie, path);
		}

		public static void RemoveCookie(HttpServletResponse res, string name, string path
			)
		{
			Log.Debug("removing cookie {} on {}", name, path);
			Cookie c = new Cookie(name, string.Empty);
			c.SetMaxAge(0);
			c.SetPath(path);
			res.AddCookie(c);
		}

		private void Render(Type cls)
		{
			injector.GetInstance(cls).Render();
		}

		// /path/foo/bar with /path/:arg1/:arg2 will set {arg1=>foo, arg2=>bar}
		private void SetMoreParams(Controller.RequestContext rc, string pathInfo, Router.Dest
			 dest)
		{
			Preconditions.CheckState(pathInfo.StartsWith(dest.prefix), "prefix should match");
			if (dest.pathParams.Count == 0 || dest.prefix.Length == pathInfo.Length)
			{
				return;
			}
			string[] parts = Iterables.ToArray<string>(WebApp.pathSplitter.Split(Sharpen.Runtime.Substring
				(pathInfo, dest.prefix.Length)));
			Log.Debug("parts={}, params={}", parts, dest.pathParams);
			for (int i = 0; i < dest.pathParams.Count && i < parts.Length; ++i)
			{
				string key = dest.pathParams[i];
				if (key[0] == ':')
				{
					rc.MoreParams()[Sharpen.Runtime.Substring(key, 1)] = parts[i];
				}
			}
		}

		private int SetCookieParams(Controller.RequestContext rc, HttpServletRequest req)
		{
			Cookie[] cookies = req.GetCookies();
			if (cookies != null)
			{
				foreach (Cookie cookie in cookies)
				{
					rc.Cookies()[cookie.GetName()] = cookie;
				}
				return cookies.Length;
			}
			return 0;
		}

		public virtual void SetDevMode(bool choice)
		{
			devMode = choice;
		}

		private void PrepareToExit()
		{
			Preconditions.CheckState(devMode, "only in dev mode");
			new Timer("webapp exit", true).Schedule(new _TimerTask_235(this), 18);
		}

		private sealed class _TimerTask_235 : TimerTask
		{
			public _TimerTask_235(Dispatcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				Org.Apache.Hadoop.Yarn.Webapp.Dispatcher.Log.Info("WebAppp /{} exiting...", this.
					_enclosing.webApp.Name());
				this._enclosing.webApp.Stop();
				System.Environment.Exit(0);
			}

			private readonly Dispatcher _enclosing;
		}
		// FINDBUG: this is intended in dev mode
		// enough time for the last local request to complete
	}
}
