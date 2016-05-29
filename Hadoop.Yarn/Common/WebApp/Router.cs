using System;
using System.Collections.Generic;
using System.Reflection;
using System.Security;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	/// <summary>Manages path info to controller#action routing.</summary>
	internal class Router
	{
		internal static readonly Logger Log = LoggerFactory.GetLogger(typeof(Router));

		internal static readonly ImmutableList<string> EmptyList = ImmutableList.Of();

		internal static readonly CharMatcher Slash = CharMatcher.Is('/');

		internal static readonly Sharpen.Pattern controllerRe = Sharpen.Pattern.Compile("^/[A-Za-z_]\\w*(?:/.*)?"
			);

		internal class Dest
		{
			internal readonly string prefix;

			internal readonly ImmutableList<string> pathParams;

			internal readonly MethodInfo action;

			internal readonly Type controllerClass;

			internal Type defaultViewClass;

			internal readonly EnumSet<WebApp.HTTP> methods;

			internal Dest(string path, MethodInfo method, Type cls, IList<string> pathParams, 
				WebApp.HTTP httpMethod)
			{
				prefix = Preconditions.CheckNotNull(path);
				action = Preconditions.CheckNotNull(method);
				controllerClass = Preconditions.CheckNotNull(cls);
				this.pathParams = pathParams != null ? ImmutableList.CopyOf(pathParams) : EmptyList;
				methods = EnumSet.Of(httpMethod);
			}
		}

		internal Type hostClass;

		internal readonly SortedDictionary<string, Router.Dest> routes = Maps.NewTreeMap(
			);

		// starting point to look for default classes
		// path->dest
		/// <summary>Add a route to the router.</summary>
		/// <remarks>
		/// Add a route to the router.
		/// e.g., add(GET, "/foo/show", FooController.class, "show", [name...]);
		/// The name list is from /foo/show/:name/...
		/// </remarks>
		internal virtual Router.Dest Add(WebApp.HTTP httpMethod, string path, Type cls, string
			 action, IList<string> names)
		{
			lock (this)
			{
				Log.Debug("adding {}({})->{}#{}", new object[] { path, names, cls, action });
				Router.Dest dest = AddController(httpMethod, path, cls, action, names);
				AddDefaultView(dest);
				return dest;
			}
		}

		private Router.Dest AddController(WebApp.HTTP httpMethod, string path, Type cls, 
			string action, IList<string> names)
		{
			try
			{
				// Look for the method in all public methods declared in the class
				// or inherited by the class.
				// Note: this does not distinguish methods with the same signature
				// but different return types.
				// TODO: We may want to deal with methods that take parameters in the future
				MethodInfo method = cls.GetMethod(action, null);
				Router.Dest dest = routes[path];
				if (dest == null)
				{
					// avoid any runtime checks
					dest = new Router.Dest(path, method, cls, names, httpMethod);
					routes[path] = dest;
					return dest;
				}
				dest.methods.AddItem(httpMethod);
				return dest;
			}
			catch (MissingMethodException)
			{
				throw new WebAppException(action + "() not found in " + cls);
			}
			catch (SecurityException)
			{
				throw new WebAppException("Security exception thrown for " + action + "() in " + 
					cls);
			}
		}

		private void AddDefaultView(Router.Dest dest)
		{
			string controllerName = dest.controllerClass.Name;
			if (controllerName.EndsWith("Controller"))
			{
				controllerName = Sharpen.Runtime.Substring(controllerName, 0, controllerName.Length
					 - 10);
			}
			dest.defaultViewClass = Find<View>(dest.controllerClass.Assembly.GetName(), StringHelper.Join
				(controllerName + "View"));
		}

		internal virtual void SetHostClass(Type cls)
		{
			hostClass = cls;
		}

		/// <summary>Resolve a path to a destination.</summary>
		internal virtual Router.Dest Resolve(string httpMethod, string path)
		{
			lock (this)
			{
				WebApp.HTTP method = WebApp.HTTP.ValueOf(httpMethod);
				// can throw
				Router.Dest dest = LookupRoute(method, path);
				if (dest == null)
				{
					return ResolveDefault(method, path);
				}
				return dest;
			}
		}

		private Router.Dest LookupRoute(WebApp.HTTP method, string path)
		{
			string key = path;
			do
			{
				Router.Dest dest = routes[key];
				if (dest != null && MethodAllowed(method, dest))
				{
					if ((object)key == path)
					{
						// shut up warnings
						Log.Debug("exact match for {}: {}", key, dest.action);
						return dest;
					}
					else
					{
						if (IsGoodMatch(dest, path))
						{
							Log.Debug("prefix match2 for {}: {}", key, dest.action);
							return dest;
						}
					}
					return ResolveAction(method, dest, path);
				}
				KeyValuePair<string, Router.Dest> lower = routes.LowerEntry(key);
				if (lower == null)
				{
					return null;
				}
				dest = lower.Value;
				if (PrefixMatches(dest, path))
				{
					if (MethodAllowed(method, dest))
					{
						if (IsGoodMatch(dest, path))
						{
							Log.Debug("prefix match for {}: {}", lower.Key, dest.action);
							return dest;
						}
						return ResolveAction(method, dest, path);
					}
					// check other candidates
					int slashPos = key.LastIndexOf('/');
					key = slashPos > 0 ? Sharpen.Runtime.Substring(path, 0, slashPos) : "/";
				}
				else
				{
					key = "/";
				}
			}
			while (true);
		}

		internal static bool MethodAllowed(WebApp.HTTP method, Router.Dest dest)
		{
			// Accept all methods by default, unless explicity configured otherwise.
			return dest.methods.Contains(method) || (dest.methods.Count == 1 && dest.methods.
				Contains(WebApp.HTTP.Get));
		}

		internal static bool PrefixMatches(Router.Dest dest, string path)
		{
			Log.Debug("checking prefix {}{} for path: {}", new object[] { dest.prefix, dest.pathParams
				, path });
			if (!path.StartsWith(dest.prefix))
			{
				return false;
			}
			int prefixLen = dest.prefix.Length;
			if (prefixLen > 1 && path.Length > prefixLen && path[prefixLen] != '/')
			{
				return false;
			}
			// prefix is / or prefix is path or prefix/...
			return true;
		}

		internal static bool IsGoodMatch(Router.Dest dest, string path)
		{
			if (Slash.CountIn(dest.prefix) > 1)
			{
				return true;
			}
			// We want to match (/foo, :a) for /foo/bar/blah and (/, :a) for /123
			// but NOT / for /foo or (/, :a) for /foo or /foo/ because default route
			// (FooController#index) for /foo and /foo/ takes precedence.
			if (dest.prefix.Length == 1)
			{
				return dest.pathParams.Count > 0 && !MaybeController(path);
			}
			return dest.pathParams.Count > 0 || (path.EndsWith("/") && Slash.CountIn(path) ==
				 2);
		}

		// /foo should match /foo/
		internal static bool MaybeController(string path)
		{
			return controllerRe.Matcher(path).Matches();
		}

		// Assume /controller/action style path
		private Router.Dest ResolveDefault(WebApp.HTTP method, string path)
		{
			IList<string> parts = WebApp.ParseRoute(path);
			string controller = parts[WebApp.RController];
			string action = parts[WebApp.RAction];
			// NameController is encouraged default
			Type cls = Find<Controller>(StringHelper.Join(controller, "Controller"));
			if (cls == null)
			{
				cls = Find<Controller>(controller);
			}
			if (cls == null)
			{
				throw new WebAppException(StringHelper.Join(path, ": controller for ", controller
					, " not found"));
			}
			return Add(method, DefaultPrefix(controller, action), cls, action, null);
		}

		private string DefaultPrefix(string controller, string action)
		{
			if (controller.Equals("default") && action.Equals("index"))
			{
				return "/";
			}
			if (action.Equals("index"))
			{
				return StringHelper.Join('/', controller);
			}
			return StringHelper.Pjoin(string.Empty, controller, action);
		}

		private Type Find<T>(string cname)
		{
			System.Type cls = typeof(T);
			string pkg = hostClass.Assembly.GetName();
			return Find(cls, pkg, cname);
		}

		private Type Find<T>(string pkg, string cname)
		{
			System.Type cls = typeof(T);
			string name = StringUtils.Capitalize(cname);
			Type found = Load(cls, StringHelper.Djoin(pkg, name));
			if (found == null)
			{
				found = Load(cls, StringHelper.Djoin(pkg, "webapp", name));
			}
			if (found == null)
			{
				found = Load(cls, StringHelper.Join(hostClass.FullName, '$', name));
			}
			return found;
		}

		private Type Load<T>(string className)
		{
			System.Type cls = typeof(T);
			Log.Debug("trying: {}", className);
			try
			{
				Type found = Sharpen.Runtime.GetType(className);
				if (cls.IsAssignableFrom(found))
				{
					Log.Debug("found {}", className);
					return (Type)found;
				}
				Log.Warn("found a {} but it's not a {}", className, cls.FullName);
			}
			catch (TypeLoadException)
			{
			}
			// OK in this case.
			return null;
		}

		// Dest may contain a candidate controller
		private Router.Dest ResolveAction(WebApp.HTTP method, Router.Dest dest, string path
			)
		{
			if (dest.prefix.Length == 1)
			{
				return null;
			}
			Preconditions.CheckState(!IsGoodMatch(dest, path), dest.prefix);
			Preconditions.CheckState(Slash.CountIn(path) > 1, path);
			IList<string> parts = WebApp.ParseRoute(path);
			string controller = parts[WebApp.RController];
			string action = parts[WebApp.RAction];
			return Add(method, StringHelper.Pjoin(string.Empty, controller, action), dest.controllerClass
				, action, null);
		}
	}
}
