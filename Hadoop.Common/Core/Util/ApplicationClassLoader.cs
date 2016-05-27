using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// A
	/// <see cref="Sharpen.URLClassLoader"/>
	/// for application isolation. Classes from the
	/// application JARs are loaded in preference to the parent loader.
	/// </summary>
	public class ApplicationClassLoader : URLClassLoader
	{
		/// <summary>Default value of the system classes if the user did not override them.</summary>
		/// <remarks>
		/// Default value of the system classes if the user did not override them.
		/// JDK classes, hadoop classes and resources, and some select third-party
		/// classes are considered system classes, and are not loaded by the
		/// application classloader.
		/// </remarks>
		public static readonly string SystemClassesDefault;

		private const string PropertiesFile = "org.apache.hadoop.application-classloader.properties";

		private const string SystemClassesDefaultKey = "system.classes.default";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Util.ApplicationClassLoader
			).FullName);

		private sealed class _FilenameFilter_62 : FilenameFilter
		{
			public _FilenameFilter_62()
			{
			}

			public bool Accept(FilePath dir, string name)
			{
				return name.EndsWith(".jar") || name.EndsWith(".JAR");
			}
		}

		private static readonly FilenameFilter JarFilenameFilter = new _FilenameFilter_62
			();

		static ApplicationClassLoader()
		{
			try
			{
				using (InputStream @is = typeof(Org.Apache.Hadoop.Util.ApplicationClassLoader).GetClassLoader
					().GetResourceAsStream(PropertiesFile))
				{
					if (@is == null)
					{
						throw new ExceptionInInitializerError("properties file " + PropertiesFile + " is not found"
							);
					}
					Properties props = new Properties();
					props.Load(@is);
					// get the system classes default
					string systemClassesDefault = props.GetProperty(SystemClassesDefaultKey);
					if (systemClassesDefault == null)
					{
						throw new ExceptionInInitializerError("property " + SystemClassesDefaultKey + " is not found"
							);
					}
					SystemClassesDefault = systemClassesDefault;
				}
			}
			catch (IOException e)
			{
				throw new ExceptionInInitializerError(e);
			}
		}

		private readonly ClassLoader parent;

		private readonly IList<string> systemClasses;

		public ApplicationClassLoader(Uri[] urls, ClassLoader parent, IList<string> systemClasses
			)
			: base(urls, parent)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("urls: " + Arrays.ToString(urls));
				Log.Debug("system classes: " + systemClasses);
			}
			this.parent = parent;
			if (parent == null)
			{
				throw new ArgumentException("No parent classloader!");
			}
			// if the caller-specified system classes are null or empty, use the default
			this.systemClasses = (systemClasses == null || systemClasses.IsEmpty()) ? Arrays.
				AsList(StringUtils.GetTrimmedStrings(SystemClassesDefault)) : systemClasses;
			Log.Info("system classes: " + this.systemClasses);
		}

		/// <exception cref="System.UriFormatException"/>
		public ApplicationClassLoader(string classpath, ClassLoader parent, IList<string>
			 systemClasses)
			: this(ConstructUrlsFromClasspath(classpath), parent, systemClasses)
		{
		}

		/// <exception cref="System.UriFormatException"/>
		internal static Uri[] ConstructUrlsFromClasspath(string classpath)
		{
			IList<Uri> urls = new AList<Uri>();
			foreach (string element in classpath.Split(FilePath.pathSeparator))
			{
				if (element.EndsWith("/*"))
				{
					string dir = Sharpen.Runtime.Substring(element, 0, element.Length - 1);
					FilePath[] files = new FilePath(dir).ListFiles(JarFilenameFilter);
					if (files != null)
					{
						foreach (FilePath file in files)
						{
							urls.AddItem(file.ToURI().ToURL());
						}
					}
				}
				else
				{
					FilePath file = new FilePath(element);
					if (file.Exists())
					{
						urls.AddItem(new FilePath(element).ToURI().ToURL());
					}
				}
			}
			return Sharpen.Collections.ToArray(urls, new Uri[urls.Count]);
		}

		public override Uri GetResource(string name)
		{
			Uri url = null;
			if (!IsSystemClass(name, systemClasses))
			{
				url = FindResource(name);
				if (url == null && name.StartsWith("/"))
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Remove leading / off " + name);
					}
					url = FindResource(Sharpen.Runtime.Substring(name, 1));
				}
			}
			if (url == null)
			{
				url = parent.GetResource(name);
			}
			if (url != null)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("getResource(" + name + ")=" + url);
				}
			}
			return url;
		}

		/// <exception cref="System.TypeLoadException"/>
		public override Type LoadClass(string name)
		{
			return this.LoadClass(name, false);
		}

		/// <exception cref="System.TypeLoadException"/>
		protected override Type LoadClass(string name, bool resolve)
		{
			lock (this)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Loading class: " + name);
				}
				Type c = FindLoadedClass(name);
				TypeLoadException ex = null;
				if (c == null && !IsSystemClass(name, systemClasses))
				{
					// Try to load class from this classloader's URLs. Note that this is like
					// the servlet spec, not the usual Java 2 behaviour where we ask the
					// parent to attempt to load first.
					try
					{
						c = FindClass(name);
						if (Log.IsDebugEnabled() && c != null)
						{
							Log.Debug("Loaded class: " + name + " ");
						}
					}
					catch (TypeLoadException e)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug(e);
						}
						ex = e;
					}
				}
				if (c == null)
				{
					// try parent
					c = parent.LoadClass(name);
					if (Log.IsDebugEnabled() && c != null)
					{
						Log.Debug("Loaded class from parent: " + name + " ");
					}
				}
				if (c == null)
				{
					throw ex != null ? ex : new TypeLoadException(name);
				}
				if (resolve)
				{
					ResolveClass(c);
				}
				return c;
			}
		}

		/// <summary>Checks if a class should be included as a system class.</summary>
		/// <remarks>
		/// Checks if a class should be included as a system class.
		/// A class is a system class if and only if it matches one of the positive
		/// patterns and none of the negative ones.
		/// </remarks>
		/// <param name="name">the class name to check</param>
		/// <param name="systemClasses">a list of system class configurations.</param>
		/// <returns>true if the class is a system class</returns>
		public static bool IsSystemClass(string name, IList<string> systemClasses)
		{
			bool result = false;
			if (systemClasses != null)
			{
				string canonicalName = name.Replace('/', '.');
				while (canonicalName.StartsWith("."))
				{
					canonicalName = Sharpen.Runtime.Substring(canonicalName, 1);
				}
				foreach (string c in systemClasses)
				{
					bool shouldInclude = true;
					if (c.StartsWith("-"))
					{
						c = Sharpen.Runtime.Substring(c, 1);
						shouldInclude = false;
					}
					if (canonicalName.StartsWith(c))
					{
						if (c.EndsWith(".") || canonicalName.Length == c.Length || canonicalName.Length >
							 c.Length && canonicalName[c.Length] == '$')
						{
							// package
							// class
							// nested
							if (shouldInclude)
							{
								result = true;
							}
							else
							{
								return false;
							}
						}
					}
				}
			}
			return result;
		}
	}
}
