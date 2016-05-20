using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>
	/// A
	/// <see cref="java.net.URLClassLoader"/>
	/// for application isolation. Classes from the
	/// application JARs are loaded in preference to the parent loader.
	/// </summary>
	public class ApplicationClassLoader : java.net.URLClassLoader
	{
		/// <summary>Default value of the system classes if the user did not override them.</summary>
		/// <remarks>
		/// Default value of the system classes if the user did not override them.
		/// JDK classes, hadoop classes and resources, and some select third-party
		/// classes are considered system classes, and are not loaded by the
		/// application classloader.
		/// </remarks>
		public static readonly string SYSTEM_CLASSES_DEFAULT;

		private const string PROPERTIES_FILE = "org.apache.hadoop.application-classloader.properties";

		private const string SYSTEM_CLASSES_DEFAULT_KEY = "system.classes.default";

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.ApplicationClassLoader
			)).getName());

		private sealed class _FilenameFilter_62 : java.io.FilenameFilter
		{
			public _FilenameFilter_62()
			{
			}

			public bool accept(java.io.File dir, string name)
			{
				return name.EndsWith(".jar") || name.EndsWith(".JAR");
			}
		}

		private static readonly java.io.FilenameFilter JAR_FILENAME_FILTER = new _FilenameFilter_62
			();

		static ApplicationClassLoader()
		{
			try
			{
				using (java.io.InputStream @is = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.ApplicationClassLoader
					)).getClassLoader().getResourceAsStream(PROPERTIES_FILE))
				{
					if (@is == null)
					{
						throw new java.lang.ExceptionInInitializerError("properties file " + PROPERTIES_FILE
							 + " is not found");
					}
					java.util.Properties props = new java.util.Properties();
					props.load(@is);
					// get the system classes default
					string systemClassesDefault = props.getProperty(SYSTEM_CLASSES_DEFAULT_KEY);
					if (systemClassesDefault == null)
					{
						throw new java.lang.ExceptionInInitializerError("property " + SYSTEM_CLASSES_DEFAULT_KEY
							 + " is not found");
					}
					SYSTEM_CLASSES_DEFAULT = systemClassesDefault;
				}
			}
			catch (System.IO.IOException e)
			{
				throw new java.lang.ExceptionInInitializerError(e);
			}
		}

		private readonly java.lang.ClassLoader parent;

		private readonly System.Collections.Generic.IList<string> systemClasses;

		public ApplicationClassLoader(java.net.URL[] urls, java.lang.ClassLoader parent, 
			System.Collections.Generic.IList<string> systemClasses)
			: base(urls, parent)
		{
			if (LOG.isDebugEnabled())
			{
				LOG.debug("urls: " + java.util.Arrays.toString(urls));
				LOG.debug("system classes: " + systemClasses);
			}
			this.parent = parent;
			if (parent == null)
			{
				throw new System.ArgumentException("No parent classloader!");
			}
			// if the caller-specified system classes are null or empty, use the default
			this.systemClasses = (systemClasses == null || systemClasses.isEmpty()) ? java.util.Arrays
				.asList(org.apache.hadoop.util.StringUtils.getTrimmedStrings(SYSTEM_CLASSES_DEFAULT
				)) : systemClasses;
			LOG.info("system classes: " + this.systemClasses);
		}

		/// <exception cref="java.net.MalformedURLException"/>
		public ApplicationClassLoader(string classpath, java.lang.ClassLoader parent, System.Collections.Generic.IList
			<string> systemClasses)
			: this(constructUrlsFromClasspath(classpath), parent, systemClasses)
		{
		}

		/// <exception cref="java.net.MalformedURLException"/>
		internal static java.net.URL[] constructUrlsFromClasspath(string classpath)
		{
			System.Collections.Generic.IList<java.net.URL> urls = new System.Collections.Generic.List
				<java.net.URL>();
			foreach (string element in classpath.split(java.io.File.pathSeparator))
			{
				if (element.EndsWith("/*"))
				{
					string dir = Sharpen.Runtime.substring(element, 0, element.Length - 1);
					java.io.File[] files = new java.io.File(dir).listFiles(JAR_FILENAME_FILTER);
					if (files != null)
					{
						foreach (java.io.File file in files)
						{
							urls.add(file.toURI().toURL());
						}
					}
				}
				else
				{
					java.io.File file = new java.io.File(element);
					if (file.exists())
					{
						urls.add(new java.io.File(element).toURI().toURL());
					}
				}
			}
			return Sharpen.Collections.ToArray(urls, new java.net.URL[urls.Count]);
		}

		public override java.net.URL getResource(string name)
		{
			java.net.URL url = null;
			if (!isSystemClass(name, systemClasses))
			{
				url = findResource(name);
				if (url == null && name.StartsWith("/"))
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug("Remove leading / off " + name);
					}
					url = findResource(Sharpen.Runtime.substring(name, 1));
				}
			}
			if (url == null)
			{
				url = parent.getResource(name);
			}
			if (url != null)
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("getResource(" + name + ")=" + url);
				}
			}
			return url;
		}

		/// <exception cref="java.lang.ClassNotFoundException"/>
		public override java.lang.Class loadClass(string name)
		{
			return this.loadClass(name, false);
		}

		/// <exception cref="java.lang.ClassNotFoundException"/>
		protected override java.lang.Class loadClass(string name, bool resolve)
		{
			lock (this)
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Loading class: " + name);
				}
				java.lang.Class c = findLoadedClass(name);
				java.lang.ClassNotFoundException ex = null;
				if (c == null && !isSystemClass(name, systemClasses))
				{
					// Try to load class from this classloader's URLs. Note that this is like
					// the servlet spec, not the usual Java 2 behaviour where we ask the
					// parent to attempt to load first.
					try
					{
						c = findClass(name);
						if (LOG.isDebugEnabled() && c != null)
						{
							LOG.debug("Loaded class: " + name + " ");
						}
					}
					catch (java.lang.ClassNotFoundException e)
					{
						if (LOG.isDebugEnabled())
						{
							LOG.debug(e);
						}
						ex = e;
					}
				}
				if (c == null)
				{
					// try parent
					c = parent.loadClass(name);
					if (LOG.isDebugEnabled() && c != null)
					{
						LOG.debug("Loaded class from parent: " + name + " ");
					}
				}
				if (c == null)
				{
					throw ex != null ? ex : new java.lang.ClassNotFoundException(name);
				}
				if (resolve)
				{
					resolveClass(c);
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
		public static bool isSystemClass(string name, System.Collections.Generic.IList<string
			> systemClasses)
		{
			bool result = false;
			if (systemClasses != null)
			{
				string canonicalName = name.Replace('/', '.');
				while (canonicalName.StartsWith("."))
				{
					canonicalName = Sharpen.Runtime.substring(canonicalName, 1);
				}
				foreach (string c in systemClasses)
				{
					bool shouldInclude = true;
					if (c.StartsWith("-"))
					{
						c = Sharpen.Runtime.substring(c, 1);
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
