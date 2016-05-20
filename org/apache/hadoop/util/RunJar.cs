using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>Run a Hadoop job jar.</summary>
	public class RunJar
	{
		/// <summary>Pattern that matches any string</summary>
		public static readonly java.util.regex.Pattern MATCH_ANY = java.util.regex.Pattern
			.compile(".*");

		/// <summary>Priority of the RunJar shutdown hook.</summary>
		public const int SHUTDOWN_HOOK_PRIORITY = 10;

		/// <summary>Environment key for using the client classloader.</summary>
		public const string HADOOP_USE_CLIENT_CLASSLOADER = "HADOOP_USE_CLIENT_CLASSLOADER";

		/// <summary>Environment key for the (user-provided) hadoop classpath.</summary>
		public const string HADOOP_CLASSPATH = "HADOOP_CLASSPATH";

		/// <summary>Environment key for the system classes.</summary>
		public const string HADOOP_CLIENT_CLASSLOADER_SYSTEM_CLASSES = "HADOOP_CLIENT_CLASSLOADER_SYSTEM_CLASSES";

		/// <summary>Unpack a jar file into a directory.</summary>
		/// <remarks>
		/// Unpack a jar file into a directory.
		/// This version unpacks all files inside the jar regardless of filename.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static void unJar(java.io.File jarFile, java.io.File toDir)
		{
			unJar(jarFile, toDir, MATCH_ANY);
		}

		/// <summary>Unpack matching files from a jar.</summary>
		/// <remarks>
		/// Unpack matching files from a jar. Entries inside the jar that do
		/// not match the given pattern will be skipped.
		/// </remarks>
		/// <param name="jarFile">the .jar file to unpack</param>
		/// <param name="toDir">the destination directory into which to unpack the jar</param>
		/// <param name="unpackRegex">the pattern to match jar entries against</param>
		/// <exception cref="System.IO.IOException"/>
		public static void unJar(java.io.File jarFile, java.io.File toDir, java.util.regex.Pattern
			 unpackRegex)
		{
			java.util.jar.JarFile jar = new java.util.jar.JarFile(jarFile);
			try
			{
				java.util.Enumeration<java.util.jar.JarEntry> entries = ((java.util.Enumeration<java.util.jar.JarEntry
					>)jar.entries());
				while (entries.MoveNext())
				{
					java.util.jar.JarEntry entry = entries.Current;
					if (!entry.isDirectory() && unpackRegex.matcher(entry.getName()).matches())
					{
						java.io.InputStream @in = jar.getInputStream(entry);
						try
						{
							java.io.File file = new java.io.File(toDir, entry.getName());
							ensureDirectory(file.getParentFile());
							java.io.OutputStream @out = new java.io.FileOutputStream(file);
							try
							{
								org.apache.hadoop.io.IOUtils.copyBytes(@in, @out, 8192);
							}
							finally
							{
								@out.close();
							}
						}
						finally
						{
							@in.close();
						}
					}
				}
			}
			finally
			{
				jar.close();
			}
		}

		/// <summary>Ensure the existence of a given directory.</summary>
		/// <exception cref="System.IO.IOException">if it cannot be created and does not already exist
		/// 	</exception>
		private static void ensureDirectory(java.io.File dir)
		{
			if (!dir.mkdirs() && !dir.isDirectory())
			{
				throw new System.IO.IOException("Mkdirs failed to create " + dir.ToString());
			}
		}

		/// <summary>Run a Hadoop job jar.</summary>
		/// <remarks>
		/// Run a Hadoop job jar.  If the main class is not in the jar's manifest,
		/// then it must be provided on the command line.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new org.apache.hadoop.util.RunJar().run(args);
		}

		/// <exception cref="System.Exception"/>
		public virtual void run(string[] args)
		{
			string usage = "RunJar jarFile [mainClass] args...";
			if (args.Length < 1)
			{
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
			int firstArg = 0;
			string fileName = args[firstArg++];
			java.io.File file = new java.io.File(fileName);
			if (!file.exists() || !file.isFile())
			{
				System.Console.Error.WriteLine("Not a valid JAR: " + file.getCanonicalPath());
				System.Environment.Exit(-1);
			}
			string mainClassName = null;
			java.util.jar.JarFile jarFile;
			try
			{
				jarFile = new java.util.jar.JarFile(fileName);
			}
			catch (System.IO.IOException io)
			{
				throw new System.IO.IOException("Error opening job jar: " + fileName).initCause(io
					);
			}
			java.util.jar.Manifest manifest = jarFile.getManifest();
			if (manifest != null)
			{
				mainClassName = manifest.getMainAttributes().getValue("Main-Class");
			}
			jarFile.close();
			if (mainClassName == null)
			{
				if (args.Length < 2)
				{
					System.Console.Error.WriteLine(usage);
					System.Environment.Exit(-1);
				}
				mainClassName = args[firstArg++];
			}
			mainClassName = mainClassName.replaceAll("/", ".");
			java.io.File tmpDir = new java.io.File(Sharpen.Runtime.getProperty("java.io.tmpdir"
				));
			ensureDirectory(tmpDir);
			java.io.File workDir;
			try
			{
				workDir = java.io.File.createTempFile("hadoop-unjar", string.Empty, tmpDir);
			}
			catch (System.IO.IOException ioe)
			{
				// If user has insufficient perms to write to tmpDir, default  
				// "Permission denied" message doesn't specify a filename. 
				System.Console.Error.WriteLine("Error creating temp dir in java.io.tmpdir " + tmpDir
					 + " due to " + ioe.Message);
				System.Environment.Exit(-1);
				return;
			}
			if (!workDir.delete())
			{
				System.Console.Error.WriteLine("Delete failed for " + workDir);
				System.Environment.Exit(-1);
			}
			ensureDirectory(workDir);
			org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(new _Runnable_201
				(workDir), SHUTDOWN_HOOK_PRIORITY);
			unJar(file, workDir);
			java.lang.ClassLoader loader = createClassLoader(file, workDir);
			java.lang.Thread.currentThread().setContextClassLoader(loader);
			java.lang.Class mainClass = java.lang.Class.forName(mainClassName, true, loader);
			java.lang.reflect.Method main = mainClass.getMethod("main", new java.lang.Class[]
				 { Sharpen.Runtime.getClassForObject(java.lang.reflect.Array.newInstance(Sharpen.Runtime.getClassForType
				(typeof(string)), 0)) });
			string[] newArgs = Sharpen.Collections.ToArray(java.util.Arrays.asList(args).subList
				(firstArg, args.Length), new string[0]);
			try
			{
				main.invoke(null, new object[] { newArgs });
			}
			catch (java.lang.reflect.InvocationTargetException e)
			{
				throw e.getTargetException();
			}
		}

		private sealed class _Runnable_201 : java.lang.Runnable
		{
			public _Runnable_201(java.io.File workDir)
			{
				this.workDir = workDir;
			}

			public void run()
			{
				org.apache.hadoop.fs.FileUtil.fullyDelete(workDir);
			}

			private readonly java.io.File workDir;
		}

		/// <summary>
		/// Creates a classloader based on the environment that was specified by the
		/// user.
		/// </summary>
		/// <remarks>
		/// Creates a classloader based on the environment that was specified by the
		/// user. If HADOOP_USE_CLIENT_CLASSLOADER is specified, it creates an
		/// application classloader that provides the isolation of the user class space
		/// from the hadoop classes and their dependencies. It forms a class space for
		/// the user jar as well as the HADOOP_CLASSPATH. Otherwise, it creates a
		/// classloader that simply adds the user jar to the classpath.
		/// </remarks>
		/// <exception cref="java.net.MalformedURLException"/>
		private java.lang.ClassLoader createClassLoader(java.io.File file, java.io.File workDir
			)
		{
			java.lang.ClassLoader loader;
			// see if the client classloader is enabled
			if (useClientClassLoader())
			{
				java.lang.StringBuilder sb = new java.lang.StringBuilder();
				sb.Append(workDir + "/").Append(java.io.File.pathSeparator).Append(file).Append(java.io.File
					.pathSeparator).Append(workDir + "/classes/").Append(java.io.File.pathSeparator)
					.Append(workDir + "/lib/*");
				// HADOOP_CLASSPATH is added to the client classpath
				string hadoopClasspath = getHadoopClasspath();
				if (hadoopClasspath != null && !hadoopClasspath.isEmpty())
				{
					sb.Append(java.io.File.pathSeparator).Append(hadoopClasspath);
				}
				string clientClasspath = sb.ToString();
				// get the system classes
				string systemClasses = getSystemClasses();
				System.Collections.Generic.IList<string> systemClassesList = systemClasses == null
					 ? null : java.util.Arrays.asList(org.apache.hadoop.util.StringUtils.getTrimmedStrings
					(systemClasses));
				// create an application classloader that isolates the user classes
				loader = new org.apache.hadoop.util.ApplicationClassLoader(clientClasspath, Sharpen.Runtime.getClassForObject
					(this).getClassLoader(), systemClassesList);
			}
			else
			{
				System.Collections.Generic.IList<java.net.URL> classPath = new System.Collections.Generic.List
					<java.net.URL>();
				classPath.add(new java.io.File(workDir + "/").toURI().toURL());
				classPath.add(file.toURI().toURL());
				classPath.add(new java.io.File(workDir, "classes/").toURI().toURL());
				java.io.File[] libs = new java.io.File(workDir, "lib").listFiles();
				if (libs != null)
				{
					for (int i = 0; i < libs.Length; i++)
					{
						classPath.add(libs[i].toURI().toURL());
					}
				}
				// create a normal parent-delegating classloader
				loader = new java.net.URLClassLoader(Sharpen.Collections.ToArray(classPath, new java.net.URL
					[0]));
			}
			return loader;
		}

		internal virtual bool useClientClassLoader()
		{
			return bool.parseBoolean(Sharpen.Runtime.getenv(HADOOP_USE_CLIENT_CLASSLOADER));
		}

		internal virtual string getHadoopClasspath()
		{
			return Sharpen.Runtime.getenv(HADOOP_CLASSPATH);
		}

		internal virtual string getSystemClasses()
		{
			return Sharpen.Runtime.getenv(HADOOP_CLIENT_CLASSLOADER_SYSTEM_CLASSES);
		}
	}
}
