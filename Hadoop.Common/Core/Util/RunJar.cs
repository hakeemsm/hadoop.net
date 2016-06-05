using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;

using Jar;
using Reflect;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>Run a Hadoop job jar.</summary>
	public class RunJar
	{
		/// <summary>Pattern that matches any string</summary>
		public static readonly Pattern MatchAny = Pattern.Compile(".*");

		/// <summary>Priority of the RunJar shutdown hook.</summary>
		public const int ShutdownHookPriority = 10;

		/// <summary>Environment key for using the client classloader.</summary>
		public const string HadoopUseClientClassloader = "HADOOP_USE_CLIENT_CLASSLOADER";

		/// <summary>Environment key for the (user-provided) hadoop classpath.</summary>
		public const string HadoopClasspath = "HADOOP_CLASSPATH";

		/// <summary>Environment key for the system classes.</summary>
		public const string HadoopClientClassloaderSystemClasses = "HADOOP_CLIENT_CLASSLOADER_SYSTEM_CLASSES";

		/// <summary>Unpack a jar file into a directory.</summary>
		/// <remarks>
		/// Unpack a jar file into a directory.
		/// This version unpacks all files inside the jar regardless of filename.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static void UnJar(FilePath jarFile, FilePath toDir)
		{
			UnJar(jarFile, toDir, MatchAny);
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
		public static void UnJar(FilePath jarFile, FilePath toDir, Pattern unpackRegex
			)
		{
			JarFile jar = new JarFile(jarFile);
			try
			{
				Enumeration<JarEntry> entries = ((Enumeration<JarEntry>)jar.Entries());
				while (entries.MoveNext())
				{
					JarEntry entry = entries.Current;
					if (!entry.IsDirectory() && unpackRegex.Matcher(entry.GetName()).Matches())
					{
						InputStream @in = jar.GetInputStream(entry);
						try
						{
							FilePath file = new FilePath(toDir, entry.GetName());
							EnsureDirectory(file.GetParentFile());
							OutputStream @out = new FileOutputStream(file);
							try
							{
								IOUtils.CopyBytes(@in, @out, 8192);
							}
							finally
							{
								@out.Close();
							}
						}
						finally
						{
							@in.Close();
						}
					}
				}
			}
			finally
			{
				jar.Close();
			}
		}

		/// <summary>Ensure the existence of a given directory.</summary>
		/// <exception cref="System.IO.IOException">if it cannot be created and does not already exist
		/// 	</exception>
		private static void EnsureDirectory(FilePath dir)
		{
			if (!dir.Mkdirs() && !dir.IsDirectory())
			{
				throw new IOException("Mkdirs failed to create " + dir.ToString());
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
			new RunJar().Run(args);
		}

		/// <exception cref="System.Exception"/>
		public virtual void Run(string[] args)
		{
			string usage = "RunJar jarFile [mainClass] args...";
			if (args.Length < 1)
			{
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
			int firstArg = 0;
			string fileName = args[firstArg++];
			FilePath file = new FilePath(fileName);
			if (!file.Exists() || !file.IsFile())
			{
				System.Console.Error.WriteLine("Not a valid JAR: " + file.GetCanonicalPath());
				System.Environment.Exit(-1);
			}
			string mainClassName = null;
			JarFile jarFile;
			try
			{
				jarFile = new JarFile(fileName);
			}
			catch (IOException io)
			{
				throw Extensions.InitCause(new IOException("Error opening job jar: " + fileName
					), io);
			}
			Manifest manifest = jarFile.GetManifest();
			if (manifest != null)
			{
				mainClassName = manifest.GetMainAttributes().GetValue("Main-Class");
			}
			jarFile.Close();
			if (mainClassName == null)
			{
				if (args.Length < 2)
				{
					System.Console.Error.WriteLine(usage);
					System.Environment.Exit(-1);
				}
				mainClassName = args[firstArg++];
			}
			mainClassName = mainClassName.ReplaceAll("/", ".");
			FilePath tmpDir = new FilePath(Runtime.GetProperty("java.io.tmpdir"));
			EnsureDirectory(tmpDir);
			FilePath workDir;
			try
			{
				workDir = FilePath.CreateTempFile("hadoop-unjar", string.Empty, tmpDir);
			}
			catch (IOException ioe)
			{
				// If user has insufficient perms to write to tmpDir, default  
				// "Permission denied" message doesn't specify a filename. 
				System.Console.Error.WriteLine("Error creating temp dir in java.io.tmpdir " + tmpDir
					 + " due to " + ioe.Message);
				System.Environment.Exit(-1);
				return;
			}
			if (!workDir.Delete())
			{
				System.Console.Error.WriteLine("Delete failed for " + workDir);
				System.Environment.Exit(-1);
			}
			EnsureDirectory(workDir);
			ShutdownHookManager.Get().AddShutdownHook(new _Runnable_201(workDir), ShutdownHookPriority
				);
			UnJar(file, workDir);
			ClassLoader loader = CreateClassLoader(file, workDir);
			Thread.CurrentThread().SetContextClassLoader(loader);
			Type mainClass = Runtime.GetType(mainClassName, true, loader);
			MethodInfo main = mainClass.GetMethod("main", new Type[] { System.Array.CreateInstance
				(typeof(string), 0).GetType() });
			string[] newArgs = Collections.ToArray(Arrays.AsList(args).SubList(firstArg
				, args.Length), new string[0]);
			try
			{
				main.Invoke(null, new object[] { newArgs });
			}
			catch (TargetInvocationException e)
			{
				throw e.InnerException;
			}
		}

		private sealed class _Runnable_201 : Runnable
		{
			public _Runnable_201(FilePath workDir)
			{
				this.workDir = workDir;
			}

			public void Run()
			{
				FileUtil.FullyDelete(workDir);
			}

			private readonly FilePath workDir;
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
		/// <exception cref="System.UriFormatException"/>
		private ClassLoader CreateClassLoader(FilePath file, FilePath workDir)
		{
			ClassLoader loader;
			// see if the client classloader is enabled
			if (UseClientClassLoader())
			{
				StringBuilder sb = new StringBuilder();
				sb.Append(workDir + "/").Append(FilePath.pathSeparator).Append(file).Append(FilePath
					.pathSeparator).Append(workDir + "/classes/").Append(FilePath.pathSeparator).Append
					(workDir + "/lib/*");
				// HADOOP_CLASSPATH is added to the client classpath
				string hadoopClasspath = GetHadoopClasspath();
				if (hadoopClasspath != null && !hadoopClasspath.IsEmpty())
				{
					sb.Append(FilePath.pathSeparator).Append(hadoopClasspath);
				}
				string clientClasspath = sb.ToString();
				// get the system classes
				string systemClasses = GetSystemClasses();
				IList<string> systemClassesList = systemClasses == null ? null : Arrays.AsList(StringUtils
					.GetTrimmedStrings(systemClasses));
				// create an application classloader that isolates the user classes
				loader = new ApplicationClassLoader(clientClasspath, GetType().GetClassLoader(), 
					systemClassesList);
			}
			else
			{
				IList<Uri> classPath = new AList<Uri>();
				classPath.AddItem(new FilePath(workDir + "/").ToURI().ToURL());
				classPath.AddItem(file.ToURI().ToURL());
				classPath.AddItem(new FilePath(workDir, "classes/").ToURI().ToURL());
				FilePath[] libs = new FilePath(workDir, "lib").ListFiles();
				if (libs != null)
				{
					for (int i = 0; i < libs.Length; i++)
					{
						classPath.AddItem(libs[i].ToURI().ToURL());
					}
				}
				// create a normal parent-delegating classloader
				loader = new URLClassLoader(Collections.ToArray(classPath, new Uri[0]));
			}
			return loader;
		}

		internal virtual bool UseClientClassLoader()
		{
			return System.Boolean.Parse(Runtime.Getenv(HadoopUseClientClassloader));
		}

		internal virtual string GetHadoopClasspath()
		{
			return Runtime.Getenv(HadoopClasspath);
		}

		internal virtual string GetSystemClasses()
		{
			return Runtime.Getenv(HadoopClientClassloaderSystemClasses);
		}
	}
}
