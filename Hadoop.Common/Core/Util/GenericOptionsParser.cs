using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// <code>GenericOptionsParser</code> is a utility to parse command line
	/// arguments generic to the Hadoop framework.
	/// </summary>
	/// <remarks>
	/// <code>GenericOptionsParser</code> is a utility to parse command line
	/// arguments generic to the Hadoop framework.
	/// <code>GenericOptionsParser</code> recognizes several standard command
	/// line arguments, enabling applications to easily specify a namenode, a
	/// ResourceManager, additional configuration resources etc.
	/// <h4 id="GenericOptions">Generic Options</h4>
	/// <p>The supported generic options are:</p>
	/// <p><blockquote><pre>
	/// -conf &lt;configuration file&gt;     specify a configuration file
	/// -D &lt;property=value&gt;            use value for given property
	/// -fs &lt;local|namenode:port&gt;      specify a namenode
	/// -jt &lt;local|resourcemanager:port&gt;    specify a ResourceManager
	/// -files &lt;comma separated list of files&gt;    specify comma separated
	/// files to be copied to the map reduce cluster
	/// -libjars &lt;comma separated list of jars&gt;   specify comma separated
	/// jar files to include in the classpath.
	/// -archives &lt;comma separated list of archives&gt;    specify comma
	/// separated archives to be unarchived on the compute machines.
	/// </pre></blockquote></p>
	/// <p>The general command line syntax is:</p>
	/// <p><tt><pre>
	/// bin/hadoop command [genericOptions] [commandOptions]
	/// </pre></tt></p>
	/// <p>Generic command line arguments <strong>might</strong> modify
	/// <code>Configuration </code> objects, given to constructors.</p>
	/// <p>The functionality is implemented using Commons CLI.</p>
	/// <p>Examples:</p>
	/// <p><blockquote><pre>
	/// $ bin/hadoop dfs -fs darwin:8020 -ls /data
	/// list /data directory in dfs with namenode darwin:8020
	/// $ bin/hadoop dfs -D fs.default.name=darwin:8020 -ls /data
	/// list /data directory in dfs with namenode darwin:8020
	/// $ bin/hadoop dfs -conf core-site.xml -conf hdfs-site.xml -ls /data
	/// list /data directory in dfs with multiple conf files specified.
	/// $ bin/hadoop job -D yarn.resourcemanager.address=darwin:8032 -submit job.xml
	/// submit a job to ResourceManager darwin:8032
	/// $ bin/hadoop job -jt darwin:8032 -submit job.xml
	/// submit a job to ResourceManager darwin:8032
	/// $ bin/hadoop job -jt local -submit job.xml
	/// submit a job to local runner
	/// $ bin/hadoop jar -libjars testlib.jar
	/// -archives test.tgz -files file.txt inputjar args
	/// job submission with libjars, files and archives
	/// </pre></blockquote></p>
	/// </remarks>
	/// <seealso cref="Tool"/>
	/// <seealso cref="ToolRunner"/>
	public class GenericOptionsParser
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Util.GenericOptionsParser
			));

		private Configuration conf;

		private CommandLine commandLine;

		/// <summary>Create an options parser with the given options to parse the args.</summary>
		/// <param name="opts">the options</param>
		/// <param name="args">the command line arguments</param>
		/// <exception cref="System.IO.IOException"></exception>
		public GenericOptionsParser(Options opts, string[] args)
			: this(new Configuration(), opts, args)
		{
		}

		/// <summary>Create an options parser to parse the args.</summary>
		/// <param name="args">the command line arguments</param>
		/// <exception cref="System.IO.IOException"></exception>
		public GenericOptionsParser(string[] args)
			: this(new Configuration(), new Options(), args)
		{
		}

		/// <summary>
		/// Create a <code>GenericOptionsParser<code> to parse only the generic Hadoop
		/// arguments.
		/// </summary>
		/// <remarks>
		/// Create a <code>GenericOptionsParser<code> to parse only the generic Hadoop
		/// arguments.
		/// The array of string arguments other than the generic arguments can be
		/// obtained by
		/// <see cref="GetRemainingArgs()"/>
		/// .
		/// </remarks>
		/// <param name="conf">the <code>Configuration</code> to modify.</param>
		/// <param name="args">command-line arguments.</param>
		/// <exception cref="System.IO.IOException"></exception>
		public GenericOptionsParser(Configuration conf, string[] args)
			: this(conf, new Options(), args)
		{
		}

		/// <summary>
		/// Create a <code>GenericOptionsParser</code> to parse given options as well
		/// as generic Hadoop options.
		/// </summary>
		/// <remarks>
		/// Create a <code>GenericOptionsParser</code> to parse given options as well
		/// as generic Hadoop options.
		/// The resulting <code>CommandLine</code> object can be obtained by
		/// <see cref="GetCommandLine()"/>
		/// .
		/// </remarks>
		/// <param name="conf">the configuration to modify</param>
		/// <param name="options">options built by the caller</param>
		/// <param name="args">User-specified arguments</param>
		/// <exception cref="System.IO.IOException"></exception>
		public GenericOptionsParser(Configuration conf, Options options, string[] args)
		{
			ParseGeneralOptions(options, conf, args);
			this.conf = conf;
		}

		/// <summary>Returns an array of Strings containing only application-specific arguments.
		/// 	</summary>
		/// <returns>
		/// array of <code>String</code>s containing the un-parsed arguments
		/// or <strong>empty array</strong> if commandLine was not defined.
		/// </returns>
		public virtual string[] GetRemainingArgs()
		{
			return (commandLine == null) ? new string[] {  } : commandLine.GetArgs();
		}

		/// <summary>Get the modified configuration</summary>
		/// <returns>the configuration that has the modified parameters.</returns>
		public virtual Configuration GetConfiguration()
		{
			return conf;
		}

		/// <summary>
		/// Returns the commons-cli <code>CommandLine</code> object
		/// to process the parsed arguments.
		/// </summary>
		/// <remarks>
		/// Returns the commons-cli <code>CommandLine</code> object
		/// to process the parsed arguments.
		/// Note: If the object is created with
		/// <see cref="GenericOptionsParser(Configuration, string[])"/
		/// 	>
		/// , then returned
		/// object will only contain parsed generic options.
		/// </remarks>
		/// <returns>
		/// <code>CommandLine</code> representing list of arguments
		/// parsed against Options descriptor.
		/// </returns>
		public virtual CommandLine GetCommandLine()
		{
			return commandLine;
		}

		/// <summary>Specify properties of each generic option</summary>
		private static Options BuildGeneralOptions(Options opts)
		{
			Option fs = OptionBuilder.Create("fs");
			Option jt = OptionBuilder.Create("jt");
			Option oconf = OptionBuilder.Create("conf");
			Option property = OptionBuilder.Create('D');
			Option libjars = OptionBuilder.Create("libjars");
			Option files = OptionBuilder.Create("files");
			Option archives = OptionBuilder.Create("archives");
			// file with security tokens
			Option tokensFile = OptionBuilder.Create("tokenCacheFile");
			opts.AddOption(fs);
			opts.AddOption(jt);
			opts.AddOption(oconf);
			opts.AddOption(property);
			opts.AddOption(libjars);
			opts.AddOption(files);
			opts.AddOption(archives);
			opts.AddOption(tokensFile);
			return opts;
		}

		/// <summary>Modify configuration according user-specified generic options</summary>
		/// <param name="conf">Configuration to be modified</param>
		/// <param name="line">User-specified generic options</param>
		/// <exception cref="System.IO.IOException"/>
		private void ProcessGeneralOptions(Configuration conf, CommandLine line)
		{
			if (line.HasOption("fs"))
			{
				FileSystem.SetDefaultUri(conf, line.GetOptionValue("fs"));
			}
			if (line.HasOption("jt"))
			{
				string optionValue = line.GetOptionValue("jt");
				if (Sharpen.Runtime.EqualsIgnoreCase(optionValue, "local"))
				{
					conf.Set("mapreduce.framework.name", optionValue);
				}
				conf.Set("yarn.resourcemanager.address", optionValue, "from -jt command line option"
					);
			}
			if (line.HasOption("conf"))
			{
				string[] values = line.GetOptionValues("conf");
				foreach (string value in values)
				{
					conf.AddResource(new Path(value));
				}
			}
			if (line.HasOption('D'))
			{
				string[] property = line.GetOptionValues('D');
				foreach (string prop in property)
				{
					string[] keyval = prop.Split("=", 2);
					if (keyval.Length == 2)
					{
						conf.Set(keyval[0], keyval[1], "from command line");
					}
				}
			}
			if (line.HasOption("libjars"))
			{
				conf.Set("tmpjars", ValidateFiles(line.GetOptionValue("libjars"), conf), "from -libjars command line option"
					);
				//setting libjars in client classpath
				Uri[] libjars = GetLibJars(conf);
				if (libjars != null && libjars.Length > 0)
				{
					conf.SetClassLoader(new URLClassLoader(libjars, conf.GetClassLoader()));
					Sharpen.Thread.CurrentThread().SetContextClassLoader(new URLClassLoader(libjars, 
						Sharpen.Thread.CurrentThread().GetContextClassLoader()));
				}
			}
			if (line.HasOption("files"))
			{
				conf.Set("tmpfiles", ValidateFiles(line.GetOptionValue("files"), conf), "from -files command line option"
					);
			}
			if (line.HasOption("archives"))
			{
				conf.Set("tmparchives", ValidateFiles(line.GetOptionValue("archives"), conf), "from -archives command line option"
					);
			}
			conf.SetBoolean("mapreduce.client.genericoptionsparser.used", true);
			// tokensFile
			if (line.HasOption("tokenCacheFile"))
			{
				string fileName = line.GetOptionValue("tokenCacheFile");
				// check if the local file exists
				FileSystem localFs = FileSystem.GetLocal(conf);
				Path p = localFs.MakeQualified(new Path(fileName));
				if (!localFs.Exists(p))
				{
					throw new FileNotFoundException("File " + fileName + " does not exist.");
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("setting conf tokensFile: " + fileName);
				}
				UserGroupInformation.GetCurrentUser().AddCredentials(Credentials.ReadTokenStorageFile
					(p, conf));
				conf.Set("mapreduce.job.credentials.binary", p.ToString(), "from -tokenCacheFile command line option"
					);
			}
		}

		/// <summary>If libjars are set in the conf, parse the libjars.</summary>
		/// <param name="conf"/>
		/// <returns>libjar urls</returns>
		/// <exception cref="System.IO.IOException"/>
		public static Uri[] GetLibJars(Configuration conf)
		{
			string jars = conf.Get("tmpjars");
			if (jars == null)
			{
				return null;
			}
			string[] files = jars.Split(",");
			IList<Uri> cp = new AList<Uri>();
			foreach (string file in files)
			{
				Path tmp = new Path(file);
				if (tmp.GetFileSystem(conf).Equals(FileSystem.GetLocal(conf)))
				{
					cp.AddItem(FileSystem.GetLocal(conf).PathToFile(tmp).ToURI().ToURL());
				}
				else
				{
					Log.Warn("The libjars file " + tmp + " is not on the local " + "filesystem. Ignoring."
						);
				}
			}
			return Sharpen.Collections.ToArray(cp, new Uri[0]);
		}

		/// <summary>
		/// takes input as a comma separated list of files
		/// and verifies if they exist.
		/// </summary>
		/// <remarks>
		/// takes input as a comma separated list of files
		/// and verifies if they exist. It defaults for file:///
		/// if the files specified do not have a scheme.
		/// it returns the paths uri converted defaulting to file:///.
		/// So an input of  /home/user/file1,/home/user/file2 would return
		/// file:///home/user/file1,file:///home/user/file2
		/// </remarks>
		/// <param name="files"/>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		private string ValidateFiles(string files, Configuration conf)
		{
			if (files == null)
			{
				return null;
			}
			string[] fileArr = files.Split(",");
			if (fileArr.Length == 0)
			{
				throw new ArgumentException("File name can't be empty string");
			}
			string[] finalArr = new string[fileArr.Length];
			for (int i = 0; i < fileArr.Length; i++)
			{
				string tmp = fileArr[i];
				if (tmp.IsEmpty())
				{
					throw new ArgumentException("File name can't be empty string");
				}
				string finalPath;
				URI pathURI;
				try
				{
					pathURI = new URI(tmp);
				}
				catch (URISyntaxException e)
				{
					throw new ArgumentException(e);
				}
				Path path = new Path(pathURI);
				FileSystem localFs = FileSystem.GetLocal(conf);
				if (pathURI.GetScheme() == null)
				{
					//default to the local file system
					//check if the file exists or not first
					if (!localFs.Exists(path))
					{
						throw new FileNotFoundException("File " + tmp + " does not exist.");
					}
					finalPath = path.MakeQualified(localFs.GetUri(), localFs.GetWorkingDirectory()).ToString
						();
				}
				else
				{
					// check if the file exists in this file system
					// we need to recreate this filesystem object to copy
					// these files to the file system ResourceManager is running
					// on.
					FileSystem fs = path.GetFileSystem(conf);
					if (!fs.Exists(path))
					{
						throw new FileNotFoundException("File " + tmp + " does not exist.");
					}
					finalPath = path.MakeQualified(fs.GetUri(), fs.GetWorkingDirectory()).ToString();
				}
				finalArr[i] = finalPath;
			}
			return StringUtils.ArrayToString(finalArr);
		}

		/// <summary>
		/// Windows powershell and cmd can parse key=value themselves, because
		/// /pkey=value is same as /pkey value under windows.
		/// </summary>
		/// <remarks>
		/// Windows powershell and cmd can parse key=value themselves, because
		/// /pkey=value is same as /pkey value under windows. However this is not
		/// compatible with how we get arbitrary key values in -Dkey=value format.
		/// Under windows -D key=value or -Dkey=value might be passed as
		/// [-Dkey, value] or [-D key, value]. This method does undo these and
		/// return a modified args list by manually changing [-D, key, value]
		/// into [-D, key=value]
		/// </remarks>
		/// <param name="args">command line arguments</param>
		/// <returns>fixed command line arguments that GnuParser can parse</returns>
		private string[] PreProcessForWindows(string[] args)
		{
			if (!Shell.Windows)
			{
				return args;
			}
			if (args == null)
			{
				return null;
			}
			IList<string> newArgs = new AList<string>(args.Length);
			for (int i = 0; i < args.Length; i++)
			{
				string prop = null;
				if (args[i].Equals("-D"))
				{
					newArgs.AddItem(args[i]);
					if (i < args.Length - 1)
					{
						prop = args[++i];
					}
				}
				else
				{
					if (args[i].StartsWith("-D"))
					{
						prop = args[i];
					}
					else
					{
						newArgs.AddItem(args[i]);
					}
				}
				if (prop != null)
				{
					if (prop.Contains("="))
					{
					}
					else
					{
						// everything good
						if (i < args.Length - 1)
						{
							prop += "=" + args[++i];
						}
					}
					newArgs.AddItem(prop);
				}
			}
			return Sharpen.Collections.ToArray(newArgs, new string[newArgs.Count]);
		}

		/// <summary>
		/// Parse the user-specified options, get the generic options, and modify
		/// configuration accordingly
		/// </summary>
		/// <param name="opts">Options to use for parsing args.</param>
		/// <param name="conf">Configuration to be modified</param>
		/// <param name="args">User-specified arguments</param>
		/// <exception cref="System.IO.IOException"/>
		private void ParseGeneralOptions(Options opts, Configuration conf, string[] args)
		{
			opts = BuildGeneralOptions(opts);
			CommandLineParser parser = new GnuParser();
			try
			{
				commandLine = parser.Parse(opts, PreProcessForWindows(args), true);
				ProcessGeneralOptions(conf, commandLine);
			}
			catch (ParseException e)
			{
				Log.Warn("options parsing failed: " + e.Message);
				HelpFormatter formatter = new HelpFormatter();
				formatter.PrintHelp("general options are: ", opts);
			}
		}

		/// <summary>Print the usage message for generic command-line options supported.</summary>
		/// <param name="out">stream to print the usage message to.</param>
		public static void PrintGenericCommandUsage(TextWriter @out)
		{
			@out.WriteLine("Generic options supported are");
			@out.WriteLine("-conf <configuration file>     specify an application configuration file"
				);
			@out.WriteLine("-D <property=value>            use value for given property");
			@out.WriteLine("-fs <local|namenode:port>      specify a namenode");
			@out.WriteLine("-jt <local|resourcemanager:port>    specify a ResourceManager");
			@out.WriteLine("-files <comma separated list of files>    " + "specify comma separated files to be copied to the map reduce cluster"
				);
			@out.WriteLine("-libjars <comma separated list of jars>    " + "specify comma separated jar files to include in the classpath."
				);
			@out.WriteLine("-archives <comma separated list of archives>    " + "specify comma separated archives to be unarchived"
				 + " on the compute machines.\n");
			@out.WriteLine("The general command line syntax is");
			@out.WriteLine("bin/hadoop command [genericOptions] [commandOptions]\n");
		}
	}
}
