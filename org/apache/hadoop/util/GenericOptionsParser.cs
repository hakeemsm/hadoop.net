using Sharpen;

namespace org.apache.hadoop.util
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
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.GenericOptionsParser
			)));

		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.commons.cli.CommandLine commandLine;

		/// <summary>Create an options parser with the given options to parse the args.</summary>
		/// <param name="opts">the options</param>
		/// <param name="args">the command line arguments</param>
		/// <exception cref="System.IO.IOException"></exception>
		public GenericOptionsParser(org.apache.commons.cli.Options opts, string[] args)
			: this(new org.apache.hadoop.conf.Configuration(), opts, args)
		{
		}

		/// <summary>Create an options parser to parse the args.</summary>
		/// <param name="args">the command line arguments</param>
		/// <exception cref="System.IO.IOException"></exception>
		public GenericOptionsParser(string[] args)
			: this(new org.apache.hadoop.conf.Configuration(), new org.apache.commons.cli.Options
				(), args)
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
		/// <see cref="getRemainingArgs()"/>
		/// .
		/// </remarks>
		/// <param name="conf">the <code>Configuration</code> to modify.</param>
		/// <param name="args">command-line arguments.</param>
		/// <exception cref="System.IO.IOException"></exception>
		public GenericOptionsParser(org.apache.hadoop.conf.Configuration conf, string[] args
			)
			: this(conf, new org.apache.commons.cli.Options(), args)
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
		/// <see cref="getCommandLine()"/>
		/// .
		/// </remarks>
		/// <param name="conf">the configuration to modify</param>
		/// <param name="options">options built by the caller</param>
		/// <param name="args">User-specified arguments</param>
		/// <exception cref="System.IO.IOException"></exception>
		public GenericOptionsParser(org.apache.hadoop.conf.Configuration conf, org.apache.commons.cli.Options
			 options, string[] args)
		{
			parseGeneralOptions(options, conf, args);
			this.conf = conf;
		}

		/// <summary>Returns an array of Strings containing only application-specific arguments.
		/// 	</summary>
		/// <returns>
		/// array of <code>String</code>s containing the un-parsed arguments
		/// or <strong>empty array</strong> if commandLine was not defined.
		/// </returns>
		public virtual string[] getRemainingArgs()
		{
			return (commandLine == null) ? new string[] {  } : commandLine.getArgs();
		}

		/// <summary>Get the modified configuration</summary>
		/// <returns>the configuration that has the modified parameters.</returns>
		public virtual org.apache.hadoop.conf.Configuration getConfiguration()
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
		/// <see cref="GenericOptionsParser(org.apache.hadoop.conf.Configuration, string[])"/
		/// 	>
		/// , then returned
		/// object will only contain parsed generic options.
		/// </remarks>
		/// <returns>
		/// <code>CommandLine</code> representing list of arguments
		/// parsed against Options descriptor.
		/// </returns>
		public virtual org.apache.commons.cli.CommandLine getCommandLine()
		{
			return commandLine;
		}

		/// <summary>Specify properties of each generic option</summary>
		private static org.apache.commons.cli.Options buildGeneralOptions(org.apache.commons.cli.Options
			 opts)
		{
			org.apache.commons.cli.Option fs = org.apache.commons.cli.OptionBuilder.create("fs"
				);
			org.apache.commons.cli.Option jt = org.apache.commons.cli.OptionBuilder.create("jt"
				);
			org.apache.commons.cli.Option oconf = org.apache.commons.cli.OptionBuilder.create
				("conf");
			org.apache.commons.cli.Option property = org.apache.commons.cli.OptionBuilder.create
				('D');
			org.apache.commons.cli.Option libjars = org.apache.commons.cli.OptionBuilder.create
				("libjars");
			org.apache.commons.cli.Option files = org.apache.commons.cli.OptionBuilder.create
				("files");
			org.apache.commons.cli.Option archives = org.apache.commons.cli.OptionBuilder.create
				("archives");
			// file with security tokens
			org.apache.commons.cli.Option tokensFile = org.apache.commons.cli.OptionBuilder.create
				("tokenCacheFile");
			opts.addOption(fs);
			opts.addOption(jt);
			opts.addOption(oconf);
			opts.addOption(property);
			opts.addOption(libjars);
			opts.addOption(files);
			opts.addOption(archives);
			opts.addOption(tokensFile);
			return opts;
		}

		/// <summary>Modify configuration according user-specified generic options</summary>
		/// <param name="conf">Configuration to be modified</param>
		/// <param name="line">User-specified generic options</param>
		/// <exception cref="System.IO.IOException"/>
		private void processGeneralOptions(org.apache.hadoop.conf.Configuration conf, org.apache.commons.cli.CommandLine
			 line)
		{
			if (line.hasOption("fs"))
			{
				org.apache.hadoop.fs.FileSystem.setDefaultUri(conf, line.getOptionValue("fs"));
			}
			if (line.hasOption("jt"))
			{
				string optionValue = line.getOptionValue("jt");
				if (Sharpen.Runtime.equalsIgnoreCase(optionValue, "local"))
				{
					conf.set("mapreduce.framework.name", optionValue);
				}
				conf.set("yarn.resourcemanager.address", optionValue, "from -jt command line option"
					);
			}
			if (line.hasOption("conf"))
			{
				string[] values = line.getOptionValues("conf");
				foreach (string value in values)
				{
					conf.addResource(new org.apache.hadoop.fs.Path(value));
				}
			}
			if (line.hasOption('D'))
			{
				string[] property = line.getOptionValues('D');
				foreach (string prop in property)
				{
					string[] keyval = prop.split("=", 2);
					if (keyval.Length == 2)
					{
						conf.set(keyval[0], keyval[1], "from command line");
					}
				}
			}
			if (line.hasOption("libjars"))
			{
				conf.set("tmpjars", validateFiles(line.getOptionValue("libjars"), conf), "from -libjars command line option"
					);
				//setting libjars in client classpath
				java.net.URL[] libjars = getLibJars(conf);
				if (libjars != null && libjars.Length > 0)
				{
					conf.setClassLoader(new java.net.URLClassLoader(libjars, conf.getClassLoader()));
					java.lang.Thread.currentThread().setContextClassLoader(new java.net.URLClassLoader
						(libjars, java.lang.Thread.currentThread().getContextClassLoader()));
				}
			}
			if (line.hasOption("files"))
			{
				conf.set("tmpfiles", validateFiles(line.getOptionValue("files"), conf), "from -files command line option"
					);
			}
			if (line.hasOption("archives"))
			{
				conf.set("tmparchives", validateFiles(line.getOptionValue("archives"), conf), "from -archives command line option"
					);
			}
			conf.setBoolean("mapreduce.client.genericoptionsparser.used", true);
			// tokensFile
			if (line.hasOption("tokenCacheFile"))
			{
				string fileName = line.getOptionValue("tokenCacheFile");
				// check if the local file exists
				org.apache.hadoop.fs.FileSystem localFs = org.apache.hadoop.fs.FileSystem.getLocal
					(conf);
				org.apache.hadoop.fs.Path p = localFs.makeQualified(new org.apache.hadoop.fs.Path
					(fileName));
				if (!localFs.exists(p))
				{
					throw new java.io.FileNotFoundException("File " + fileName + " does not exist.");
				}
				if (LOG.isDebugEnabled())
				{
					LOG.debug("setting conf tokensFile: " + fileName);
				}
				org.apache.hadoop.security.UserGroupInformation.getCurrentUser().addCredentials(org.apache.hadoop.security.Credentials
					.readTokenStorageFile(p, conf));
				conf.set("mapreduce.job.credentials.binary", p.ToString(), "from -tokenCacheFile command line option"
					);
			}
		}

		/// <summary>If libjars are set in the conf, parse the libjars.</summary>
		/// <param name="conf"/>
		/// <returns>libjar urls</returns>
		/// <exception cref="System.IO.IOException"/>
		public static java.net.URL[] getLibJars(org.apache.hadoop.conf.Configuration conf
			)
		{
			string jars = conf.get("tmpjars");
			if (jars == null)
			{
				return null;
			}
			string[] files = jars.split(",");
			System.Collections.Generic.IList<java.net.URL> cp = new System.Collections.Generic.List
				<java.net.URL>();
			foreach (string file in files)
			{
				org.apache.hadoop.fs.Path tmp = new org.apache.hadoop.fs.Path(file);
				if (tmp.getFileSystem(conf).Equals(org.apache.hadoop.fs.FileSystem.getLocal(conf)
					))
				{
					cp.add(org.apache.hadoop.fs.FileSystem.getLocal(conf).pathToFile(tmp).toURI().toURL
						());
				}
				else
				{
					LOG.warn("The libjars file " + tmp + " is not on the local " + "filesystem. Ignoring."
						);
				}
			}
			return Sharpen.Collections.ToArray(cp, new java.net.URL[0]);
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
		private string validateFiles(string files, org.apache.hadoop.conf.Configuration conf
			)
		{
			if (files == null)
			{
				return null;
			}
			string[] fileArr = files.split(",");
			if (fileArr.Length == 0)
			{
				throw new System.ArgumentException("File name can't be empty string");
			}
			string[] finalArr = new string[fileArr.Length];
			for (int i = 0; i < fileArr.Length; i++)
			{
				string tmp = fileArr[i];
				if (tmp.isEmpty())
				{
					throw new System.ArgumentException("File name can't be empty string");
				}
				string finalPath;
				java.net.URI pathURI;
				try
				{
					pathURI = new java.net.URI(tmp);
				}
				catch (java.net.URISyntaxException e)
				{
					throw new System.ArgumentException(e);
				}
				org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(pathURI);
				org.apache.hadoop.fs.FileSystem localFs = org.apache.hadoop.fs.FileSystem.getLocal
					(conf);
				if (pathURI.getScheme() == null)
				{
					//default to the local file system
					//check if the file exists or not first
					if (!localFs.exists(path))
					{
						throw new java.io.FileNotFoundException("File " + tmp + " does not exist.");
					}
					finalPath = path.makeQualified(localFs.getUri(), localFs.getWorkingDirectory()).ToString
						();
				}
				else
				{
					// check if the file exists in this file system
					// we need to recreate this filesystem object to copy
					// these files to the file system ResourceManager is running
					// on.
					org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
					if (!fs.exists(path))
					{
						throw new java.io.FileNotFoundException("File " + tmp + " does not exist.");
					}
					finalPath = path.makeQualified(fs.getUri(), fs.getWorkingDirectory()).ToString();
				}
				finalArr[i] = finalPath;
			}
			return org.apache.hadoop.util.StringUtils.arrayToString(finalArr);
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
		private string[] preProcessForWindows(string[] args)
		{
			if (!org.apache.hadoop.util.Shell.WINDOWS)
			{
				return args;
			}
			if (args == null)
			{
				return null;
			}
			System.Collections.Generic.IList<string> newArgs = new System.Collections.Generic.List
				<string>(args.Length);
			for (int i = 0; i < args.Length; i++)
			{
				string prop = null;
				if (args[i].Equals("-D"))
				{
					newArgs.add(args[i]);
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
						newArgs.add(args[i]);
					}
				}
				if (prop != null)
				{
					if (prop.contains("="))
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
					newArgs.add(prop);
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
		private void parseGeneralOptions(org.apache.commons.cli.Options opts, org.apache.hadoop.conf.Configuration
			 conf, string[] args)
		{
			opts = buildGeneralOptions(opts);
			org.apache.commons.cli.CommandLineParser parser = new org.apache.commons.cli.GnuParser
				();
			try
			{
				commandLine = parser.parse(opts, preProcessForWindows(args), true);
				processGeneralOptions(conf, commandLine);
			}
			catch (org.apache.commons.cli.ParseException e)
			{
				LOG.warn("options parsing failed: " + e.Message);
				org.apache.commons.cli.HelpFormatter formatter = new org.apache.commons.cli.HelpFormatter
					();
				formatter.printHelp("general options are: ", opts);
			}
		}

		/// <summary>Print the usage message for generic command-line options supported.</summary>
		/// <param name="out">stream to print the usage message to.</param>
		public static void printGenericCommandUsage(System.IO.TextWriter @out)
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
