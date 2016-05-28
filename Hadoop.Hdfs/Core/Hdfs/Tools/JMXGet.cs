using System;
using System.Collections.Generic;
using Javax.Management;
using Javax.Management.Remote;
using Org.Apache.Commons.Cli;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>
	/// tool to get data from NameNode or DataNode using MBeans currently the
	/// following MBeans are available (under hadoop domain):
	/// hadoop:service=NameNode,name=FSNamesystemState (static)
	/// hadoop:service=NameNode,name=NameNodeActivity (dynamic)
	/// hadoop:service=NameNode,name=RpcActivityForPort9000 (dynamic)
	/// hadoop:service=DataNode,name=RpcActivityForPort50020 (dynamic)
	/// hadoop:name=service=DataNode,FSDatasetState-UndefinedStorageId663800459
	/// (static)
	/// hadoop:service=DataNode,name=DataNodeActivity-UndefinedStorageId-520845215
	/// (dynamic)
	/// implementation note: all logging is sent to System.err (since it is a command
	/// line tool)
	/// </summary>
	public class JMXGet
	{
		private const string format = "%s=%s%n";

		private AList<ObjectName> hadoopObjectNames;

		private MBeanServerConnection mbsc;

		private string service = "NameNode";

		private string port = string.Empty;

		private string server = "localhost";

		private string localVMUrl = null;

		public JMXGet()
		{
		}

		public virtual void SetService(string service)
		{
			this.service = service;
		}

		public virtual void SetPort(string port)
		{
			this.port = port;
		}

		public virtual void SetServer(string server)
		{
			this.server = server;
		}

		public virtual void SetLocalVMUrl(string url)
		{
			this.localVMUrl = url;
		}

		/// <summary>print all attributes' values</summary>
		/// <exception cref="System.Exception"/>
		public virtual void PrintAllValues()
		{
			Err("List of all the available keys:");
			object val = null;
			foreach (ObjectName oname in hadoopObjectNames)
			{
				Err(">>>>>>>>jmx name: " + oname.GetCanonicalKeyPropertyListString());
				MBeanInfo mbinfo = mbsc.GetMBeanInfo(oname);
				MBeanAttributeInfo[] mbinfos = mbinfo.GetAttributes();
				foreach (MBeanAttributeInfo mb in mbinfos)
				{
					val = mbsc.GetAttribute(oname, mb.GetName());
					System.Console.Out.Format(format, mb.GetName(), (val == null) ? string.Empty : val
						.ToString());
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void PrintAllMatchedAttributes(string attrRegExp)
		{
			Err("List of the keys matching " + attrRegExp + " :");
			object val = null;
			Sharpen.Pattern p = Sharpen.Pattern.Compile(attrRegExp);
			foreach (ObjectName oname in hadoopObjectNames)
			{
				Err(">>>>>>>>jmx name: " + oname.GetCanonicalKeyPropertyListString());
				MBeanInfo mbinfo = mbsc.GetMBeanInfo(oname);
				MBeanAttributeInfo[] mbinfos = mbinfo.GetAttributes();
				foreach (MBeanAttributeInfo mb in mbinfos)
				{
					if (p.Matcher(mb.GetName()).LookingAt())
					{
						val = mbsc.GetAttribute(oname, mb.GetName());
						System.Console.Out.Format(format, mb.GetName(), (val == null) ? string.Empty : val
							.ToString());
					}
				}
			}
		}

		/// <summary>get single value by key</summary>
		/// <exception cref="System.Exception"/>
		public virtual string GetValue(string key)
		{
			object val = null;
			foreach (ObjectName oname in hadoopObjectNames)
			{
				try
				{
					val = mbsc.GetAttribute(oname, key);
				}
				catch (AttributeNotFoundException)
				{
					/* just go to the next */
					continue;
				}
				catch (ReflectionException re)
				{
					if (re.InnerException is MissingMethodException)
					{
						continue;
					}
				}
				Err("Info: key = " + key + "; val = " + (val == null ? "null" : val.GetType()) + 
					":" + val);
				break;
			}
			return (val == null) ? string.Empty : val.ToString();
		}

		/// <exception cref="System.Exception">initializes MBeanServer</exception>
		public virtual void Init()
		{
			Err("init: server=" + server + ";port=" + port + ";service=" + service + ";localVMUrl="
				 + localVMUrl);
			string url_string = null;
			// build connection url
			if (localVMUrl != null)
			{
				// use
				// jstat -snap <vmpid> | grep sun.management.JMXConnectorServer.address
				// to get url
				url_string = localVMUrl;
				Err("url string for local pid = " + localVMUrl + " = " + url_string);
			}
			else
			{
				if (!port.IsEmpty() && !server.IsEmpty())
				{
					// using server and port
					url_string = "service:jmx:rmi:///jndi/rmi://" + server + ":" + port + "/jmxrmi";
				}
			}
			// else url stays null
			// Create an RMI connector client and
			// connect it to the RMI connector server
			if (url_string == null)
			{
				// assume local vm (for example for Testing)
				mbsc = ManagementFactory.GetPlatformMBeanServer();
			}
			else
			{
				JMXServiceURL url = new JMXServiceURL(url_string);
				Err("Create RMI connector and connect to the RMI connector server" + url);
				JMXConnector jmxc = JMXConnectorFactory.Connect(url, null);
				// Get an MBeanServerConnection
				//
				Err("\nGet an MBeanServerConnection");
				mbsc = jmxc.GetMBeanServerConnection();
			}
			// Get domains from MBeanServer
			//
			Err("\nDomains:");
			string[] domains = mbsc.GetDomains();
			Arrays.Sort(domains);
			foreach (string domain in domains)
			{
				Err("\tDomain = " + domain);
			}
			// Get MBeanServer's default domain
			//
			Err("\nMBeanServer default domain = " + mbsc.GetDefaultDomain());
			// Get MBean count
			//
			Err("\nMBean count = " + mbsc.GetMBeanCount());
			// Query MBean names for specific domain "hadoop" and service
			ObjectName query = new ObjectName("Hadoop:service=" + service + ",*");
			hadoopObjectNames = new AList<ObjectName>(5);
			Err("\nQuery MBeanServer MBeans:");
			ICollection<ObjectName> names = new TreeSet<ObjectName>(mbsc.QueryNames(query, null
				));
			foreach (ObjectName name in names)
			{
				hadoopObjectNames.AddItem(name);
				Err("Hadoop service: " + name);
			}
		}

		/// <summary>Print JMXGet usage information</summary>
		internal static void PrintUsage(Options opts)
		{
			HelpFormatter formatter = new HelpFormatter();
			formatter.PrintHelp("jmxget options are: ", opts);
		}

		/// <param name="msg">error message</param>
		private static void Err(string msg)
		{
			System.Console.Error.WriteLine(msg);
		}

		/// <summary>parse args</summary>
		/// <exception cref="System.ArgumentException"/>
		private static CommandLine ParseArgs(Options opts, params string[] args)
		{
			OptionBuilder.WithArgName("NameNode|DataNode");
			OptionBuilder.HasArg();
			OptionBuilder.WithDescription("specify jmx service (NameNode by default)");
			Option jmx_service = OptionBuilder.Create("service");
			OptionBuilder.WithArgName("mbean server");
			OptionBuilder.HasArg();
			OptionBuilder.WithDescription("specify mbean server (localhost by default)");
			Option jmx_server = OptionBuilder.Create("server");
			OptionBuilder.WithDescription("print help");
			Option jmx_help = OptionBuilder.Create("help");
			OptionBuilder.WithArgName("mbean server port");
			OptionBuilder.HasArg();
			OptionBuilder.WithDescription("specify mbean server port, " + "if missing - it will try to connect to MBean Server in the same VM"
				);
			Option jmx_port = OptionBuilder.Create("port");
			OptionBuilder.WithArgName("VM's connector url");
			OptionBuilder.HasArg();
			OptionBuilder.WithDescription("connect to the VM on the same machine;" + "\n use:\n jstat -J-Djstat.showUnsupported=true -snap <vmpid> | "
				 + "grep sun.management.JMXConnectorServer.address\n " + "to find the url");
			Option jmx_localVM = OptionBuilder.Create("localVM");
			opts.AddOption(jmx_server);
			opts.AddOption(jmx_help);
			opts.AddOption(jmx_service);
			opts.AddOption(jmx_port);
			opts.AddOption(jmx_localVM);
			CommandLine commandLine = null;
			CommandLineParser parser = new GnuParser();
			try
			{
				commandLine = parser.Parse(opts, args, true);
			}
			catch (ParseException e)
			{
				PrintUsage(opts);
				throw new ArgumentException("invalid args: " + e.Message);
			}
			return commandLine;
		}

		public static void Main(string[] args)
		{
			int res = -1;
			// parse arguments
			Options opts = new Options();
			CommandLine commandLine = null;
			try
			{
				commandLine = ParseArgs(opts, args);
			}
			catch (ArgumentException)
			{
				commandLine = null;
			}
			if (commandLine == null)
			{
				// invalid arguments
				Err("Invalid args");
				PrintUsage(opts);
				ExitUtil.Terminate(-1);
			}
			Org.Apache.Hadoop.Hdfs.Tools.JMXGet jm = new Org.Apache.Hadoop.Hdfs.Tools.JMXGet(
				);
			if (commandLine.HasOption("port"))
			{
				jm.SetPort(commandLine.GetOptionValue("port"));
			}
			if (commandLine.HasOption("service"))
			{
				jm.SetService(commandLine.GetOptionValue("service"));
			}
			if (commandLine.HasOption("server"))
			{
				jm.SetServer(commandLine.GetOptionValue("server"));
			}
			if (commandLine.HasOption("localVM"))
			{
				// from the file /tmp/hsperfdata*
				jm.SetLocalVMUrl(commandLine.GetOptionValue("localVM"));
			}
			if (commandLine.HasOption("help"))
			{
				PrintUsage(opts);
				ExitUtil.Terminate(0);
			}
			// rest of args
			args = commandLine.GetArgs();
			try
			{
				jm.Init();
				if (args.Length == 0)
				{
					jm.PrintAllValues();
				}
				else
				{
					foreach (string key in args)
					{
						Err("key = " + key);
						string val = jm.GetValue(key);
						if (val != null)
						{
							System.Console.Out.Format(Org.Apache.Hadoop.Hdfs.Tools.JMXGet.format, key, val);
						}
					}
				}
				res = 0;
			}
			catch (Exception re)
			{
				Sharpen.Runtime.PrintStackTrace(re);
				res = -1;
			}
			ExitUtil.Terminate(res);
		}
	}
}
