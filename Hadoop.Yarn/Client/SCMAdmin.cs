using System;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class SCMAdmin : Configured, Tool
	{
		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		public SCMAdmin()
			: base()
		{
		}

		public SCMAdmin(Configuration conf)
			: base(conf)
		{
		}

		private static void PrintHelp(string cmd)
		{
			string summary = "scmadmin is the command to execute shared cache manager" + "administrative commands.\n"
				 + "The full syntax is: \n\n" + "hadoop scmadmin" + " [-runCleanerTask]" + " [-help [cmd]]\n";
			string runCleanerTask = "-runCleanerTask: Run cleaner task right away.\n";
			string help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n"
				 + "\t\tis specified.\n";
			if ("runCleanerTask".Equals(cmd))
			{
				System.Console.Out.WriteLine(runCleanerTask);
			}
			else
			{
				if ("help".Equals(cmd))
				{
					System.Console.Out.WriteLine(help);
				}
				else
				{
					System.Console.Out.WriteLine(summary);
					System.Console.Out.WriteLine(runCleanerTask);
					System.Console.Out.WriteLine(help);
					System.Console.Out.WriteLine();
					ToolRunner.PrintGenericCommandUsage(System.Console.Out);
				}
			}
		}

		/// <summary>Displays format of commands.</summary>
		/// <param name="cmd">The command that is being executed.</param>
		private static void PrintUsage(string cmd)
		{
			if ("-runCleanerTask".Equals(cmd))
			{
				System.Console.Error.WriteLine("Usage: yarn scmadmin" + " [-runCleanerTask]");
			}
			else
			{
				System.Console.Error.WriteLine("Usage: yarn scmadmin");
				System.Console.Error.WriteLine("           [-runCleanerTask]");
				System.Console.Error.WriteLine("           [-help [cmd]]");
				System.Console.Error.WriteLine();
				ToolRunner.PrintGenericCommandUsage(System.Console.Error);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual SCMAdminProtocol CreateSCMAdminProtocol()
		{
			// Get the current configuration
			YarnConfiguration conf = new YarnConfiguration(GetConf());
			// Create the admin client
			IPEndPoint addr = conf.GetSocketAddr(YarnConfiguration.ScmAdminAddress, YarnConfiguration
				.DefaultScmAdminAddress, YarnConfiguration.DefaultScmAdminPort);
			YarnRPC rpc = YarnRPC.Create(conf);
			SCMAdminProtocol scmAdminProtocol = (SCMAdminProtocol)rpc.GetProxy(typeof(SCMAdminProtocol
				), addr, conf);
			return scmAdminProtocol;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private int RunCleanerTask()
		{
			// run cleaner task right away
			SCMAdminProtocol scmAdminProtocol = CreateSCMAdminProtocol();
			RunSharedCacheCleanerTaskRequest request = recordFactory.NewRecordInstance<RunSharedCacheCleanerTaskRequest
				>();
			RunSharedCacheCleanerTaskResponse response = scmAdminProtocol.RunCleanerTask(request
				);
			if (response.GetAccepted())
			{
				System.Console.Out.WriteLine("request accepted by shared cache manager");
				return 0;
			}
			else
			{
				System.Console.Out.WriteLine("request rejected by shared cache manager");
				return 1;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length < 1)
			{
				PrintUsage(string.Empty);
				return -1;
			}
			int i = 0;
			string cmd = args[i++];
			try
			{
				if ("-runCleanerTask".Equals(cmd))
				{
					if (args.Length != 1)
					{
						PrintUsage(cmd);
						return -1;
					}
					else
					{
						return RunCleanerTask();
					}
				}
				else
				{
					if ("-help".Equals(cmd))
					{
						if (i < args.Length)
						{
							PrintUsage(args[i]);
						}
						else
						{
							PrintHelp(string.Empty);
						}
						return 0;
					}
					else
					{
						System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": Unknown command"
							);
						PrintUsage(string.Empty);
						return -1;
					}
				}
			}
			catch (ArgumentException arge)
			{
				System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": " + arge.GetLocalizedMessage
					());
				PrintUsage(cmd);
			}
			catch (RemoteException e)
			{
				//
				// This is a error returned by hadoop server. Print
				// out the first line of the error message, ignore the stack trace.
				try
				{
					string[] content;
					content = e.GetLocalizedMessage().Split("\n");
					System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": " + content
						[0]);
				}
				catch (Exception ex)
				{
					System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": " + ex.GetLocalizedMessage
						());
				}
			}
			catch (Exception e)
			{
				System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": " + e.GetLocalizedMessage
					());
			}
			return -1;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int result = ToolRunner.Run(new Org.Apache.Hadoop.Yarn.Client.SCMAdmin(), args);
			System.Environment.Exit(result);
		}
	}
}
