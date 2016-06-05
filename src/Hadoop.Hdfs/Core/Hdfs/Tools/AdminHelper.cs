using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Tools;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>Helper methods for CacheAdmin/CryptoAdmin/StoragePolicyAdmin</summary>
	public class AdminHelper
	{
		/// <summary>Maximum length for printed lines</summary>
		internal const int MaxLineWidth = 80;

		internal const string HelpCommandName = "-help";

		/// <exception cref="System.IO.IOException"/>
		internal static DistributedFileSystem GetDFS(Configuration conf)
		{
			FileSystem fs = FileSystem.Get(conf);
			if (!(fs is DistributedFileSystem))
			{
				throw new ArgumentException("FileSystem " + fs.GetUri() + " is not an HDFS file system"
					);
			}
			return (DistributedFileSystem)fs;
		}

		/// <summary>NN exceptions contain the stack trace as part of the exception message.</summary>
		/// <remarks>
		/// NN exceptions contain the stack trace as part of the exception message.
		/// When it's a known error, pretty-print the error and squish the stack trace.
		/// </remarks>
		internal static string PrettifyException(Exception e)
		{
			return e.GetType().Name + ": " + e.GetLocalizedMessage().Split("\n")[0];
		}

		internal static TableListing GetOptionDescriptionListing()
		{
			return new TableListing.Builder().AddField(string.Empty).AddField(string.Empty, true
				).WrapWidth(MaxLineWidth).HideHeaders().Build();
		}

		/// <summary>Parses a time-to-live value from a string</summary>
		/// <returns>The ttl in milliseconds</returns>
		/// <exception cref="System.IO.IOException">if it could not be parsed</exception>
		internal static long ParseTtlString(string maxTtlString)
		{
			long maxTtl = null;
			if (maxTtlString != null)
			{
				if (Sharpen.Runtime.EqualsIgnoreCase(maxTtlString, "never"))
				{
					maxTtl = CachePoolInfo.RelativeExpiryNever;
				}
				else
				{
					maxTtl = DFSUtil.ParseRelativeTime(maxTtlString);
				}
			}
			return maxTtl;
		}

		internal static long ParseLimitString(string limitString)
		{
			long limit = null;
			if (limitString != null)
			{
				if (Sharpen.Runtime.EqualsIgnoreCase(limitString, "unlimited"))
				{
					limit = CachePoolInfo.LimitUnlimited;
				}
				else
				{
					limit = long.Parse(limitString);
				}
			}
			return limit;
		}

		internal static AdminHelper.Command DetermineCommand(string commandName, AdminHelper.Command
			[] commands)
		{
			Preconditions.CheckNotNull(commands);
			if (HelpCommandName.Equals(commandName))
			{
				return new AdminHelper.HelpCommand(commands);
			}
			foreach (AdminHelper.Command command in commands)
			{
				if (command.GetName().Equals(commandName))
				{
					return command;
				}
			}
			return null;
		}

		internal static void PrintUsage(bool longUsage, string toolName, AdminHelper.Command
			[] commands)
		{
			Preconditions.CheckNotNull(commands);
			System.Console.Error.WriteLine("Usage: bin/hdfs " + toolName + " [COMMAND]");
			AdminHelper.HelpCommand helpCommand = new AdminHelper.HelpCommand(commands);
			foreach (AdminHelper.Command command in commands)
			{
				if (longUsage)
				{
					System.Console.Error.Write(command.GetLongUsage());
				}
				else
				{
					System.Console.Error.Write("          " + command.GetShortUsage());
				}
			}
			System.Console.Error.Write(longUsage ? helpCommand.GetLongUsage() : ("          "
				 + helpCommand.GetShortUsage()));
			System.Console.Error.WriteLine();
		}

		internal interface Command
		{
			string GetName();

			string GetShortUsage();

			string GetLongUsage();

			/// <exception cref="System.IO.IOException"/>
			int Run(Configuration conf, IList<string> args);
		}

		internal class HelpCommand : AdminHelper.Command
		{
			private readonly AdminHelper.Command[] commands;

			public HelpCommand(AdminHelper.Command[] commands)
			{
				Preconditions.CheckNotNull(commands != null);
				this.commands = commands;
			}

			public virtual string GetName()
			{
				return HelpCommandName;
			}

			public virtual string GetShortUsage()
			{
				return "[-help <command-name>]\n";
			}

			public virtual string GetLongUsage()
			{
				TableListing listing = AdminHelper.GetOptionDescriptionListing();
				listing.AddRow("<command-name>", "The command for which to get " + "detailed help. If no command is specified, print detailed help for "
					 + "all commands");
				return GetShortUsage() + "\n" + "Get detailed help about a command.\n\n" + listing
					.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				if (args.Count == 0)
				{
					foreach (AdminHelper.Command command in commands)
					{
						System.Console.Error.WriteLine(command.GetLongUsage());
					}
					return 0;
				}
				if (args.Count != 1)
				{
					System.Console.Out.WriteLine("You must give exactly one argument to -help.");
					return 0;
				}
				string commandName = args[0];
				// prepend a dash to match against the command names
				AdminHelper.Command command_1 = AdminHelper.DetermineCommand("-" + commandName, commands
					);
				if (command_1 == null)
				{
					System.Console.Error.Write("Unknown command '" + commandName + "'.\n");
					System.Console.Error.Write("Valid help command names are:\n");
					string separator = string.Empty;
					foreach (AdminHelper.Command c in commands)
					{
						System.Console.Error.Write(separator + Sharpen.Runtime.Substring(c.GetName(), 1));
						separator = ", ";
					}
					System.Console.Error.Write("\n");
					return 1;
				}
				System.Console.Error.Write(command_1.GetLongUsage());
				return 0;
			}
		}
	}
}
