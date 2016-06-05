using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>This class implements crypto command-line operations.</summary>
	public class CryptoAdmin : Configured, Tool
	{
		public CryptoAdmin()
			: this(null)
		{
		}

		public CryptoAdmin(Configuration conf)
			: base(conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Run(string[] args)
		{
			if (args.Length == 0)
			{
				AdminHelper.PrintUsage(false, "crypto", Commands);
				return 1;
			}
			AdminHelper.Command command = AdminHelper.DetermineCommand(args[0], Commands);
			if (command == null)
			{
				System.Console.Error.WriteLine("Can't understand command '" + args[0] + "'");
				if (!args[0].StartsWith("-"))
				{
					System.Console.Error.WriteLine("Command names must start with dashes.");
				}
				AdminHelper.PrintUsage(false, "crypto", Commands);
				return 1;
			}
			IList<string> argsList = new List<string>();
			for (int j = 1; j < args.Length; j++)
			{
				argsList.AddItem(args[j]);
			}
			try
			{
				return command.Run(GetConf(), argsList);
			}
			catch (ArgumentException e)
			{
				System.Console.Error.WriteLine(PrettifyException(e));
				return -1;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] argsArray)
		{
			Org.Apache.Hadoop.Hdfs.Tools.CryptoAdmin cryptoAdmin = new Org.Apache.Hadoop.Hdfs.Tools.CryptoAdmin
				(new Configuration());
			System.Environment.Exit(cryptoAdmin.Run(argsArray));
		}

		/// <summary>NN exceptions contain the stack trace as part of the exception message.</summary>
		/// <remarks>
		/// NN exceptions contain the stack trace as part of the exception message.
		/// When it's a known error, pretty-print the error and squish the stack trace.
		/// </remarks>
		private static string PrettifyException(Exception e)
		{
			return e.GetType().Name + ": " + e.GetLocalizedMessage().Split("\n")[0];
		}

		private class CreateZoneCommand : AdminHelper.Command
		{
			public virtual string GetName()
			{
				return "-createZone";
			}

			public virtual string GetShortUsage()
			{
				return "[" + GetName() + " -keyName <keyName> -path <path>]\n";
			}

			public virtual string GetLongUsage()
			{
				TableListing listing = AdminHelper.GetOptionDescriptionListing();
				listing.AddRow("<path>", "The path of the encryption zone to create. " + "It must be an empty directory."
					);
				listing.AddRow("<keyName>", "Name of the key to use for the " + "encryption zone."
					);
				return GetShortUsage() + "\n" + "Create a new encryption zone.\n\n" + listing.ToString
					();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				string path = StringUtils.PopOptionWithArgument("-path", args);
				if (path == null)
				{
					System.Console.Error.WriteLine("You must specify a path with -path.");
					return 1;
				}
				string keyName = StringUtils.PopOptionWithArgument("-keyName", args);
				if (keyName == null)
				{
					System.Console.Error.WriteLine("You must specify a key name with -keyName.");
					return 1;
				}
				if (!args.IsEmpty())
				{
					System.Console.Error.WriteLine("Can't understand argument: " + args[0]);
					return 1;
				}
				DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
				try
				{
					dfs.CreateEncryptionZone(new Path(path), keyName);
					System.Console.Out.WriteLine("Added encryption zone " + path);
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine(PrettifyException(e));
					return 2;
				}
				return 0;
			}
		}

		private class ListZonesCommand : AdminHelper.Command
		{
			public virtual string GetName()
			{
				return "-listZones";
			}

			public virtual string GetShortUsage()
			{
				return "[" + GetName() + "]\n";
			}

			public virtual string GetLongUsage()
			{
				return GetShortUsage() + "\n" + "List all encryption zones. Requires superuser permissions.\n\n";
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Run(Configuration conf, IList<string> args)
			{
				if (!args.IsEmpty())
				{
					System.Console.Error.WriteLine("Can't understand argument: " + args[0]);
					return 1;
				}
				DistributedFileSystem dfs = AdminHelper.GetDFS(conf);
				try
				{
					TableListing listing = new TableListing.Builder().AddField(string.Empty).AddField
						(string.Empty, true).WrapWidth(AdminHelper.MaxLineWidth).HideHeaders().Build();
					RemoteIterator<EncryptionZone> it = dfs.ListEncryptionZones();
					while (it.HasNext())
					{
						EncryptionZone ez = it.Next();
						listing.AddRow(ez.GetPath(), ez.GetKeyName());
					}
					System.Console.Out.WriteLine(listing.ToString());
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine(PrettifyException(e));
					return 2;
				}
				return 0;
			}
		}

		private static readonly AdminHelper.Command[] Commands = new AdminHelper.Command[
			] { new CryptoAdmin.CreateZoneCommand(), new CryptoAdmin.ListZonesCommand() };
	}
}
