/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.Cli;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Registry.Client.Api;
using Org.Apache.Hadoop.Registry.Client.Binding;
using Org.Apache.Hadoop.Registry.Client.Exceptions;
using Org.Apache.Hadoop.Registry.Client.Types;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Cli
{
	/// <summary>Command line for registry operations.</summary>
	public class RegistryCli : Configured, Tool, IDisposable
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Registry.Cli.RegistryCli
			));

		protected internal readonly TextWriter sysout;

		protected internal readonly TextWriter syserr;

		private RegistryOperations registry;

		private const string LsUsage = "ls pathName";

		private const string ResolveUsage = "resolve pathName";

		private const string BindUsage = "bind -inet  -api apiName -p portNumber -h hostName  pathName"
			 + "\n" + "bind -webui uriString -api apiName  pathName" + "\n" + "bind -rest uriString -api apiName  pathName";

		private const string MknodeUsage = "mknode directoryName";

		private const string RmUsage = "rm pathName";

		private const string Usage = "\n" + LsUsage + "\n" + ResolveUsage + "\n" + BindUsage
			 + "\n" + MknodeUsage + "\n" + RmUsage;

		public RegistryCli(TextWriter sysout, TextWriter syserr)
		{
			Configuration conf = new Configuration();
			base.SetConf(conf);
			registry = RegistryOperationsFactory.CreateInstance(conf);
			registry.Start();
			this.sysout = sysout;
			this.syserr = syserr;
		}

		public RegistryCli(RegistryOperations reg, Configuration conf, TextWriter sysout, 
			TextWriter syserr)
			: base(conf)
		{
			Preconditions.CheckArgument(reg != null, "Null registry");
			registry = reg;
			this.sysout = sysout;
			this.syserr = syserr;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = -1;
			try
			{
				using (Org.Apache.Hadoop.Registry.Cli.RegistryCli cli = new Org.Apache.Hadoop.Registry.Cli.RegistryCli
					(System.Console.Out, System.Console.Error))
				{
					res = ToolRunner.Run(cli, args);
				}
			}
			catch (Exception e)
			{
				ExitUtil.Terminate(res, e);
			}
			ExitUtil.Terminate(res);
		}

		/// <summary>Close the object by stopping the registry.</summary>
		/// <remarks>
		/// Close the object by stopping the registry.
		/// <p>
		/// <i>Important:</i>
		/// <p>
		/// After this call is made, no operations may be made of this
		/// object, <i>or of a YARN registry instance used when constructing
		/// this object. </i>
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			ServiceOperations.StopQuietly(registry);
			registry = null;
		}

		private int UsageError(string err, string usage)
		{
			syserr.WriteLine("Error: " + err);
			syserr.WriteLine("Usage: " + usage);
			return -1;
		}

		private bool ValidatePath(string path)
		{
			if (!path.StartsWith("/"))
			{
				syserr.WriteLine("Path must start with /; given path was: " + path);
				return false;
			}
			return true;
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Preconditions.CheckArgument(GetConf() != null, "null configuration");
			if (args.Length > 0)
			{
				switch (args[0])
				{
					case "ls":
					{
						return Ls(args);
					}

					case "resolve":
					{
						return Resolve(args);
					}

					case "bind":
					{
						return Bind(args);
					}

					case "mknode":
					{
						return Mknode(args);
					}

					case "rm":
					{
						return Rm(args);
					}

					default:
					{
						return UsageError("Invalid command: " + args[0], Usage);
					}
				}
			}
			return UsageError("No command arg passed.", Usage);
		}

		public virtual int Ls(string[] args)
		{
			Options lsOption = new Options();
			CommandLineParser parser = new GnuParser();
			try
			{
				CommandLine line = parser.Parse(lsOption, args);
				IList<string> argsList = line.GetArgList();
				if (argsList.Count != 2)
				{
					return UsageError("ls requires exactly one path argument", LsUsage);
				}
				if (!ValidatePath(argsList[1]))
				{
					return -1;
				}
				try
				{
					IList<string> children = registry.List(argsList[1]);
					foreach (string child in children)
					{
						sysout.WriteLine(child);
					}
					return 0;
				}
				catch (Exception e)
				{
					syserr.WriteLine(AnalyzeException("ls", e, argsList));
				}
				return -1;
			}
			catch (ParseException exp)
			{
				return UsageError("Invalid syntax " + exp, LsUsage);
			}
		}

		public virtual int Resolve(string[] args)
		{
			Options resolveOption = new Options();
			CommandLineParser parser = new GnuParser();
			try
			{
				CommandLine line = parser.Parse(resolveOption, args);
				IList<string> argsList = line.GetArgList();
				if (argsList.Count != 2)
				{
					return UsageError("resolve requires exactly one path argument", ResolveUsage);
				}
				if (!ValidatePath(argsList[1]))
				{
					return -1;
				}
				try
				{
					ServiceRecord record = registry.Resolve(argsList[1]);
					foreach (Endpoint endpoint in record.external)
					{
						sysout.WriteLine(" Endpoint(ProtocolType=" + endpoint.protocolType + ", Api=" + endpoint
							.api + ");" + " Addresses(AddressType=" + endpoint.addressType + ") are: ");
						foreach (IDictionary<string, string> address in endpoint.addresses)
						{
							sysout.WriteLine("[ ");
							foreach (KeyValuePair<string, string> entry in address)
							{
								sysout.Write("\t" + entry.Key + ":" + entry.Value);
							}
							sysout.WriteLine("\n]");
						}
						sysout.WriteLine();
					}
					return 0;
				}
				catch (Exception e)
				{
					syserr.WriteLine(AnalyzeException("resolve", e, argsList));
				}
				return -1;
			}
			catch (ParseException exp)
			{
				return UsageError("Invalid syntax " + exp, ResolveUsage);
			}
		}

		public virtual int Bind(string[] args)
		{
			Option rest = OptionBuilder.Create("rest");
			Option webui = OptionBuilder.Create("webui");
			Option inet = OptionBuilder.Create("inet");
			Option port = OptionBuilder.Create("p");
			Option host = OptionBuilder.Create("h");
			Option apiOpt = OptionBuilder.Create("api");
			Options inetOption = new Options();
			inetOption.AddOption(inet);
			inetOption.AddOption(port);
			inetOption.AddOption(host);
			inetOption.AddOption(apiOpt);
			Options webuiOpt = new Options();
			webuiOpt.AddOption(webui);
			webuiOpt.AddOption(apiOpt);
			Options restOpt = new Options();
			restOpt.AddOption(rest);
			restOpt.AddOption(apiOpt);
			CommandLineParser parser = new GnuParser();
			ServiceRecord sr = new ServiceRecord();
			CommandLine line;
			if (args.Length <= 1)
			{
				return UsageError("Invalid syntax ", BindUsage);
			}
			if (args[1].Equals("-inet"))
			{
				int portNum;
				string hostName;
				string api;
				try
				{
					line = parser.Parse(inetOption, args);
				}
				catch (ParseException exp)
				{
					return UsageError("Invalid syntax " + exp.Message, BindUsage);
				}
				if (line.HasOption("inet") && line.HasOption("p") && line.HasOption("h") && line.
					HasOption("api"))
				{
					try
					{
						portNum = System.Convert.ToInt32(line.GetOptionValue("p"));
					}
					catch (FormatException exp)
					{
						return UsageError("Invalid Port - int required" + exp.Message, BindUsage);
					}
					hostName = line.GetOptionValue("h");
					api = line.GetOptionValue("api");
					sr.AddExternalEndpoint(RegistryTypeUtils.InetAddrEndpoint(api, ProtocolTypes.ProtocolHadoopIpc
						, hostName, portNum));
				}
				else
				{
					return UsageError("Missing options: must have host, port and api", BindUsage);
				}
			}
			else
			{
				if (args[1].Equals("-webui"))
				{
					try
					{
						line = parser.Parse(webuiOpt, args);
					}
					catch (ParseException exp)
					{
						return UsageError("Invalid syntax " + exp.Message, BindUsage);
					}
					if (line.HasOption("webui") && line.HasOption("api"))
					{
						URI theUri;
						try
						{
							theUri = new URI(line.GetOptionValue("webui"));
						}
						catch (URISyntaxException e)
						{
							return UsageError("Invalid URI: " + e.Message, BindUsage);
						}
						sr.AddExternalEndpoint(RegistryTypeUtils.WebEndpoint(line.GetOptionValue("api"), 
							theUri));
					}
					else
					{
						return UsageError("Missing options: must have value for uri and api", BindUsage);
					}
				}
				else
				{
					if (args[1].Equals("-rest"))
					{
						try
						{
							line = parser.Parse(restOpt, args);
						}
						catch (ParseException exp)
						{
							return UsageError("Invalid syntax " + exp.Message, BindUsage);
						}
						if (line.HasOption("rest") && line.HasOption("api"))
						{
							URI theUri = null;
							try
							{
								theUri = new URI(line.GetOptionValue("rest"));
							}
							catch (URISyntaxException e)
							{
								return UsageError("Invalid URI: " + e.Message, BindUsage);
							}
							sr.AddExternalEndpoint(RegistryTypeUtils.RestEndpoint(line.GetOptionValue("api"), 
								theUri));
						}
						else
						{
							return UsageError("Missing options: must have value for uri and api", BindUsage);
						}
					}
					else
					{
						return UsageError("Invalid syntax", BindUsage);
					}
				}
			}
			IList<string> argsList = line.GetArgList();
			if (argsList.Count != 2)
			{
				return UsageError("bind requires exactly one path argument", BindUsage);
			}
			if (!ValidatePath(argsList[1]))
			{
				return -1;
			}
			try
			{
				registry.Bind(argsList[1], sr, BindFlags.Overwrite);
				return 0;
			}
			catch (Exception e)
			{
				syserr.WriteLine(AnalyzeException("bind", e, argsList));
			}
			return -1;
		}

		public virtual int Mknode(string[] args)
		{
			Options mknodeOption = new Options();
			CommandLineParser parser = new GnuParser();
			try
			{
				CommandLine line = parser.Parse(mknodeOption, args);
				IList<string> argsList = line.GetArgList();
				if (argsList.Count != 2)
				{
					return UsageError("mknode requires exactly one path argument", MknodeUsage);
				}
				if (!ValidatePath(argsList[1]))
				{
					return -1;
				}
				try
				{
					registry.Mknode(args[1], false);
					return 0;
				}
				catch (Exception e)
				{
					syserr.WriteLine(AnalyzeException("mknode", e, argsList));
				}
				return -1;
			}
			catch (ParseException exp)
			{
				return UsageError("Invalid syntax " + exp.ToString(), MknodeUsage);
			}
		}

		public virtual int Rm(string[] args)
		{
			Option recursive = OptionBuilder.Create("r");
			Options rmOption = new Options();
			rmOption.AddOption(recursive);
			bool recursiveOpt = false;
			CommandLineParser parser = new GnuParser();
			try
			{
				CommandLine line = parser.Parse(rmOption, args);
				IList<string> argsList = line.GetArgList();
				if (argsList.Count != 2)
				{
					return UsageError("RM requires exactly one path argument", RmUsage);
				}
				if (!ValidatePath(argsList[1]))
				{
					return -1;
				}
				try
				{
					if (line.HasOption("r"))
					{
						recursiveOpt = true;
					}
					registry.Delete(argsList[1], recursiveOpt);
					return 0;
				}
				catch (Exception e)
				{
					syserr.WriteLine(AnalyzeException("rm", e, argsList));
				}
				return -1;
			}
			catch (ParseException exp)
			{
				return UsageError("Invalid syntax " + exp.ToString(), RmUsage);
			}
		}

		/// <summary>
		/// Given an exception and a possibly empty argument list, generate
		/// a diagnostics string for use in error messages
		/// </summary>
		/// <param name="operation">the operation that failed</param>
		/// <param name="e">exception</param>
		/// <param name="argsList">arguments list</param>
		/// <returns>a string intended for the user</returns>
		internal virtual string AnalyzeException(string operation, Exception e, IList<string
			> argsList)
		{
			string pathArg = !argsList.IsEmpty() ? argsList[1] : "(none)";
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Operation {} on path {} failed with exception {}", operation, pathArg, 
					e, e);
			}
			if (e is InvalidPathnameException)
			{
				return "InvalidPath :" + pathArg + ": " + e;
			}
			if (e is PathNotFoundException)
			{
				return "Path not found: " + pathArg;
			}
			if (e is NoRecordException)
			{
				return "No service record at path " + pathArg;
			}
			if (e is AuthenticationFailedException)
			{
				return "Failed to authenticate to registry : " + e;
			}
			if (e is NoPathPermissionsException)
			{
				return "No Permission to path: " + pathArg + ": " + e;
			}
			if (e is AccessControlException)
			{
				return "No Permission to path: " + pathArg + ": " + e;
			}
			if (e is InvalidRecordException)
			{
				return "Unable to read record at: " + pathArg + ": " + e;
			}
			if (e is IOException)
			{
				return "IO Exception when accessing path :" + pathArg + ": " + e;
			}
			// something else went very wrong here
			return "Exception " + e;
		}
	}
}
