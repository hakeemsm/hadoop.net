using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Cli
{
	public class RMAdminCLI : HAAdmin
	{
		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private bool directlyAccessNodeLabelStore = false;

		internal static CommonNodeLabelsManager localNodeLabelsManager = null;

		private const string NoLabelErrMsg = "No cluster node-labels are specified";

		private const string NoMappingErrMsg = "No node-to-labels mappings are specified";

		protected internal static readonly IDictionary<string, HAAdmin.UsageInfo> AdminUsage
			 = ImmutableMap.Builder<string, HAAdmin.UsageInfo>().Put("-refreshQueues", new HAAdmin.UsageInfo
			(string.Empty, "Reload the queues' acls, states and scheduler specific " + "properties. \n\t\tResourceManager will reload the "
			 + "mapred-queues configuration file.")).Put("-refreshNodes", new HAAdmin.UsageInfo
			(string.Empty, "Refresh the hosts information at the ResourceManager.")).Put("-refreshSuperUserGroupsConfiguration"
			, new HAAdmin.UsageInfo(string.Empty, "Refresh superuser proxy groups mappings")
			).Put("-refreshUserToGroupsMappings", new HAAdmin.UsageInfo(string.Empty, "Refresh user-to-groups mappings"
			)).Put("-refreshAdminAcls", new HAAdmin.UsageInfo(string.Empty, "Refresh acls for administration of ResourceManager"
			)).Put("-refreshServiceAcl", new HAAdmin.UsageInfo(string.Empty, "Reload the service-level authorization policy file. \n\t\t"
			 + "ResoureceManager will reload the authorization policy file.")).Put("-getGroups"
			, new HAAdmin.UsageInfo("[username]", "Get the groups which given user belongs to."
			)).Put("-addToClusterNodeLabels", new HAAdmin.UsageInfo("[label1,label2,label3] (label splitted by \",\")"
			, "add to cluster node labels ")).Put("-removeFromClusterNodeLabels", new HAAdmin.UsageInfo
			("[label1,label2,label3] (label splitted by \",\")", "remove from cluster node labels"
			)).Put("-replaceLabelsOnNode", new HAAdmin.UsageInfo("[node1[:port]=label1,label2 node2[:port]=label1,label2]"
			, "replace labels on nodes" + " (please note that we do not support specifying multiple"
			 + " labels on a single host for now.)")).Put("-directlyAccessNodeLabelStore", new 
			HAAdmin.UsageInfo(string.Empty, "Directly access node label store, " + "with this option, all node label related operations"
			 + " will not connect RM. Instead, they will" + " access/modify stored node labels directly."
			 + " By default, it is false (access via RM)." + " AND PLEASE NOTE: if you configured"
			 + " yarn.node-labels.fs-store.root-dir to a local directory" + " (instead of NFS or HDFS), this option will only work"
			 + " when the command run on the machine where RM is running.")).Build();

		public RMAdminCLI()
			: base()
		{
		}

		public RMAdminCLI(Configuration conf)
			: base(conf)
		{
		}

		protected internal virtual void SetErrOut(TextWriter errOut)
		{
			this.errOut = errOut;
		}

		private static void AppendHAUsage(StringBuilder usageBuilder)
		{
			foreach (KeyValuePair<string, HAAdmin.UsageInfo> cmdEntry in Usage)
			{
				if (cmdEntry.Key.Equals("-help"))
				{
					continue;
				}
				HAAdmin.UsageInfo usageInfo = cmdEntry.Value;
				usageBuilder.Append(" [" + cmdEntry.Key + " " + usageInfo.args + "]");
			}
		}

		private static void BuildHelpMsg(string cmd, StringBuilder builder)
		{
			HAAdmin.UsageInfo usageInfo = AdminUsage[cmd];
			if (usageInfo == null)
			{
				usageInfo = Usage[cmd];
				if (usageInfo == null)
				{
					return;
				}
			}
			string space = (usageInfo.args == string.Empty) ? string.Empty : " ";
			builder.Append("   " + cmd + space + usageInfo.args + ": " + usageInfo.help);
		}

		private static void BuildIndividualUsageMsg(string cmd, StringBuilder builder)
		{
			bool isHACommand = false;
			HAAdmin.UsageInfo usageInfo = AdminUsage[cmd];
			if (usageInfo == null)
			{
				usageInfo = Usage[cmd];
				if (usageInfo == null)
				{
					return;
				}
				isHACommand = true;
			}
			string space = (usageInfo.args == string.Empty) ? string.Empty : " ";
			builder.Append("Usage: yarn rmadmin [" + cmd + space + usageInfo.args + "]\n");
			if (isHACommand)
			{
				builder.Append(cmd + " can only be used when RM HA is enabled");
			}
		}

		private static void BuildUsageMsg(StringBuilder builder, bool isHAEnabled)
		{
			builder.Append("Usage: yarn rmadmin\n");
			foreach (KeyValuePair<string, HAAdmin.UsageInfo> cmdEntry in AdminUsage)
			{
				HAAdmin.UsageInfo usageInfo = cmdEntry.Value;
				builder.Append("   " + cmdEntry.Key + " " + usageInfo.args + "\n");
			}
			if (isHAEnabled)
			{
				foreach (KeyValuePair<string, HAAdmin.UsageInfo> cmdEntry_1 in Usage)
				{
					string cmdKey = cmdEntry_1.Key;
					if (!cmdKey.Equals("-help"))
					{
						HAAdmin.UsageInfo usageInfo = cmdEntry_1.Value;
						builder.Append("   " + cmdKey + " " + usageInfo.args + "\n");
					}
				}
			}
			builder.Append("   -help" + " [cmd]\n");
		}

		private static void PrintHelp(string cmd, bool isHAEnabled)
		{
			StringBuilder summary = new StringBuilder();
			summary.Append("rmadmin is the command to execute YARN administrative " + "commands.\n"
				);
			summary.Append("The full syntax is: \n\n" + "yarn rmadmin" + " [-refreshQueues]" 
				+ " [-refreshNodes]" + " [-refreshSuperUserGroupsConfiguration]" + " [-refreshUserToGroupsMappings]"
				 + " [-refreshAdminAcls]" + " [-refreshServiceAcl]" + " [-getGroup [username]]" 
				+ " [[-addToClusterNodeLabels [label1,label2,label3]]" + " [-removeFromClusterNodeLabels [label1,label2,label3]]"
				 + " [-replaceLabelsOnNode [node1[:port]=label1,label2 node2[:port]=label1]" + " [-directlyAccessNodeLabelStore]]"
				);
			if (isHAEnabled)
			{
				AppendHAUsage(summary);
			}
			summary.Append(" [-help [cmd]]");
			summary.Append("\n");
			StringBuilder helpBuilder = new StringBuilder();
			System.Console.Out.WriteLine(summary);
			foreach (string cmdKey in AdminUsage.Keys)
			{
				BuildHelpMsg(cmdKey, helpBuilder);
				helpBuilder.Append("\n");
			}
			if (isHAEnabled)
			{
				foreach (string cmdKey_1 in Usage.Keys)
				{
					if (!cmdKey_1.Equals("-help"))
					{
						BuildHelpMsg(cmdKey_1, helpBuilder);
						helpBuilder.Append("\n");
					}
				}
			}
			helpBuilder.Append("   -help [cmd]: Displays help for the given command or all commands"
				 + " if none is specified.");
			System.Console.Out.WriteLine(helpBuilder);
			System.Console.Out.WriteLine();
			ToolRunner.PrintGenericCommandUsage(System.Console.Out);
		}

		/// <summary>Displays format of commands.</summary>
		/// <param name="cmd">The command that is being executed.</param>
		private static void PrintUsage(string cmd, bool isHAEnabled)
		{
			StringBuilder usageBuilder = new StringBuilder();
			if (AdminUsage.Contains(cmd) || Usage.Contains(cmd))
			{
				BuildIndividualUsageMsg(cmd, usageBuilder);
			}
			else
			{
				BuildUsageMsg(usageBuilder, isHAEnabled);
			}
			System.Console.Error.WriteLine(usageBuilder);
			ToolRunner.PrintGenericCommandUsage(System.Console.Error);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual ResourceManagerAdministrationProtocol CreateAdminProtocol
			()
		{
			// Get the current configuration
			YarnConfiguration conf = new YarnConfiguration(GetConf());
			return ClientRMProxy.CreateRMProxy<ResourceManagerAdministrationProtocol>(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private int RefreshQueues()
		{
			// Refresh the queue properties
			ResourceManagerAdministrationProtocol adminProtocol = CreateAdminProtocol();
			RefreshQueuesRequest request = recordFactory.NewRecordInstance<RefreshQueuesRequest
				>();
			adminProtocol.RefreshQueues(request);
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private int RefreshNodes()
		{
			// Refresh the nodes
			ResourceManagerAdministrationProtocol adminProtocol = CreateAdminProtocol();
			RefreshNodesRequest request = recordFactory.NewRecordInstance<RefreshNodesRequest
				>();
			adminProtocol.RefreshNodes(request);
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private int RefreshUserToGroupsMappings()
		{
			// Refresh the user-to-groups mappings
			ResourceManagerAdministrationProtocol adminProtocol = CreateAdminProtocol();
			RefreshUserToGroupsMappingsRequest request = recordFactory.NewRecordInstance<RefreshUserToGroupsMappingsRequest
				>();
			adminProtocol.RefreshUserToGroupsMappings(request);
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private int RefreshSuperUserGroupsConfiguration()
		{
			// Refresh the super-user groups
			ResourceManagerAdministrationProtocol adminProtocol = CreateAdminProtocol();
			RefreshSuperUserGroupsConfigurationRequest request = recordFactory.NewRecordInstance
				<RefreshSuperUserGroupsConfigurationRequest>();
			adminProtocol.RefreshSuperUserGroupsConfiguration(request);
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private int RefreshAdminAcls()
		{
			// Refresh the admin acls
			ResourceManagerAdministrationProtocol adminProtocol = CreateAdminProtocol();
			RefreshAdminAclsRequest request = recordFactory.NewRecordInstance<RefreshAdminAclsRequest
				>();
			adminProtocol.RefreshAdminAcls(request);
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private int RefreshServiceAcls()
		{
			// Refresh the service acls
			ResourceManagerAdministrationProtocol adminProtocol = CreateAdminProtocol();
			RefreshServiceAclsRequest request = recordFactory.NewRecordInstance<RefreshServiceAclsRequest
				>();
			adminProtocol.RefreshServiceAcls(request);
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private int GetGroups(string[] usernames)
		{
			// Get groups users belongs to
			ResourceManagerAdministrationProtocol adminProtocol = CreateAdminProtocol();
			if (usernames.Length == 0)
			{
				usernames = new string[] { UserGroupInformation.GetCurrentUser().GetUserName() };
			}
			foreach (string username in usernames)
			{
				StringBuilder sb = new StringBuilder();
				sb.Append(username + " :");
				foreach (string group in adminProtocol.GetGroupsForUser(username))
				{
					sb.Append(" ");
					sb.Append(group);
				}
				System.Console.Out.WriteLine(sb);
			}
			return 0;
		}

		// Make it protected to make unit test can change it.
		protected internal static CommonNodeLabelsManager GetNodeLabelManagerInstance(Configuration
			 conf)
		{
			lock (typeof(RMAdminCLI))
			{
				if (localNodeLabelsManager == null)
				{
					localNodeLabelsManager = new CommonNodeLabelsManager();
					localNodeLabelsManager.Init(conf);
					localNodeLabelsManager.Start();
				}
				return localNodeLabelsManager;
			}
		}

		private ICollection<string> BuildNodeLabelsSetFromStr(string args)
		{
			ICollection<string> labels = new HashSet<string>();
			foreach (string p in args.Split(","))
			{
				if (!p.Trim().IsEmpty())
				{
					labels.AddItem(p.Trim());
				}
			}
			if (labels.IsEmpty())
			{
				throw new ArgumentException(NoLabelErrMsg);
			}
			return labels;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private int AddToClusterNodeLabels(string args)
		{
			ICollection<string> labels = BuildNodeLabelsSetFromStr(args);
			if (directlyAccessNodeLabelStore)
			{
				GetNodeLabelManagerInstance(GetConf()).AddToCluserNodeLabels(labels);
			}
			else
			{
				ResourceManagerAdministrationProtocol adminProtocol = CreateAdminProtocol();
				AddToClusterNodeLabelsRequest request = AddToClusterNodeLabelsRequest.NewInstance
					(labels);
				adminProtocol.AddToClusterNodeLabels(request);
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private int RemoveFromClusterNodeLabels(string args)
		{
			ICollection<string> labels = BuildNodeLabelsSetFromStr(args);
			if (directlyAccessNodeLabelStore)
			{
				GetNodeLabelManagerInstance(GetConf()).RemoveFromClusterNodeLabels(labels);
			}
			else
			{
				ResourceManagerAdministrationProtocol adminProtocol = CreateAdminProtocol();
				RemoveFromClusterNodeLabelsRequest request = RemoveFromClusterNodeLabelsRequest.NewInstance
					(labels);
				adminProtocol.RemoveFromClusterNodeLabels(request);
			}
			return 0;
		}

		private IDictionary<NodeId, ICollection<string>> BuildNodeLabelsMapFromStr(string
			 args)
		{
			IDictionary<NodeId, ICollection<string>> map = new Dictionary<NodeId, ICollection
				<string>>();
			foreach (string nodeToLabels in args.Split("[ \n]"))
			{
				nodeToLabels = nodeToLabels.Trim();
				if (nodeToLabels.IsEmpty() || nodeToLabels.StartsWith("#"))
				{
					continue;
				}
				// "," also supported for compatibility
				string[] splits = nodeToLabels.Split("=");
				int index = 0;
				if (splits.Length != 2)
				{
					splits = nodeToLabels.Split(",");
					index = 1;
				}
				string nodeIdStr = splits[0];
				if (index == 0)
				{
					splits = splits[1].Split(",");
				}
				Preconditions.CheckArgument(!nodeIdStr.Trim().IsEmpty(), "node name cannot be empty"
					);
				NodeId nodeId = ConverterUtils.ToNodeIdWithDefaultPort(nodeIdStr);
				map[nodeId] = new HashSet<string>();
				for (int i = index; i < splits.Length; i++)
				{
					if (!splits[i].Trim().IsEmpty())
					{
						map[nodeId].AddItem(splits[i].Trim());
					}
				}
				int nLabels = map[nodeId].Count;
				Preconditions.CheckArgument(nLabels <= 1, "%d labels specified on host=%s" + ", please note that we do not support specifying multiple"
					 + " labels on a single host for now.", nLabels, nodeIdStr);
			}
			if (map.IsEmpty())
			{
				throw new ArgumentException(NoMappingErrMsg);
			}
			return map;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private int ReplaceLabelsOnNodes(string args)
		{
			IDictionary<NodeId, ICollection<string>> map = BuildNodeLabelsMapFromStr(args);
			return ReplaceLabelsOnNodes(map);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private int ReplaceLabelsOnNodes(IDictionary<NodeId, ICollection<string>> map)
		{
			if (directlyAccessNodeLabelStore)
			{
				GetNodeLabelManagerInstance(GetConf()).ReplaceLabelsOnNode(map);
			}
			else
			{
				ResourceManagerAdministrationProtocol adminProtocol = CreateAdminProtocol();
				ReplaceLabelsOnNodeRequest request = ReplaceLabelsOnNodeRequest.NewInstance(map);
				adminProtocol.ReplaceLabelsOnNode(request);
			}
			return 0;
		}

		/// <exception cref="System.Exception"/>
		public override int Run(string[] args)
		{
			// -directlyAccessNodeLabelStore is a additional option for node label
			// access, so just search if we have specified this option, and remove it
			IList<string> argsList = new AList<string>();
			for (int i = 0; i < args.Length; i++)
			{
				if (args[i].Equals("-directlyAccessNodeLabelStore"))
				{
					directlyAccessNodeLabelStore = true;
				}
				else
				{
					argsList.AddItem(args[i]);
				}
			}
			args = Sharpen.Collections.ToArray(argsList, new string[0]);
			YarnConfiguration yarnConf = GetConf() == null ? new YarnConfiguration() : new YarnConfiguration
				(GetConf());
			bool isHAEnabled = yarnConf.GetBoolean(YarnConfiguration.RmHaEnabled, YarnConfiguration
				.DefaultRmHaEnabled);
			if (args.Length < 1)
			{
				PrintUsage(string.Empty, isHAEnabled);
				return -1;
			}
			int exitCode = -1;
			int i_1 = 0;
			string cmd = args[i_1++];
			exitCode = 0;
			if ("-help".Equals(cmd))
			{
				if (i_1 < args.Length)
				{
					PrintUsage(args[i_1], isHAEnabled);
				}
				else
				{
					PrintHelp(string.Empty, isHAEnabled);
				}
				return exitCode;
			}
			if (Usage.Contains(cmd))
			{
				if (isHAEnabled)
				{
					return base.Run(args);
				}
				System.Console.Out.WriteLine("Cannot run " + cmd + " when ResourceManager HA is not enabled"
					);
				return -1;
			}
			//
			// verify that we have enough command line parameters
			//
			if ("-refreshAdminAcls".Equals(cmd) || "-refreshQueues".Equals(cmd) || "-refreshNodes"
				.Equals(cmd) || "-refreshServiceAcl".Equals(cmd) || "-refreshUserToGroupsMappings"
				.Equals(cmd) || "-refreshSuperUserGroupsConfiguration".Equals(cmd))
			{
				if (args.Length != 1)
				{
					PrintUsage(cmd, isHAEnabled);
					return exitCode;
				}
			}
			try
			{
				if ("-refreshQueues".Equals(cmd))
				{
					exitCode = RefreshQueues();
				}
				else
				{
					if ("-refreshNodes".Equals(cmd))
					{
						exitCode = RefreshNodes();
					}
					else
					{
						if ("-refreshUserToGroupsMappings".Equals(cmd))
						{
							exitCode = RefreshUserToGroupsMappings();
						}
						else
						{
							if ("-refreshSuperUserGroupsConfiguration".Equals(cmd))
							{
								exitCode = RefreshSuperUserGroupsConfiguration();
							}
							else
							{
								if ("-refreshAdminAcls".Equals(cmd))
								{
									exitCode = RefreshAdminAcls();
								}
								else
								{
									if ("-refreshServiceAcl".Equals(cmd))
									{
										exitCode = RefreshServiceAcls();
									}
									else
									{
										if ("-getGroups".Equals(cmd))
										{
											string[] usernames = Arrays.CopyOfRange(args, i_1, args.Length);
											exitCode = GetGroups(usernames);
										}
										else
										{
											if ("-addToClusterNodeLabels".Equals(cmd))
											{
												if (i_1 >= args.Length)
												{
													System.Console.Error.WriteLine(NoLabelErrMsg);
													exitCode = -1;
												}
												else
												{
													exitCode = AddToClusterNodeLabels(args[i_1]);
												}
											}
											else
											{
												if ("-removeFromClusterNodeLabels".Equals(cmd))
												{
													if (i_1 >= args.Length)
													{
														System.Console.Error.WriteLine(NoLabelErrMsg);
														exitCode = -1;
													}
													else
													{
														exitCode = RemoveFromClusterNodeLabels(args[i_1]);
													}
												}
												else
												{
													if ("-replaceLabelsOnNode".Equals(cmd))
													{
														if (i_1 >= args.Length)
														{
															System.Console.Error.WriteLine(NoMappingErrMsg);
															exitCode = -1;
														}
														else
														{
															exitCode = ReplaceLabelsOnNodes(args[i_1]);
														}
													}
													else
													{
														exitCode = -1;
														System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": Unknown command"
															);
														PrintUsage(string.Empty, isHAEnabled);
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			catch (ArgumentException arge)
			{
				exitCode = -1;
				System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": " + arge.GetLocalizedMessage
					());
				PrintUsage(cmd, isHAEnabled);
			}
			catch (RemoteException e)
			{
				//
				// This is a error returned by hadoop server. Print
				// out the first line of the error mesage, ignore the stack trace.
				exitCode = -1;
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
				exitCode = -1;
				System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": " + e.GetLocalizedMessage
					());
			}
			if (null != localNodeLabelsManager)
			{
				localNodeLabelsManager.Stop();
			}
			return exitCode;
		}

		public override void SetConf(Configuration conf)
		{
			if (conf != null)
			{
				conf = AddSecurityConfiguration(conf);
			}
			base.SetConf(conf);
		}

		/// <summary>
		/// Add the requisite security principal settings to the given Configuration,
		/// returning a copy.
		/// </summary>
		/// <param name="conf">the original config</param>
		/// <returns>a copy with the security settings added</returns>
		private static Configuration AddSecurityConfiguration(Configuration conf)
		{
			// Make a copy so we don't mutate it. Also use an YarnConfiguration to
			// force loading of yarn-site.xml.
			conf = new YarnConfiguration(conf);
			conf.Set(CommonConfigurationKeys.HadoopSecurityServiceUserNameKey, conf.Get(YarnConfiguration
				.RmPrincipal, string.Empty));
			return conf;
		}

		protected override HAServiceTarget ResolveTarget(string rmId)
		{
			ICollection<string> rmIds = HAUtil.GetRMHAIds(GetConf());
			if (!rmIds.Contains(rmId))
			{
				StringBuilder msg = new StringBuilder();
				msg.Append(rmId + " is not a valid serviceId. It should be one of ");
				foreach (string id in rmIds)
				{
					msg.Append(id + " ");
				}
				throw new ArgumentException(msg.ToString());
			}
			try
			{
				YarnConfiguration conf = new YarnConfiguration(GetConf());
				conf.Set(YarnConfiguration.RmHaId, rmId);
				return new RMHAServiceTarget(conf);
			}
			catch (ArgumentException)
			{
				throw new YarnRuntimeException("Could not connect to " + rmId + "; the configuration for it might be missing"
					);
			}
			catch (IOException)
			{
				throw new YarnRuntimeException("Could not connect to RM HA Admin for node " + rmId
					);
			}
		}

		protected override string GetUsageString()
		{
			return "Usage: rmadmin";
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int result = ToolRunner.Run(new Org.Apache.Hadoop.Yarn.Client.Cli.RMAdminCLI(), args
				);
			System.Environment.Exit(result);
		}
	}
}
