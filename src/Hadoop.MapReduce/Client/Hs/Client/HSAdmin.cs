using System.Net;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Client
{
	public class HSAdmin : Configured, Tool
	{
		public HSAdmin()
			: base()
		{
		}

		public HSAdmin(JobConf conf)
			: base(conf)
		{
		}

		public override void SetConf(Configuration conf)
		{
			if (conf != null)
			{
				conf = AddSecurityConfiguration(conf);
			}
			base.SetConf(conf);
		}

		private Configuration AddSecurityConfiguration(Configuration conf)
		{
			conf = new JobConf(conf);
			conf.Set(CommonConfigurationKeys.HadoopSecurityServiceUserNameKey, conf.Get(JHAdminConfig
				.MrHistoryPrincipal, string.Empty));
			return conf;
		}

		/// <summary>Displays format of commands.</summary>
		/// <param name="cmd">The command that is being executed.</param>
		private static void PrintUsage(string cmd)
		{
			if ("-refreshUserToGroupsMappings".Equals(cmd))
			{
				System.Console.Error.WriteLine("Usage: mapred hsadmin [-refreshUserToGroupsMappings]"
					);
			}
			else
			{
				if ("-refreshSuperUserGroupsConfiguration".Equals(cmd))
				{
					System.Console.Error.WriteLine("Usage: mapred hsadmin [-refreshSuperUserGroupsConfiguration]"
						);
				}
				else
				{
					if ("-refreshAdminAcls".Equals(cmd))
					{
						System.Console.Error.WriteLine("Usage: mapred hsadmin [-refreshAdminAcls]");
					}
					else
					{
						if ("-refreshLoadedJobCache".Equals(cmd))
						{
							System.Console.Error.WriteLine("Usage: mapred hsadmin [-refreshLoadedJobCache]");
						}
						else
						{
							if ("-refreshJobRetentionSettings".Equals(cmd))
							{
								System.Console.Error.WriteLine("Usage: mapred hsadmin [-refreshJobRetentionSettings]"
									);
							}
							else
							{
								if ("-refreshLogRetentionSettings".Equals(cmd))
								{
									System.Console.Error.WriteLine("Usage: mapred hsadmin [-refreshLogRetentionSettings]"
										);
								}
								else
								{
									if ("-getGroups".Equals(cmd))
									{
										System.Console.Error.WriteLine("Usage: mapred hsadmin" + " [-getGroups [username]]"
											);
									}
									else
									{
										System.Console.Error.WriteLine("Usage: mapred hsadmin");
										System.Console.Error.WriteLine("           [-refreshUserToGroupsMappings]");
										System.Console.Error.WriteLine("           [-refreshSuperUserGroupsConfiguration]"
											);
										System.Console.Error.WriteLine("           [-refreshAdminAcls]");
										System.Console.Error.WriteLine("           [-refreshLoadedJobCache]");
										System.Console.Error.WriteLine("           [-refreshJobRetentionSettings]");
										System.Console.Error.WriteLine("           [-refreshLogRetentionSettings]");
										System.Console.Error.WriteLine("           [-getGroups [username]]");
										System.Console.Error.WriteLine("           [-help [cmd]]");
										System.Console.Error.WriteLine();
										ToolRunner.PrintGenericCommandUsage(System.Console.Error);
									}
								}
							}
						}
					}
				}
			}
		}

		private static void PrintHelp(string cmd)
		{
			string summary = "hsadmin is the command to execute Job History server administrative commands.\n"
				 + "The full syntax is: \n\n" + "mapred hsadmin" + " [-refreshUserToGroupsMappings]"
				 + " [-refreshSuperUserGroupsConfiguration]" + " [-refreshAdminAcls]" + " [-refreshLoadedJobCache]"
				 + " [-refreshLogRetentionSettings]" + " [-refreshJobRetentionSettings]" + " [-getGroups [username]]"
				 + " [-help [cmd]]\n";
			string refreshUserToGroupsMappings = "-refreshUserToGroupsMappings: Refresh user-to-groups mappings\n";
			string refreshSuperUserGroupsConfiguration = "-refreshSuperUserGroupsConfiguration: Refresh superuser proxy groups mappings\n";
			string refreshAdminAcls = "-refreshAdminAcls: Refresh acls for administration of Job history server\n";
			string refreshLoadedJobCache = "-refreshLoadedJobCache: Refresh loaded job cache of Job history server\n";
			string refreshJobRetentionSettings = "-refreshJobRetentionSettings:" + "Refresh job history period,job cleaner settings\n";
			string refreshLogRetentionSettings = "-refreshLogRetentionSettings:" + "Refresh log retention period and log retention check interval\n";
			string getGroups = "-getGroups [username]: Get the groups which given user belongs to\n";
			string help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n"
				 + "\t\tis specified.\n";
			if ("refreshUserToGroupsMappings".Equals(cmd))
			{
				System.Console.Out.WriteLine(refreshUserToGroupsMappings);
			}
			else
			{
				if ("help".Equals(cmd))
				{
					System.Console.Out.WriteLine(help);
				}
				else
				{
					if ("refreshSuperUserGroupsConfiguration".Equals(cmd))
					{
						System.Console.Out.WriteLine(refreshSuperUserGroupsConfiguration);
					}
					else
					{
						if ("refreshAdminAcls".Equals(cmd))
						{
							System.Console.Out.WriteLine(refreshAdminAcls);
						}
						else
						{
							if ("refreshLoadedJobCache".Equals(cmd))
							{
								System.Console.Out.WriteLine(refreshLoadedJobCache);
							}
							else
							{
								if ("refreshJobRetentionSettings".Equals(cmd))
								{
									System.Console.Out.WriteLine(refreshJobRetentionSettings);
								}
								else
								{
									if ("refreshLogRetentionSettings".Equals(cmd))
									{
										System.Console.Out.WriteLine(refreshLogRetentionSettings);
									}
									else
									{
										if ("getGroups".Equals(cmd))
										{
											System.Console.Out.WriteLine(getGroups);
										}
										else
										{
											System.Console.Out.WriteLine(summary);
											System.Console.Out.WriteLine(refreshUserToGroupsMappings);
											System.Console.Out.WriteLine(refreshSuperUserGroupsConfiguration);
											System.Console.Out.WriteLine(refreshAdminAcls);
											System.Console.Out.WriteLine(refreshLoadedJobCache);
											System.Console.Out.WriteLine(refreshJobRetentionSettings);
											System.Console.Out.WriteLine(refreshLogRetentionSettings);
											System.Console.Out.WriteLine(getGroups);
											System.Console.Out.WriteLine(help);
											System.Console.Out.WriteLine();
											ToolRunner.PrintGenericCommandUsage(System.Console.Out);
										}
									}
								}
							}
						}
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int GetGroups(string[] usernames)
		{
			// Get groups users belongs to
			if (usernames.Length == 0)
			{
				usernames = new string[] { UserGroupInformation.GetCurrentUser().GetUserName() };
			}
			// Get the current configuration
			Configuration conf = GetConf();
			IPEndPoint address = conf.GetSocketAddr(JHAdminConfig.JhsAdminAddress, JHAdminConfig
				.DefaultJhsAdminAddress, JHAdminConfig.DefaultJhsAdminPort);
			GetUserMappingsProtocol getUserMappingProtocol = HSProxies.CreateProxy<GetUserMappingsProtocol
				>(conf, address, UserGroupInformation.GetCurrentUser());
			foreach (string username in usernames)
			{
				StringBuilder sb = new StringBuilder();
				sb.Append(username + " :");
				foreach (string group in getUserMappingProtocol.GetGroupsForUser(username))
				{
					sb.Append(" ");
					sb.Append(group);
				}
				System.Console.Out.WriteLine(sb);
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private int RefreshUserToGroupsMappings()
		{
			// Get the current configuration
			Configuration conf = GetConf();
			IPEndPoint address = conf.GetSocketAddr(JHAdminConfig.JhsAdminAddress, JHAdminConfig
				.DefaultJhsAdminAddress, JHAdminConfig.DefaultJhsAdminPort);
			RefreshUserMappingsProtocol refreshProtocol = HSProxies.CreateProxy<RefreshUserMappingsProtocol
				>(conf, address, UserGroupInformation.GetCurrentUser());
			// Refresh the user-to-groups mappings
			refreshProtocol.RefreshUserToGroupsMappings();
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private int RefreshSuperUserGroupsConfiguration()
		{
			// Refresh the super-user groups
			Configuration conf = GetConf();
			IPEndPoint address = conf.GetSocketAddr(JHAdminConfig.JhsAdminAddress, JHAdminConfig
				.DefaultJhsAdminAddress, JHAdminConfig.DefaultJhsAdminPort);
			RefreshUserMappingsProtocol refreshProtocol = HSProxies.CreateProxy<RefreshUserMappingsProtocol
				>(conf, address, UserGroupInformation.GetCurrentUser());
			// Refresh the super-user group mappings
			refreshProtocol.RefreshSuperUserGroupsConfiguration();
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private int RefreshAdminAcls()
		{
			// Refresh the admin acls
			Configuration conf = GetConf();
			IPEndPoint address = conf.GetSocketAddr(JHAdminConfig.JhsAdminAddress, JHAdminConfig
				.DefaultJhsAdminAddress, JHAdminConfig.DefaultJhsAdminPort);
			HSAdminRefreshProtocol refreshProtocol = HSProxies.CreateProxy<HSAdminRefreshProtocol
				>(conf, address, UserGroupInformation.GetCurrentUser());
			refreshProtocol.RefreshAdminAcls();
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private int RefreshLoadedJobCache()
		{
			// Refresh the loaded job cache
			Configuration conf = GetConf();
			IPEndPoint address = conf.GetSocketAddr(JHAdminConfig.JhsAdminAddress, JHAdminConfig
				.DefaultJhsAdminAddress, JHAdminConfig.DefaultJhsAdminPort);
			HSAdminRefreshProtocol refreshProtocol = HSProxies.CreateProxy<HSAdminRefreshProtocol
				>(conf, address, UserGroupInformation.GetCurrentUser());
			refreshProtocol.RefreshLoadedJobCache();
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private int RefreshJobRetentionSettings()
		{
			// Refresh job retention settings
			Configuration conf = GetConf();
			IPEndPoint address = conf.GetSocketAddr(JHAdminConfig.JhsAdminAddress, JHAdminConfig
				.DefaultJhsAdminAddress, JHAdminConfig.DefaultJhsAdminPort);
			HSAdminRefreshProtocol refreshProtocol = HSProxies.CreateProxy<HSAdminRefreshProtocol
				>(conf, address, UserGroupInformation.GetCurrentUser());
			refreshProtocol.RefreshJobRetentionSettings();
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private int RefreshLogRetentionSettings()
		{
			// Refresh log retention settings
			Configuration conf = GetConf();
			IPEndPoint address = conf.GetSocketAddr(JHAdminConfig.JhsAdminAddress, JHAdminConfig
				.DefaultJhsAdminAddress, JHAdminConfig.DefaultJhsAdminPort);
			HSAdminRefreshProtocol refreshProtocol = HSProxies.CreateProxy<HSAdminRefreshProtocol
				>(conf, address, UserGroupInformation.GetCurrentUser());
			refreshProtocol.RefreshLogRetentionSettings();
			return 0;
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length < 1)
			{
				PrintUsage(string.Empty);
				return -1;
			}
			int exitCode = -1;
			int i = 0;
			string cmd = args[i++];
			if ("-refreshUserToGroupsMappings".Equals(cmd) || "-refreshSuperUserGroupsConfiguration"
				.Equals(cmd) || "-refreshAdminAcls".Equals(cmd) || "-refreshLoadedJobCache".Equals
				(cmd) || "-refreshJobRetentionSettings".Equals(cmd) || "-refreshLogRetentionSettings"
				.Equals(cmd))
			{
				if (args.Length != 1)
				{
					PrintUsage(cmd);
					return exitCode;
				}
			}
			exitCode = 0;
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
						if ("-refreshLoadedJobCache".Equals(cmd))
						{
							exitCode = RefreshLoadedJobCache();
						}
						else
						{
							if ("-refreshJobRetentionSettings".Equals(cmd))
							{
								exitCode = RefreshJobRetentionSettings();
							}
							else
							{
								if ("-refreshLogRetentionSettings".Equals(cmd))
								{
									exitCode = RefreshLogRetentionSettings();
								}
								else
								{
									if ("-getGroups".Equals(cmd))
									{
										string[] usernames = Arrays.CopyOfRange(args, i, args.Length);
										exitCode = GetGroups(usernames);
									}
									else
									{
										if ("-help".Equals(cmd))
										{
											if (i < args.Length)
											{
												PrintHelp(args[i]);
											}
											else
											{
												PrintHelp(string.Empty);
											}
										}
										else
										{
											exitCode = -1;
											System.Console.Error.WriteLine(Sharpen.Runtime.Substring(cmd, 1) + ": Unknown command"
												);
											PrintUsage(string.Empty);
										}
									}
								}
							}
						}
					}
				}
			}
			return exitCode;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			JobConf conf = new JobConf();
			int result = ToolRunner.Run(new Org.Apache.Hadoop.Mapreduce.V2.HS.Client.HSAdmin(
				conf), args);
			System.Environment.Exit(result);
		}
	}
}
