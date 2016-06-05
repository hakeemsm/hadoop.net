using System;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security.Authentication.Util;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>This class implements parsing and handling of Kerberos principal names.</summary>
	/// <remarks>
	/// This class implements parsing and handling of Kerberos principal names. In
	/// particular, it splits them apart and translates them down into local
	/// operating system names.
	/// </remarks>
	public class HadoopKerberosName : KerberosName
	{
		/// <summary>Create a name from the full Kerberos principal name.</summary>
		/// <param name="name"/>
		public HadoopKerberosName(string name)
			: base(name)
		{
		}

		/// <summary>Set the static configuration to get the rules.</summary>
		/// <remarks>
		/// Set the static configuration to get the rules.
		/// <p/>
		/// IMPORTANT: This method does a NOP if the rules have been set already.
		/// If there is a need to reset the rules, the
		/// <see cref="Org.Apache.Hadoop.Security.Authentication.Util.KerberosName.SetRules(string)
		/// 	"/>
		/// method should be invoked directly.
		/// </remarks>
		/// <param name="conf">the new configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public static void SetConfiguration(Configuration conf)
		{
			string defaultRule;
			switch (SecurityUtil.GetAuthenticationMethod(conf))
			{
				case UserGroupInformation.AuthenticationMethod.Kerberos:
				case UserGroupInformation.AuthenticationMethod.KerberosSsl:
				{
					try
					{
						KerberosUtil.GetDefaultRealm();
					}
					catch (Exception ke)
					{
						throw new ArgumentException("Can't get Kerberos realm", ke);
					}
					defaultRule = "DEFAULT";
					break;
				}

				default:
				{
					// just extract the simple user name
					defaultRule = "RULE:[1:$1] RULE:[2:$1]";
					break;
				}
			}
			string ruleString = conf.Get(CommonConfigurationKeysPublic.HadoopSecurityAuthToLocal
				, defaultRule);
			SetRules(ruleString);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			SetConfiguration(new Configuration());
			foreach (string arg in args)
			{
				Org.Apache.Hadoop.Security.HadoopKerberosName name = new Org.Apache.Hadoop.Security.HadoopKerberosName
					(arg);
				System.Console.Out.WriteLine("Name: " + name + " to " + name.GetShortName());
			}
		}
	}
}
