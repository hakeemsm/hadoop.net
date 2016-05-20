using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>This class implements parsing and handling of Kerberos principal names.</summary>
	/// <remarks>
	/// This class implements parsing and handling of Kerberos principal names. In
	/// particular, it splits them apart and translates them down into local
	/// operating system names.
	/// </remarks>
	public class HadoopKerberosName : org.apache.hadoop.security.authentication.util.KerberosName
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
		/// <see cref="org.apache.hadoop.security.authentication.util.KerberosName.setRules(string)
		/// 	"/>
		/// method should be invoked directly.
		/// </remarks>
		/// <param name="conf">the new configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public static void setConfiguration(org.apache.hadoop.conf.Configuration conf)
		{
			string defaultRule;
			switch (org.apache.hadoop.security.SecurityUtil.getAuthenticationMethod(conf))
			{
				case org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS
					:
				case org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS_SSL
					:
				{
					try
					{
						org.apache.hadoop.security.authentication.util.KerberosUtil.getDefaultRealm();
					}
					catch (System.Exception ke)
					{
						throw new System.ArgumentException("Can't get Kerberos realm", ke);
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
			string ruleString = conf.get(org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL
				, defaultRule);
			setRules(ruleString);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			setConfiguration(new org.apache.hadoop.conf.Configuration());
			foreach (string arg in args)
			{
				org.apache.hadoop.security.HadoopKerberosName name = new org.apache.hadoop.security.HadoopKerberosName
					(arg);
				System.Console.Out.WriteLine("Name: " + name + " to " + name.getShortName());
			}
		}
	}
}
