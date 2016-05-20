using Sharpen;

namespace org.apache.hadoop.tools
{
	/// <summary>
	/// Base class for the HDFS and MR implementations of tools which fetch and
	/// display the groups that users belong to.
	/// </summary>
	public abstract class GetGroupsBase : org.apache.hadoop.conf.Configured, org.apache.hadoop.util.Tool
	{
		private System.IO.TextWriter @out;

		/// <summary>Create an instance of this tool using the given configuration.</summary>
		/// <param name="conf"/>
		protected internal GetGroupsBase(org.apache.hadoop.conf.Configuration conf)
			: this(conf, System.Console.Out)
		{
		}

		/// <summary>Used exclusively for testing.</summary>
		/// <param name="conf">The configuration to use.</param>
		/// <param name="out">The PrintStream to write to, instead of System.out</param>
		protected internal GetGroupsBase(org.apache.hadoop.conf.Configuration conf, System.IO.TextWriter
			 @out)
			: base(conf)
		{
			this.@out = @out;
		}

		/// <summary>
		/// Get the groups for the users given and print formatted output to the
		/// <see cref="System.IO.TextWriter"/>
		/// configured earlier.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual int run(string[] args)
		{
			if (args.Length == 0)
			{
				args = new string[] { org.apache.hadoop.security.UserGroupInformation.getCurrentUser
					().getUserName() };
			}
			foreach (string username in args)
			{
				java.lang.StringBuilder sb = new java.lang.StringBuilder();
				sb.Append(username + " :");
				foreach (string group in getUgmProtocol().getGroupsForUser(username))
				{
					sb.Append(" ");
					sb.Append(group);
				}
				@out.WriteLine(sb);
			}
			return 0;
		}

		/// <summary>
		/// Must be overridden by subclasses to get the address where the
		/// <see cref="GetUserMappingsProtocol"/>
		/// implementation is running.
		/// </summary>
		/// <param name="conf">The configuration to use.</param>
		/// <returns>The address where the service is listening.</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract java.net.InetSocketAddress getProtocolAddress(org.apache.hadoop.conf.Configuration
			 conf);

		/// <summary>
		/// Get a client of the
		/// <see cref="GetUserMappingsProtocol"/>
		/// .
		/// </summary>
		/// <returns>
		/// A
		/// <see cref="GetUserMappingsProtocol"/>
		/// client proxy.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.tools.GetUserMappingsProtocol getUgmProtocol
			()
		{
			org.apache.hadoop.tools.GetUserMappingsProtocol userGroupMappingProtocol = org.apache.hadoop.ipc.RPC
				.getProxy<org.apache.hadoop.tools.GetUserMappingsProtocol>(org.apache.hadoop.tools.GetUserMappingsProtocol
				.versionID, getProtocolAddress(getConf()), org.apache.hadoop.security.UserGroupInformation
				.getCurrentUser(), getConf(), org.apache.hadoop.net.NetUtils.getSocketFactory(getConf
				(), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.tools.GetUserMappingsProtocol
				))));
			return userGroupMappingProtocol;
		}
	}
}
