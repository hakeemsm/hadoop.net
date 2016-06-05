using System.IO;
using System.Net;
using System.Text;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Tools
{
	/// <summary>
	/// Base class for the HDFS and MR implementations of tools which fetch and
	/// display the groups that users belong to.
	/// </summary>
	public abstract class GetGroupsBase : Configured, Tool
	{
		private TextWriter @out;

		/// <summary>Create an instance of this tool using the given configuration.</summary>
		/// <param name="conf"/>
		protected internal GetGroupsBase(Configuration conf)
			: this(conf, System.Console.Out)
		{
		}

		/// <summary>Used exclusively for testing.</summary>
		/// <param name="conf">The configuration to use.</param>
		/// <param name="out">The PrintStream to write to, instead of System.out</param>
		protected internal GetGroupsBase(Configuration conf, TextWriter @out)
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
		public virtual int Run(string[] args)
		{
			if (args.Length == 0)
			{
				args = new string[] { UserGroupInformation.GetCurrentUser().GetUserName() };
			}
			foreach (string username in args)
			{
				StringBuilder sb = new StringBuilder();
				sb.Append(username + " :");
				foreach (string group in GetUgmProtocol().GetGroupsForUser(username))
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
		protected internal abstract IPEndPoint GetProtocolAddress(Configuration conf);

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
		protected internal virtual GetUserMappingsProtocol GetUgmProtocol()
		{
			GetUserMappingsProtocol userGroupMappingProtocol = RPC.GetProxy<GetUserMappingsProtocol
				>(GetUserMappingsProtocol.versionID, GetProtocolAddress(GetConf()), UserGroupInformation
				.GetCurrentUser(), GetConf(), NetUtils.GetSocketFactory(GetConf(), typeof(GetUserMappingsProtocol
				)));
			return userGroupMappingProtocol;
		}
	}
}
