using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>Utilities for working with ZooKeeper.</summary>
	public class ZKUtil
	{
		/// <summary>
		/// Parse ACL permission string, partially borrowed from
		/// ZooKeeperMain private method
		/// </summary>
		private static int getPermFromString(string permString)
		{
			int perm = 0;
			for (int i = 0; i < permString.Length; i++)
			{
				char c = permString[i];
				switch (c)
				{
					case 'r':
					{
						perm |= org.apache.zookeeper.ZooDefs.Perms.READ;
						break;
					}

					case 'w':
					{
						perm |= org.apache.zookeeper.ZooDefs.Perms.WRITE;
						break;
					}

					case 'c':
					{
						perm |= org.apache.zookeeper.ZooDefs.Perms.CREATE;
						break;
					}

					case 'd':
					{
						perm |= org.apache.zookeeper.ZooDefs.Perms.DELETE;
						break;
					}

					case 'a':
					{
						perm |= org.apache.zookeeper.ZooDefs.Perms.ADMIN;
						break;
					}

					default:
					{
						throw new org.apache.hadoop.util.ZKUtil.BadAclFormatException("Invalid permission '"
							 + c + "' in permission string '" + permString + "'");
					}
				}
			}
			return perm;
		}

		/// <summary>
		/// Helper method to remove a subset of permissions (remove) from a
		/// given set (perms).
		/// </summary>
		/// <param name="perms">
		/// The permissions flag to remove from. Should be an OR of a
		/// some combination of
		/// <see cref="org.apache.zookeeper.ZooDefs.Perms"/>
		/// </param>
		/// <param name="remove">
		/// The permissions to be removed. Should be an OR of a
		/// some combination of
		/// <see cref="org.apache.zookeeper.ZooDefs.Perms"/>
		/// </param>
		/// <returns>
		/// A permissions flag that is an OR of
		/// <see cref="org.apache.zookeeper.ZooDefs.Perms"/>
		/// present in perms and not present in remove
		/// </returns>
		public static int removeSpecificPerms(int perms, int remove)
		{
			return perms ^ remove;
		}

		/// <summary>Parse comma separated list of ACL entries to secure generated nodes, e.g.
		/// 	</summary>
		/// <remarks>
		/// Parse comma separated list of ACL entries to secure generated nodes, e.g.
		/// <code>sasl:hdfs/host1@MY.DOMAIN:cdrwa,sasl:hdfs/host2@MY.DOMAIN:cdrwa</code>
		/// </remarks>
		/// <returns>ACL list</returns>
		/// <exception>{@link BadAclFormatException} if an ACL is invalid</exception>
		/// <exception cref="org.apache.hadoop.util.ZKUtil.BadAclFormatException"/>
		public static System.Collections.Generic.IList<org.apache.zookeeper.data.ACL> parseACLs
			(string aclString)
		{
			System.Collections.Generic.IList<org.apache.zookeeper.data.ACL> acl = com.google.common.collect.Lists
				.newArrayList();
			if (aclString == null)
			{
				return acl;
			}
			System.Collections.Generic.IList<string> aclComps = com.google.common.collect.Lists
				.newArrayList(com.google.common.@base.Splitter.on(',').omitEmptyStrings().trimResults
				().split(aclString));
			foreach (string a in aclComps)
			{
				// from ZooKeeperMain private method
				int firstColon = a.IndexOf(':');
				int lastColon = a.LastIndexOf(':');
				if (firstColon == -1 || lastColon == -1 || firstColon == lastColon)
				{
					throw new org.apache.hadoop.util.ZKUtil.BadAclFormatException("ACL '" + a + "' not of expected form scheme:id:perm"
						);
				}
				org.apache.zookeeper.data.ACL newAcl = new org.apache.zookeeper.data.ACL();
				newAcl.setId(new org.apache.zookeeper.data.Id(Sharpen.Runtime.substring(a, 0, firstColon
					), Sharpen.Runtime.substring(a, firstColon + 1, lastColon)));
				newAcl.setPerms(getPermFromString(Sharpen.Runtime.substring(a, lastColon + 1)));
				acl.add(newAcl);
			}
			return acl;
		}

		/// <summary>Parse a comma-separated list of authentication mechanisms.</summary>
		/// <remarks>
		/// Parse a comma-separated list of authentication mechanisms. Each
		/// such mechanism should be of the form 'scheme:auth' -- the same
		/// syntax used for the 'addAuth' command in the ZK CLI.
		/// </remarks>
		/// <param name="authString">the comma-separated auth mechanisms</param>
		/// <returns>a list of parsed authentications</returns>
		/// <exception>{@link BadAuthFormatException} if the auth format is invalid</exception>
		/// <exception cref="org.apache.hadoop.util.ZKUtil.BadAuthFormatException"/>
		public static System.Collections.Generic.IList<org.apache.hadoop.util.ZKUtil.ZKAuthInfo
			> parseAuth(string authString)
		{
			System.Collections.Generic.IList<org.apache.hadoop.util.ZKUtil.ZKAuthInfo> ret = 
				com.google.common.collect.Lists.newArrayList();
			if (authString == null)
			{
				return ret;
			}
			System.Collections.Generic.IList<string> authComps = com.google.common.collect.Lists
				.newArrayList(com.google.common.@base.Splitter.on(',').omitEmptyStrings().trimResults
				().split(authString));
			foreach (string comp in authComps)
			{
				string[] parts = comp.split(":", 2);
				if (parts.Length != 2)
				{
					throw new org.apache.hadoop.util.ZKUtil.BadAuthFormatException("Auth '" + comp + 
						"' not of expected form scheme:auth");
				}
				ret.add(new org.apache.hadoop.util.ZKUtil.ZKAuthInfo(parts[0], Sharpen.Runtime.getBytesForString
					(parts[1], com.google.common.@base.Charsets.UTF_8)));
			}
			return ret;
		}

		/// <summary>
		/// Because ZK ACLs and authentication information may be secret,
		/// allow the configuration values to be indirected through a file
		/// by specifying the configuration as "@/path/to/file".
		/// </summary>
		/// <remarks>
		/// Because ZK ACLs and authentication information may be secret,
		/// allow the configuration values to be indirected through a file
		/// by specifying the configuration as "@/path/to/file". If this
		/// syntax is used, this function will return the contents of the file
		/// as a String.
		/// </remarks>
		/// <param name="valInConf">the value from the Configuration</param>
		/// <returns>
		/// either the same value, or the contents of the referenced
		/// file if the configured value starts with "@"
		/// </returns>
		/// <exception cref="System.IO.IOException">if the file cannot be read</exception>
		public static string resolveConfIndirection(string valInConf)
		{
			if (valInConf == null)
			{
				return null;
			}
			if (!valInConf.StartsWith("@"))
			{
				return valInConf;
			}
			string path = Sharpen.Runtime.substring(valInConf, 1).Trim();
			return com.google.common.io.Files.toString(new java.io.File(path), com.google.common.@base.Charsets
				.UTF_8).Trim();
		}

		/// <summary>An authentication token passed to ZooKeeper.addAuthInfo</summary>
		public class ZKAuthInfo
		{
			private readonly string scheme;

			private readonly byte[] auth;

			public ZKAuthInfo(string scheme, byte[] auth)
				: base()
			{
				this.scheme = scheme;
				this.auth = auth;
			}

			public virtual string getScheme()
			{
				return scheme;
			}

			public virtual byte[] getAuth()
			{
				return auth;
			}
		}

		[System.Serializable]
		public class BadAclFormatException : org.apache.hadoop.HadoopIllegalArgumentException
		{
			private const long serialVersionUID = 1L;

			public BadAclFormatException(string message)
				: base(message)
			{
			}
		}

		[System.Serializable]
		public class BadAuthFormatException : org.apache.hadoop.HadoopIllegalArgumentException
		{
			private const long serialVersionUID = 1L;

			public BadAuthFormatException(string message)
				: base(message)
			{
			}
		}
	}
}
