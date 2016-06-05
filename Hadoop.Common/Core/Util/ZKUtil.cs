using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.IO;
using Org.Apache.Hadoop;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>Utilities for working with ZooKeeper.</summary>
	public class ZKUtil
	{
		/// <summary>
		/// Parse ACL permission string, partially borrowed from
		/// ZooKeeperMain private method
		/// </summary>
		private static int GetPermFromString(string permString)
		{
			int perm = 0;
			for (int i = 0; i < permString.Length; i++)
			{
				char c = permString[i];
				switch (c)
				{
					case 'r':
					{
						perm |= ZooDefs.Perms.Read;
						break;
					}

					case 'w':
					{
						perm |= ZooDefs.Perms.Write;
						break;
					}

					case 'c':
					{
						perm |= ZooDefs.Perms.Create;
						break;
					}

					case 'd':
					{
						perm |= ZooDefs.Perms.Delete;
						break;
					}

					case 'a':
					{
						perm |= ZooDefs.Perms.Admin;
						break;
					}

					default:
					{
						throw new ZKUtil.BadAclFormatException("Invalid permission '" + c + "' in permission string '"
							 + permString + "'");
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
		/// <see cref="Org.Apache.Zookeeper.ZooDefs.Perms"/>
		/// </param>
		/// <param name="remove">
		/// The permissions to be removed. Should be an OR of a
		/// some combination of
		/// <see cref="Org.Apache.Zookeeper.ZooDefs.Perms"/>
		/// </param>
		/// <returns>
		/// A permissions flag that is an OR of
		/// <see cref="Org.Apache.Zookeeper.ZooDefs.Perms"/>
		/// present in perms and not present in remove
		/// </returns>
		public static int RemoveSpecificPerms(int perms, int remove)
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
		/// <exception cref="Org.Apache.Hadoop.Util.ZKUtil.BadAclFormatException"/>
		public static IList<ACL> ParseACLs(string aclString)
		{
			IList<ACL> acl = Lists.NewArrayList();
			if (aclString == null)
			{
				return acl;
			}
			IList<string> aclComps = Lists.NewArrayList(Splitter.On(',').OmitEmptyStrings().TrimResults
				().Split(aclString));
			foreach (string a in aclComps)
			{
				// from ZooKeeperMain private method
				int firstColon = a.IndexOf(':');
				int lastColon = a.LastIndexOf(':');
				if (firstColon == -1 || lastColon == -1 || firstColon == lastColon)
				{
					throw new ZKUtil.BadAclFormatException("ACL '" + a + "' not of expected form scheme:id:perm"
						);
				}
				ACL newAcl = new ACL();
				newAcl.SetId(new ID(Runtime.Substring(a, 0, firstColon), Runtime.Substring
					(a, firstColon + 1, lastColon)));
				newAcl.SetPerms(GetPermFromString(Runtime.Substring(a, lastColon + 1)));
				acl.AddItem(newAcl);
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
		/// <exception cref="Org.Apache.Hadoop.Util.ZKUtil.BadAuthFormatException"/>
		public static IList<ZKUtil.ZKAuthInfo> ParseAuth(string authString)
		{
			IList<ZKUtil.ZKAuthInfo> ret = Lists.NewArrayList();
			if (authString == null)
			{
				return ret;
			}
			IList<string> authComps = Lists.NewArrayList(Splitter.On(',').OmitEmptyStrings().
				TrimResults().Split(authString));
			foreach (string comp in authComps)
			{
				string[] parts = comp.Split(":", 2);
				if (parts.Length != 2)
				{
					throw new ZKUtil.BadAuthFormatException("Auth '" + comp + "' not of expected form scheme:auth"
						);
				}
				ret.AddItem(new ZKUtil.ZKAuthInfo(parts[0], Runtime.GetBytesForString(parts
					[1], Charsets.Utf8)));
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
		public static string ResolveConfIndirection(string valInConf)
		{
			if (valInConf == null)
			{
				return null;
			}
			if (!valInConf.StartsWith("@"))
			{
				return valInConf;
			}
			string path = Runtime.Substring(valInConf, 1).Trim();
			return Files.ToString(new FilePath(path), Charsets.Utf8).Trim();
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

			public virtual string GetScheme()
			{
				return scheme;
			}

			public virtual byte[] GetAuth()
			{
				return auth;
			}
		}

		[System.Serializable]
		public class BadAclFormatException : HadoopIllegalArgumentException
		{
			private const long serialVersionUID = 1L;

			public BadAclFormatException(string message)
				: base(message)
			{
			}
		}

		[System.Serializable]
		public class BadAuthFormatException : HadoopIllegalArgumentException
		{
			private const long serialVersionUID = 1L;

			public BadAuthFormatException(string message)
				: base(message)
			{
			}
		}
	}
}
