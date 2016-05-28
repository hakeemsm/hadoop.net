using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	/// <summary>
	/// Helper to configure FileSystemAccess user/group and proxyuser
	/// configuration for testing using Java System properties.
	/// </summary>
	/// <remarks>
	/// Helper to configure FileSystemAccess user/group and proxyuser
	/// configuration for testing using Java System properties.
	/// <p/>
	/// It uses the
	/// <see cref="SysPropsForTestsLoader"/>
	/// to load JavaSystem
	/// properties for testing.
	/// </remarks>
	public class HadoopUsersConfTestHelper
	{
		static HadoopUsersConfTestHelper()
		{
			SysPropsForTestsLoader.Init();
		}

		public const string HadoopProxyuser = "test.hadoop.proxyuser";

		public const string HadoopProxyuserHosts = "test.hadoop.proxyuser.hosts";

		public const string HadoopProxyuserGroups = "test.hadoop.proxyuser.groups";

		public const string HadoopUserPrefix = "test.hadoop.user.";

		/// <summary>Returns a valid FileSystemAccess proxyuser for the FileSystemAccess cluster.
		/// 	</summary>
		/// <remarks>
		/// Returns a valid FileSystemAccess proxyuser for the FileSystemAccess cluster.
		/// <p/>
		/// The user is read from the Java System property
		/// <code>test.hadoop.proxyuser</code> which defaults to the current user
		/// (java System property <code>user.name</code>).
		/// <p/>
		/// This property should be set in the <code>test.properties</code> file.
		/// <p/>
		/// When running FileSystemAccess minicluster it is used to configure the FileSystemAccess minicluster.
		/// <p/>
		/// When using an external FileSystemAccess cluster, it is expected this property is set to
		/// a valid proxy user.
		/// </remarks>
		/// <returns>a valid FileSystemAccess proxyuser for the FileSystemAccess cluster.</returns>
		public static string GetHadoopProxyUser()
		{
			return Runtime.GetProperty(HadoopProxyuser, Runtime.GetProperty("user.name"));
		}

		/// <summary>Returns the hosts for the FileSystemAccess proxyuser settings.</summary>
		/// <remarks>
		/// Returns the hosts for the FileSystemAccess proxyuser settings.
		/// <p/>
		/// The hosts are read from the Java System property
		/// <code>test.hadoop.proxyuser.hosts</code> which defaults to <code>*</code>.
		/// <p/>
		/// This property should be set in the <code>test.properties</code> file.
		/// <p/>
		/// This property is ONLY used when running FileSystemAccess minicluster, it is used to
		/// configure the FileSystemAccess minicluster.
		/// <p/>
		/// When using an external FileSystemAccess cluster this property is ignored.
		/// </remarks>
		/// <returns>the hosts for the FileSystemAccess proxyuser settings.</returns>
		public static string GetHadoopProxyUserHosts()
		{
			return Runtime.GetProperty(HadoopProxyuserHosts, "*");
		}

		/// <summary>Returns the groups for the FileSystemAccess proxyuser settings.</summary>
		/// <remarks>
		/// Returns the groups for the FileSystemAccess proxyuser settings.
		/// <p/>
		/// The hosts are read from the Java System property
		/// <code>test.hadoop.proxyuser.groups</code> which defaults to <code>*</code>.
		/// <p/>
		/// This property should be set in the <code>test.properties</code> file.
		/// <p/>
		/// This property is ONLY used when running FileSystemAccess minicluster, it is used to
		/// configure the FileSystemAccess minicluster.
		/// <p/>
		/// When using an external FileSystemAccess cluster this property is ignored.
		/// </remarks>
		/// <returns>the groups for the FileSystemAccess proxyuser settings.</returns>
		public static string GetHadoopProxyUserGroups()
		{
			return Runtime.GetProperty(HadoopProxyuserGroups, "*");
		}

		private static readonly string[] DefaultUsers = new string[] { "user1", "user2" };

		private static readonly string[] DefaultUsersGroup = new string[] { "group1", "supergroup"
			 };

		/// <summary>Returns the FileSystemAccess users to be used for tests.</summary>
		/// <remarks>
		/// Returns the FileSystemAccess users to be used for tests. These users are defined
		/// in the <code>test.properties</code> file in properties of the form
		/// <code>test.hadoop.user.#USER#=#GROUP1#,#GROUP2#,...</code>.
		/// <p/>
		/// These properties are used to configure the FileSystemAccess minicluster user/group
		/// information.
		/// <p/>
		/// When using an external FileSystemAccess cluster these properties should match the
		/// user/groups settings in the cluster.
		/// </remarks>
		/// <returns>the FileSystemAccess users used for testing.</returns>
		public static string[] GetHadoopUsers()
		{
			IList<string> users = new AList<string>();
			foreach (string name in Runtime.GetProperties().StringPropertyNames())
			{
				if (name.StartsWith(HadoopUserPrefix))
				{
					users.AddItem(Sharpen.Runtime.Substring(name, HadoopUserPrefix.Length));
				}
			}
			return (users.Count != 0) ? Sharpen.Collections.ToArray(users, new string[users.Count
				]) : DefaultUsers;
		}

		/// <summary>Returns the groups a FileSystemAccess user belongs to during tests.</summary>
		/// <remarks>
		/// Returns the groups a FileSystemAccess user belongs to during tests. These users/groups
		/// are defined in the <code>test.properties</code> file in properties of the
		/// form <code>test.hadoop.user.#USER#=#GROUP1#,#GROUP2#,...</code>.
		/// <p/>
		/// These properties are used to configure the FileSystemAccess minicluster user/group
		/// information.
		/// <p/>
		/// When using an external FileSystemAccess cluster these properties should match the
		/// user/groups settings in the cluster.
		/// </remarks>
		/// <param name="user">user name to get gropus.</param>
		/// <returns>the groups of FileSystemAccess users used for testing.</returns>
		public static string[] GetHadoopUserGroups(string user)
		{
			if (GetHadoopUsers() == DefaultUsers)
			{
				foreach (string defaultUser in DefaultUsers)
				{
					if (defaultUser.Equals(user))
					{
						return DefaultUsersGroup;
					}
				}
				return new string[0];
			}
			else
			{
				string groups = Runtime.GetProperty(HadoopUserPrefix + user);
				return (groups != null) ? groups.Split(",") : new string[0];
			}
		}

		public static Configuration GetBaseConf()
		{
			Configuration conf = new Configuration();
			foreach (string name in Runtime.GetProperties().StringPropertyNames())
			{
				conf.Set(name, Runtime.GetProperty(name));
			}
			return conf;
		}

		public static void AddUserConf(Configuration conf)
		{
			conf.Set("hadoop.security.authentication", "simple");
			conf.Set("hadoop.proxyuser." + HadoopUsersConfTestHelper.GetHadoopProxyUser() + ".hosts"
				, HadoopUsersConfTestHelper.GetHadoopProxyUserHosts());
			conf.Set("hadoop.proxyuser." + HadoopUsersConfTestHelper.GetHadoopProxyUser() + ".groups"
				, HadoopUsersConfTestHelper.GetHadoopProxyUserGroups());
			foreach (string user in HadoopUsersConfTestHelper.GetHadoopUsers())
			{
				string[] groups = HadoopUsersConfTestHelper.GetHadoopUserGroups(user);
				UserGroupInformation.CreateUserForTesting(user, groups);
			}
		}
	}
}
