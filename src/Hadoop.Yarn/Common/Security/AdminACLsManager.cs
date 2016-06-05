using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security
{
	public class AdminACLsManager
	{
		/// <summary>Log object for this class</summary>
		internal static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Security.AdminACLsManager
			));

		/// <summary>The current user at the time of object creation</summary>
		private readonly UserGroupInformation owner;

		/// <summary>Holds list of administrator users</summary>
		private readonly AccessControlList adminAcl;

		/// <summary>True if ACLs are enabled</summary>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.YarnAclEnable"/>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.DefaultYarnAclEnable
		/// 	"/>
		private readonly bool aclsEnabled;

		/// <summary>Constructs and initializes this AdminACLsManager</summary>
		/// <param name="conf">configuration for this object to use</param>
		public AdminACLsManager(Configuration conf)
		{
			this.adminAcl = new AccessControlList(conf.Get(YarnConfiguration.YarnAdminAcl, YarnConfiguration
				.DefaultYarnAdminAcl));
			try
			{
				owner = UserGroupInformation.GetCurrentUser();
				adminAcl.AddUser(owner.GetShortUserName());
			}
			catch (IOException e)
			{
				Log.Warn("Could not add current user to admin:" + e);
				throw new YarnRuntimeException(e);
			}
			aclsEnabled = conf.GetBoolean(YarnConfiguration.YarnAclEnable, YarnConfiguration.
				DefaultYarnAclEnable);
		}

		/// <summary>Returns the owner</summary>
		/// <returns>Current user at the time of object creation</returns>
		public virtual UserGroupInformation GetOwner()
		{
			return owner;
		}

		/// <summary>Returns whether ACLs are enabled</summary>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.YarnAclEnable"/>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.DefaultYarnAclEnable
		/// 	"/>
		/// <returns><tt>true</tt> if ACLs are enabled</returns>
		public virtual bool AreACLsEnabled()
		{
			return aclsEnabled;
		}

		/// <summary>Returns whether the specified user/group is an administrator</summary>
		/// <param name="callerUGI">user/group to to check</param>
		/// <returns>
		/// <tt>true</tt> if the UserGroupInformation specified
		/// is a member of the access control list for administrators
		/// </returns>
		public virtual bool IsAdmin(UserGroupInformation callerUGI)
		{
			return adminAcl.IsUserAllowed(callerUGI);
		}
	}
}
