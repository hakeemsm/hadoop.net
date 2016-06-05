using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security
{
	/// <summary>
	/// An implementation of the interface will provide authorization related
	/// information and enforce permission check.
	/// </summary>
	/// <remarks>
	/// An implementation of the interface will provide authorization related
	/// information and enforce permission check. It is excepted that any of the
	/// methods defined in this interface should be non-blocking call and should not
	/// involve expensive computation as these method could be invoked in RPC.
	/// </remarks>
	public abstract class YarnAuthorizationProvider
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(YarnAuthorizationProvider
			));

		private static YarnAuthorizationProvider authorizer = null;

		public static YarnAuthorizationProvider GetInstance(Configuration conf)
		{
			lock (typeof(YarnAuthorizationProvider))
			{
				if (authorizer == null)
				{
					Type authorizerClass = conf.GetClass(YarnConfiguration.YarnAuthorizationProvider, 
						typeof(ConfiguredYarnAuthorizer));
					authorizer = (YarnAuthorizationProvider)ReflectionUtils.NewInstance(authorizerClass
						, conf);
					authorizer.Init(conf);
					Log.Info(authorizerClass.FullName + " is instiantiated.");
				}
			}
			return authorizer;
		}

		/// <summary>Initialize the provider.</summary>
		/// <remarks>
		/// Initialize the provider. Invoked on daemon startup. DefaultYarnAuthorizer is
		/// initialized based on configurations.
		/// </remarks>
		public abstract void Init(Configuration conf);

		/// <summary>Check if user has the permission to access the target object.</summary>
		/// <param name="accessType">The type of accessing method.</param>
		/// <param name="target">The target object being accessed, e.g. app/queue</param>
		/// <param name="user">User who access the target</param>
		/// <returns>true if user can access the object, otherwise false.</returns>
		public abstract bool CheckPermission(AccessType accessType, PrivilegedEntity target
			, UserGroupInformation user);

		/// <summary>Set ACLs for the target object.</summary>
		/// <remarks>
		/// Set ACLs for the target object. AccessControlList class encapsulate the
		/// users and groups who can access the target.
		/// </remarks>
		/// <param name="target">The target object.</param>
		/// <param name="acls">
		/// A map from access method to a list of users and/or groups who has
		/// permission to do the access.
		/// </param>
		/// <param name="ugi">User who sets the permissions.</param>
		public abstract void SetPermission(PrivilegedEntity target, IDictionary<AccessType
			, AccessControlList> acls, UserGroupInformation ugi);

		/// <summary>Set a list of users/groups who have admin access</summary>
		/// <param name="acls">users/groups who have admin access</param>
		/// <param name="ugi">User who sets the admin acls.</param>
		public abstract void SetAdmins(AccessControlList acls, UserGroupInformation ugi);

		/// <summary>Check if the user is an admin.</summary>
		/// <param name="ugi">the user to be determined if it is an admin</param>
		/// <returns>true if the given user is an admin</returns>
		public abstract bool IsAdmin(UserGroupInformation ugi);
	}
}
