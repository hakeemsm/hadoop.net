using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Security
{
	public class ApplicationACLsManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Security.ApplicationACLsManager
			));

		private static AccessControlList DefaultYarnAppAcl = new AccessControlList(YarnConfiguration
			.DefaultYarnAppAcl);

		private readonly Configuration conf;

		private readonly AdminACLsManager adminAclsManager;

		private readonly ConcurrentMap<ApplicationId, IDictionary<ApplicationAccessType, 
			AccessControlList>> applicationACLS = new ConcurrentHashMap<ApplicationId, IDictionary
			<ApplicationAccessType, AccessControlList>>();

		[VisibleForTesting]
		public ApplicationACLsManager()
			: this(new Configuration())
		{
		}

		public ApplicationACLsManager(Configuration conf)
		{
			this.conf = conf;
			this.adminAclsManager = new AdminACLsManager(this.conf);
		}

		public virtual bool AreACLsEnabled()
		{
			return adminAclsManager.AreACLsEnabled();
		}

		public virtual void AddApplication(ApplicationId appId, IDictionary<ApplicationAccessType
			, string> acls)
		{
			IDictionary<ApplicationAccessType, AccessControlList> finalMap = new Dictionary<ApplicationAccessType
				, AccessControlList>(acls.Count);
			foreach (KeyValuePair<ApplicationAccessType, string> acl in acls)
			{
				finalMap[acl.Key] = new AccessControlList(acl.Value);
			}
			this.applicationACLS[appId] = finalMap;
		}

		public virtual void RemoveApplication(ApplicationId appId)
		{
			Sharpen.Collections.Remove(this.applicationACLS, appId);
		}

		/// <summary>
		/// If authorization is enabled, checks whether the user (in the callerUGI) is
		/// authorized to perform the access specified by 'applicationAccessType' on
		/// the application by checking if the user is applicationOwner or part of
		/// application ACL for the specific access-type.
		/// </summary>
		/// <remarks>
		/// If authorization is enabled, checks whether the user (in the callerUGI) is
		/// authorized to perform the access specified by 'applicationAccessType' on
		/// the application by checking if the user is applicationOwner or part of
		/// application ACL for the specific access-type.
		/// <ul>
		/// <li>The owner of the application can have all access-types on the
		/// application</li>
		/// <li>For all other users/groups application-acls are checked</li>
		/// </ul>
		/// </remarks>
		/// <param name="callerUGI"/>
		/// <param name="applicationAccessType"/>
		/// <param name="applicationOwner"/>
		/// <param name="applicationId"/>
		public virtual bool CheckAccess(UserGroupInformation callerUGI, ApplicationAccessType
			 applicationAccessType, string applicationOwner, ApplicationId applicationId)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Verifying access-type " + applicationAccessType + " for " + callerUGI 
					+ " on application " + applicationId + " owned by " + applicationOwner);
			}
			string user = callerUGI.GetShortUserName();
			if (!AreACLsEnabled())
			{
				return true;
			}
			AccessControlList applicationACL = DefaultYarnAppAcl;
			IDictionary<ApplicationAccessType, AccessControlList> acls = this.applicationACLS
				[applicationId];
			if (acls == null)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("ACL not found for application " + applicationId + " owned by " + applicationOwner
						 + ". Using default [" + YarnConfiguration.DefaultYarnAppAcl + "]");
				}
			}
			else
			{
				AccessControlList applicationACLInMap = acls[applicationAccessType];
				if (applicationACLInMap != null)
				{
					applicationACL = applicationACLInMap;
				}
				else
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("ACL not found for access-type " + applicationAccessType + " for application "
							 + applicationId + " owned by " + applicationOwner + ". Using default [" + YarnConfiguration
							.DefaultYarnAppAcl + "]");
					}
				}
			}
			// Allow application-owner for any type of access on the application
			if (this.adminAclsManager.IsAdmin(callerUGI) || user.Equals(applicationOwner) || 
				applicationACL.IsUserAllowed(callerUGI))
			{
				return true;
			}
			return false;
		}
	}
}
