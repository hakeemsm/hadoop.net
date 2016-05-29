using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Collections.Map;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Security
{
	/// <summary><code>TimelineACLsManager</code> check the entity level timeline data access.
	/// 	</summary>
	public class TimelineACLsManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.Security.TimelineACLsManager
			));

		private const int DomainAccessEntryCacheSize = 100;

		private AdminACLsManager adminAclsManager;

		private IDictionary<string, TimelineACLsManager.AccessControlListExt> aclExts;

		private TimelineStore store;

		public TimelineACLsManager(Configuration conf)
		{
			this.adminAclsManager = new AdminACLsManager(conf);
			aclExts = Sharpen.Collections.SynchronizedMap(new LRUMap(DomainAccessEntryCacheSize
				));
		}

		public virtual void SetTimelineStore(TimelineStore store)
		{
			this.store = store;
		}

		/// <exception cref="System.IO.IOException"/>
		private TimelineACLsManager.AccessControlListExt LoadDomainFromTimelineStore(string
			 domainId)
		{
			if (store == null)
			{
				return null;
			}
			TimelineDomain domain = store.GetDomain(domainId);
			if (domain == null)
			{
				return null;
			}
			else
			{
				return PutDomainIntoCache(domain);
			}
		}

		public virtual void ReplaceIfExist(TimelineDomain domain)
		{
			if (aclExts.Contains(domain.GetId()))
			{
				PutDomainIntoCache(domain);
			}
		}

		private TimelineACLsManager.AccessControlListExt PutDomainIntoCache(TimelineDomain
			 domain)
		{
			IDictionary<ApplicationAccessType, AccessControlList> acls = new Dictionary<ApplicationAccessType
				, AccessControlList>(2);
			acls[ApplicationAccessType.ViewApp] = new AccessControlList(StringHelper.Cjoin(domain
				.GetReaders()));
			acls[ApplicationAccessType.ModifyApp] = new AccessControlList(StringHelper.Cjoin(
				domain.GetWriters()));
			TimelineACLsManager.AccessControlListExt aclExt = new TimelineACLsManager.AccessControlListExt
				(domain.GetOwner(), acls);
			aclExts[domain.GetId()] = aclExt;
			return aclExt;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool CheckAccess(UserGroupInformation callerUGI, ApplicationAccessType
			 applicationAccessType, TimelineEntity entity)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Verifying the access of " + (callerUGI == null ? null : callerUGI.GetShortUserName
					()) + " on the timeline entity " + new EntityIdentifier(entity.GetEntityId(), entity
					.GetEntityType()));
			}
			if (!adminAclsManager.AreACLsEnabled())
			{
				return true;
			}
			// find domain owner and acls
			TimelineACLsManager.AccessControlListExt aclExt = aclExts[entity.GetDomainId()];
			if (aclExt == null)
			{
				aclExt = LoadDomainFromTimelineStore(entity.GetDomainId());
			}
			if (aclExt == null)
			{
				throw new YarnException("Domain information of the timeline entity " + new EntityIdentifier
					(entity.GetEntityId(), entity.GetEntityType()) + " doesn't exist.");
			}
			string owner = aclExt.owner;
			AccessControlList domainACL = aclExt.acls[applicationAccessType];
			if (domainACL == null)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("ACL not found for access-type " + applicationAccessType + " for domain "
						 + entity.GetDomainId() + " owned by " + owner + ". Using default [" + YarnConfiguration
						.DefaultYarnAppAcl + "]");
				}
				domainACL = new AccessControlList(YarnConfiguration.DefaultYarnAppAcl);
			}
			if (callerUGI != null && (adminAclsManager.IsAdmin(callerUGI) || callerUGI.GetShortUserName
				().Equals(owner) || domainACL.IsUserAllowed(callerUGI)))
			{
				return true;
			}
			return false;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool CheckAccess(UserGroupInformation callerUGI, TimelineDomain domain
			)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Verifying the access of " + (callerUGI == null ? null : callerUGI.GetShortUserName
					()) + " on the timeline domain " + domain);
			}
			if (!adminAclsManager.AreACLsEnabled())
			{
				return true;
			}
			string owner = domain.GetOwner();
			if (owner == null || owner.Length == 0)
			{
				throw new YarnException("Owner information of the timeline domain " + domain.GetId
					() + " is corrupted.");
			}
			if (callerUGI != null && (adminAclsManager.IsAdmin(callerUGI) || callerUGI.GetShortUserName
				().Equals(owner)))
			{
				return true;
			}
			return false;
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual AdminACLsManager SetAdminACLsManager(AdminACLsManager adminAclsManager
			)
		{
			AdminACLsManager oldAdminACLsManager = this.adminAclsManager;
			this.adminAclsManager = adminAclsManager;
			return oldAdminACLsManager;
		}

		private class AccessControlListExt
		{
			private string owner;

			private IDictionary<ApplicationAccessType, AccessControlList> acls;

			public AccessControlListExt(string owner, IDictionary<ApplicationAccessType, AccessControlList
				> acls)
			{
				this.owner = owner;
				this.acls = acls;
			}
		}
	}
}
