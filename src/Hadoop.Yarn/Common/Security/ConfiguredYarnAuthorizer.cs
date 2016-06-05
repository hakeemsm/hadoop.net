using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security
{
	/// <summary>A YarnAuthorizationProvider implementation based on configuration files.
	/// 	</summary>
	public class ConfiguredYarnAuthorizer : YarnAuthorizationProvider
	{
		private readonly ConcurrentMap<PrivilegedEntity, IDictionary<AccessType, AccessControlList
			>> allAcls = new ConcurrentHashMap<PrivilegedEntity, IDictionary<AccessType, AccessControlList
			>>();

		private volatile AccessControlList adminAcl = null;

		public override void Init(Configuration conf)
		{
			adminAcl = new AccessControlList(conf.Get(YarnConfiguration.YarnAdminAcl, YarnConfiguration
				.DefaultYarnAdminAcl));
		}

		public override void SetPermission(PrivilegedEntity target, IDictionary<AccessType
			, AccessControlList> acls, UserGroupInformation ugi)
		{
			allAcls[target] = acls;
		}

		public override bool CheckPermission(AccessType accessType, PrivilegedEntity target
			, UserGroupInformation user)
		{
			bool ret = false;
			IDictionary<AccessType, AccessControlList> acls = allAcls[target];
			if (acls != null)
			{
				AccessControlList list = acls[accessType];
				if (list != null)
				{
					ret = list.IsUserAllowed(user);
				}
			}
			// recursively look up the queue to see if parent queue has the permission.
			if (target.GetType() == PrivilegedEntity.EntityType.Queue && !ret)
			{
				string queueName = target.GetName();
				if (!queueName.Contains("."))
				{
					return ret;
				}
				string parentQueueName = Sharpen.Runtime.Substring(queueName, 0, queueName.LastIndexOf
					("."));
				return CheckPermission(accessType, new PrivilegedEntity(target.GetType(), parentQueueName
					), user);
			}
			return ret;
		}

		public override void SetAdmins(AccessControlList acls, UserGroupInformation ugi)
		{
			adminAcl = acls;
		}

		public override bool IsAdmin(UserGroupInformation ugi)
		{
			return adminAcl.IsUserAllowed(ugi);
		}

		public virtual AccessControlList GetAdminAcls()
		{
			return this.adminAcl;
		}
	}
}
