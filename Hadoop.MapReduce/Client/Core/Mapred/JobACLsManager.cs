using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class JobACLsManager
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.JobACLsManager
			));

		internal Configuration conf;

		private readonly AccessControlList adminAcl;

		public JobACLsManager(Configuration conf)
		{
			adminAcl = new AccessControlList(conf.Get(MRConfig.MrAdmins, " "));
			this.conf = conf;
		}

		public virtual bool AreACLsEnabled()
		{
			return conf.GetBoolean(MRConfig.MrAclsEnabled, false);
		}

		/// <summary>
		/// Construct the jobACLs from the configuration so that they can be kept in
		/// the memory.
		/// </summary>
		/// <remarks>
		/// Construct the jobACLs from the configuration so that they can be kept in
		/// the memory. If authorization is disabled on the JT, nothing is constructed
		/// and an empty map is returned.
		/// </remarks>
		/// <returns>JobACL to AccessControlList map.</returns>
		public virtual IDictionary<JobACL, AccessControlList> ConstructJobACLs(Configuration
			 conf)
		{
			IDictionary<JobACL, AccessControlList> acls = new Dictionary<JobACL, AccessControlList
				>();
			// Don't construct anything if authorization is disabled.
			if (!AreACLsEnabled())
			{
				return acls;
			}
			foreach (JobACL aclName in JobACL.Values())
			{
				string aclConfigName = aclName.GetAclName();
				string aclConfigured = conf.Get(aclConfigName);
				if (aclConfigured == null)
				{
					// If ACLs are not configured at all, we grant no access to anyone. So
					// jobOwner and cluster administrator _only_ can do 'stuff'
					aclConfigured = " ";
				}
				acls[aclName] = new AccessControlList(aclConfigured);
			}
			return acls;
		}

		/// <summary>
		/// Is the calling user an admin for the mapreduce cluster
		/// i.e.
		/// </summary>
		/// <remarks>
		/// Is the calling user an admin for the mapreduce cluster
		/// i.e. member of mapreduce.cluster.administrators
		/// </remarks>
		/// <returns>true, if user is an admin</returns>
		internal virtual bool IsMRAdmin(UserGroupInformation callerUGI)
		{
			if (adminAcl.IsUserAllowed(callerUGI))
			{
				return true;
			}
			return false;
		}

		/// <summary>
		/// If authorization is enabled, checks whether the user (in the callerUGI)
		/// is authorized to perform the operation specified by 'jobOperation' on
		/// the job by checking if the user is jobOwner or part of job ACL for the
		/// specific job operation.
		/// </summary>
		/// <remarks>
		/// If authorization is enabled, checks whether the user (in the callerUGI)
		/// is authorized to perform the operation specified by 'jobOperation' on
		/// the job by checking if the user is jobOwner or part of job ACL for the
		/// specific job operation.
		/// <ul>
		/// <li>The owner of the job can do any operation on the job</li>
		/// <li>For all other users/groups job-acls are checked</li>
		/// </ul>
		/// </remarks>
		/// <param name="callerUGI"/>
		/// <param name="jobOperation"/>
		/// <param name="jobOwner"/>
		/// <param name="jobACL"/>
		public virtual bool CheckAccess(UserGroupInformation callerUGI, JobACL jobOperation
			, string jobOwner, AccessControlList jobACL)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("checkAccess job acls, jobOwner: " + jobOwner + " jobacl: " + jobOperation
					.ToString() + " user: " + callerUGI.GetShortUserName());
			}
			string user = callerUGI.GetShortUserName();
			if (!AreACLsEnabled())
			{
				return true;
			}
			// Allow Job-owner for any operation on the job
			if (IsMRAdmin(callerUGI) || user.Equals(jobOwner) || jobACL.IsUserAllowed(callerUGI
				))
			{
				return true;
			}
			return false;
		}
	}
}
