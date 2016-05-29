using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>
	/// <see cref="ActiveUsersManager"/>
	/// tracks active users in the system.
	/// A user is deemed to be active if he has any running applications with
	/// outstanding resource requests.
	/// An active user is defined as someone with outstanding resource requests.
	/// </summary>
	public class ActiveUsersManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.ActiveUsersManager
			));

		private readonly QueueMetrics metrics;

		private int activeUsers = 0;

		private IDictionary<string, ICollection<ApplicationId>> usersApplications = new Dictionary
			<string, ICollection<ApplicationId>>();

		public ActiveUsersManager(QueueMetrics metrics)
		{
			this.metrics = metrics;
		}

		/// <summary>An application has new outstanding requests.</summary>
		/// <param name="user">application user</param>
		/// <param name="applicationId">activated application</param>
		public virtual void ActivateApplication(string user, ApplicationId applicationId)
		{
			lock (this)
			{
				ICollection<ApplicationId> userApps = usersApplications[user];
				if (userApps == null)
				{
					userApps = new HashSet<ApplicationId>();
					usersApplications[user] = userApps;
					++activeUsers;
					metrics.IncrActiveUsers();
					Log.Debug("User " + user + " added to activeUsers, currently: " + activeUsers);
				}
				if (userApps.AddItem(applicationId))
				{
					metrics.ActivateApp(user);
				}
			}
		}

		/// <summary>An application has no more outstanding requests.</summary>
		/// <param name="user">application user</param>
		/// <param name="applicationId">deactivated application</param>
		public virtual void DeactivateApplication(string user, ApplicationId applicationId
			)
		{
			lock (this)
			{
				ICollection<ApplicationId> userApps = usersApplications[user];
				if (userApps != null)
				{
					if (userApps.Remove(applicationId))
					{
						metrics.DeactivateApp(user);
					}
					if (userApps.IsEmpty())
					{
						Sharpen.Collections.Remove(usersApplications, user);
						--activeUsers;
						metrics.DecrActiveUsers();
						Log.Debug("User " + user + " removed from activeUsers, currently: " + activeUsers
							);
					}
				}
			}
		}

		/// <summary>Get number of active users i.e.</summary>
		/// <remarks>
		/// Get number of active users i.e. users with applications which have pending
		/// resource requests.
		/// </remarks>
		/// <returns>number of active users</returns>
		public virtual int GetNumActiveUsers()
		{
			lock (this)
			{
				return activeUsers;
			}
		}
	}
}
