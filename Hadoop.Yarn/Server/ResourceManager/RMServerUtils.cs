using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	/// <summary>Utility methods to aid serving RM data through the REST and RPC APIs</summary>
	public class RMServerUtils
	{
		public static IList<RMNode> QueryRMNodes(RMContext context, EnumSet<NodeState> acceptedStates
			)
		{
			// nodes contains nodes that are NEW, RUNNING OR UNHEALTHY
			AList<RMNode> results = new AList<RMNode>();
			if (acceptedStates.Contains(NodeState.New) || acceptedStates.Contains(NodeState.Running
				) || acceptedStates.Contains(NodeState.Unhealthy))
			{
				foreach (RMNode rmNode in context.GetRMNodes().Values)
				{
					if (acceptedStates.Contains(rmNode.GetState()))
					{
						results.AddItem(rmNode);
					}
				}
			}
			// inactiveNodes contains nodes that are DECOMMISSIONED, LOST, OR REBOOTED
			if (acceptedStates.Contains(NodeState.Decommissioned) || acceptedStates.Contains(
				NodeState.Lost) || acceptedStates.Contains(NodeState.Rebooted))
			{
				foreach (RMNode rmNode in context.GetInactiveRMNodes().Values)
				{
					if (acceptedStates.Contains(rmNode.GetState()))
					{
						results.AddItem(rmNode);
					}
				}
			}
			return results;
		}

		/// <summary>
		/// Utility method to validate a list resource requests, by insuring that the
		/// requested memory/vcore is non-negative and not greater than max
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidResourceRequestException
		/// 	"/>
		public static void NormalizeAndValidateRequests(IList<ResourceRequest> ask, Resource
			 maximumResource, string queueName, YarnScheduler scheduler, RMContext rmContext
			)
		{
			QueueInfo queueInfo = null;
			try
			{
				queueInfo = scheduler.GetQueueInfo(queueName, false, false);
			}
			catch (IOException)
			{
			}
			foreach (ResourceRequest resReq in ask)
			{
				SchedulerUtils.NormalizeAndvalidateRequest(resReq, maximumResource, queueName, scheduler
					, rmContext, queueInfo);
			}
		}

		/*
		* @throw <code>InvalidResourceBlacklistRequestException </code> if the
		* resource is not able to be added to the blacklist.
		*/
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidResourceBlacklistRequestException
		/// 	"/>
		public static void ValidateBlacklistRequest(ResourceBlacklistRequest blacklistRequest
			)
		{
			if (blacklistRequest != null)
			{
				IList<string> plus = blacklistRequest.GetBlacklistAdditions();
				if (plus != null && plus.Contains(ResourceRequest.Any))
				{
					throw new InvalidResourceBlacklistRequestException("Cannot add " + ResourceRequest
						.Any + " to the blacklist!");
				}
			}
		}

		/// <summary>
		/// It will validate to make sure all the containers belong to correct
		/// application attempt id.
		/// </summary>
		/// <remarks>
		/// It will validate to make sure all the containers belong to correct
		/// application attempt id. If not then it will throw
		/// <see cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidContainerReleaseException"/>
		/// </remarks>
		/// <param name="containerReleaseList">containers to be released as requested by application master.
		/// 	</param>
		/// <param name="appAttemptId">Application attempt Id</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidContainerReleaseException
		/// 	"/>
		public static void ValidateContainerReleaseRequest(IList<ContainerId> containerReleaseList
			, ApplicationAttemptId appAttemptId)
		{
			foreach (ContainerId cId in containerReleaseList)
			{
				if (!appAttemptId.Equals(cId.GetApplicationAttemptId()))
				{
					throw new InvalidContainerReleaseException("Cannot release container : " + cId.ToString
						() + " not belonging to this application attempt : " + appAttemptId);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static UserGroupInformation VerifyAdminAccess(YarnAuthorizationProvider authorizer
			, string method, Log Log)
		{
			// by default, this method will use AdminService as module name
			return VerifyAdminAccess(authorizer, method, "AdminService", Log);
		}

		/// <summary>
		/// Utility method to verify if the current user has access based on the
		/// passed
		/// <see cref="Org.Apache.Hadoop.Security.Authorize.AccessControlList"/>
		/// </summary>
		/// <param name="authorizer">
		/// the
		/// <see cref="Org.Apache.Hadoop.Security.Authorize.AccessControlList"/>
		/// to check against
		/// </param>
		/// <param name="method">the method name to be logged</param>
		/// <param name="module">like AdminService or NodeLabelManager</param>
		/// <param name="Log">the logger to use</param>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Security.UserGroupInformation"/>
		/// of the current user
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static UserGroupInformation VerifyAdminAccess(YarnAuthorizationProvider authorizer
			, string method, string module, Log Log)
		{
			UserGroupInformation user;
			try
			{
				user = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ioe)
			{
				Log.Warn("Couldn't get current user", ioe);
				RMAuditLogger.LogFailure("UNKNOWN", method, string.Empty, "AdminService", "Couldn't get current user"
					);
				throw;
			}
			if (!authorizer.IsAdmin(user))
			{
				Log.Warn("User " + user.GetShortUserName() + " doesn't have permission" + " to call '"
					 + method + "'");
				RMAuditLogger.LogFailure(user.GetShortUserName(), method, string.Empty, module, RMAuditLogger.AuditConstants
					.UnauthorizedUser);
				throw new AccessControlException("User " + user.GetShortUserName() + " doesn't have permission"
					 + " to call '" + method + "'");
			}
			if (Log.IsTraceEnabled())
			{
				Log.Trace(method + " invoked by user " + user.GetShortUserName());
			}
			return user;
		}

		public static YarnApplicationState CreateApplicationState(RMAppState rmAppState)
		{
			switch (rmAppState)
			{
				case RMAppState.New:
				{
					return YarnApplicationState.New;
				}

				case RMAppState.NewSaving:
				{
					return YarnApplicationState.NewSaving;
				}

				case RMAppState.Submitted:
				{
					return YarnApplicationState.Submitted;
				}

				case RMAppState.Accepted:
				{
					return YarnApplicationState.Accepted;
				}

				case RMAppState.Running:
				{
					return YarnApplicationState.Running;
				}

				case RMAppState.Finishing:
				case RMAppState.Finished:
				{
					return YarnApplicationState.Finished;
				}

				case RMAppState.Killed:
				{
					return YarnApplicationState.Killed;
				}

				case RMAppState.Failed:
				{
					return YarnApplicationState.Failed;
				}

				default:
				{
					throw new YarnRuntimeException("Unknown state passed!");
				}
			}
		}

		public static YarnApplicationAttemptState CreateApplicationAttemptState(RMAppAttemptState
			 rmAppAttemptState)
		{
			switch (rmAppAttemptState)
			{
				case RMAppAttemptState.New:
				{
					return YarnApplicationAttemptState.New;
				}

				case RMAppAttemptState.Submitted:
				{
					return YarnApplicationAttemptState.Submitted;
				}

				case RMAppAttemptState.Scheduled:
				{
					return YarnApplicationAttemptState.Scheduled;
				}

				case RMAppAttemptState.Allocated:
				{
					return YarnApplicationAttemptState.Allocated;
				}

				case RMAppAttemptState.Launched:
				{
					return YarnApplicationAttemptState.Launched;
				}

				case RMAppAttemptState.AllocatedSaving:
				case RMAppAttemptState.LaunchedUnmanagedSaving:
				{
					return YarnApplicationAttemptState.AllocatedSaving;
				}

				case RMAppAttemptState.Running:
				{
					return YarnApplicationAttemptState.Running;
				}

				case RMAppAttemptState.Finishing:
				{
					return YarnApplicationAttemptState.Finishing;
				}

				case RMAppAttemptState.Finished:
				{
					return YarnApplicationAttemptState.Finished;
				}

				case RMAppAttemptState.Killed:
				{
					return YarnApplicationAttemptState.Killed;
				}

				case RMAppAttemptState.Failed:
				{
					return YarnApplicationAttemptState.Failed;
				}

				default:
				{
					throw new YarnRuntimeException("Unknown state passed!");
				}
			}
		}

		/// <summary>Statically defined dummy ApplicationResourceUsageREport.</summary>
		/// <remarks>
		/// Statically defined dummy ApplicationResourceUsageREport.  Used as
		/// a return value when a valid report cannot be found.
		/// </remarks>
		public static readonly ApplicationResourceUsageReport DummyApplicationResourceUsageReport
			 = BuilderUtils.NewApplicationResourceUsageReport(-1, -1, Resources.CreateResource
			(-1, -1), Resources.CreateResource(-1, -1), Resources.CreateResource(-1, -1), 0, 
			0);

		/// <summary>
		/// Find all configs whose name starts with
		/// YarnConfiguration.RM_PROXY_USER_PREFIX, and add a record for each one by
		/// replacing the prefix with ProxyUsers.CONF_HADOOP_PROXYUSER
		/// </summary>
		public static void ProcessRMProxyUsersConf(Configuration conf)
		{
			IDictionary<string, string> rmProxyUsers = new Dictionary<string, string>();
			foreach (KeyValuePair<string, string> entry in conf)
			{
				string propName = entry.Key;
				if (propName.StartsWith(YarnConfiguration.RmProxyUserPrefix))
				{
					rmProxyUsers[ProxyUsers.ConfHadoopProxyuser + "." + Sharpen.Runtime.Substring(propName
						, YarnConfiguration.RmProxyUserPrefix.Length)] = entry.Value;
				}
			}
			foreach (KeyValuePair<string, string> entry_1 in rmProxyUsers)
			{
				conf.Set(entry_1.Key, entry_1.Value);
			}
		}
	}
}
