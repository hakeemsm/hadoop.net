using System;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet.Http;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class RMWebServices
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.RMWebServices
			).FullName);

		private const string Empty = string.Empty;

		private const string Any = "*";

		private readonly ResourceManager rm;

		private static RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private readonly Configuration conf;

		[Context]
		private HttpServletResponse response;

		public const string DelegationTokenHeader = "Hadoop-YARN-RM-Delegation-Token";

		[Com.Google.Inject.Inject]
		public RMWebServices(ResourceManager rm, Configuration conf)
		{
			this.rm = rm;
			this.conf = conf;
		}

		internal RMWebServices(ResourceManager rm, Configuration conf, HttpServletResponse
			 response)
			: this(rm, conf)
		{
			this.response = response;
		}

		protected internal virtual bool HasAccess(RMApp app, HttpServletRequest hsr)
		{
			// Check for the authorization.
			UserGroupInformation callerUGI = GetCallerUserGroupInformation(hsr, true);
			if (callerUGI != null && !(this.rm.GetApplicationACLsManager().CheckAccess(callerUGI
				, ApplicationAccessType.ViewApp, app.GetUser(), app.GetApplicationId()) || this.
				rm.GetQueueACLsManager().CheckAccess(callerUGI, QueueACL.AdministerQueue, app.GetQueue
				())))
			{
				return false;
			}
			return true;
		}

		private void Init()
		{
			//clear content type
			response.SetContentType(null);
		}

		[GET]
		public virtual ClusterInfo Get()
		{
			return GetClusterInfo();
		}

		[GET]
		public virtual ClusterInfo GetClusterInfo()
		{
			Init();
			return new ClusterInfo(this.rm);
		}

		[GET]
		public virtual ClusterMetricsInfo GetClusterMetricsInfo()
		{
			Init();
			return new ClusterMetricsInfo(this.rm);
		}

		[GET]
		public virtual SchedulerTypeInfo GetSchedulerInfo()
		{
			Init();
			ResourceScheduler rs = rm.GetResourceScheduler();
			SchedulerInfo sinfo;
			if (rs is CapacityScheduler)
			{
				CapacityScheduler cs = (CapacityScheduler)rs;
				CSQueue root = cs.GetRootQueue();
				sinfo = new CapacitySchedulerInfo(root);
			}
			else
			{
				if (rs is FairScheduler)
				{
					FairScheduler fs = (FairScheduler)rs;
					sinfo = new FairSchedulerInfo(fs);
				}
				else
				{
					if (rs is FifoScheduler)
					{
						sinfo = new FifoSchedulerInfo(this.rm);
					}
					else
					{
						throw new NotFoundException("Unknown scheduler configured");
					}
				}
			}
			return new SchedulerTypeInfo(sinfo);
		}

		/// <summary>Returns all nodes in the cluster.</summary>
		/// <remarks>
		/// Returns all nodes in the cluster. If the states param is given, returns
		/// all nodes that are in the comma-separated list of states.
		/// </remarks>
		[GET]
		public virtual NodesInfo GetNodes(string states)
		{
			Init();
			ResourceScheduler sched = this.rm.GetResourceScheduler();
			if (sched == null)
			{
				throw new NotFoundException("Null ResourceScheduler instance");
			}
			EnumSet<NodeState> acceptedStates;
			if (states == null)
			{
				acceptedStates = EnumSet.AllOf<NodeState>();
			}
			else
			{
				acceptedStates = EnumSet.NoneOf<NodeState>();
				foreach (string stateStr in states.Split(","))
				{
					acceptedStates.AddItem(NodeState.ValueOf(StringUtils.ToUpperCase(stateStr)));
				}
			}
			ICollection<RMNode> rmNodes = RMServerUtils.QueryRMNodes(this.rm.GetRMContext(), 
				acceptedStates);
			NodesInfo nodesInfo = new NodesInfo();
			foreach (RMNode rmNode in rmNodes)
			{
				NodeInfo nodeInfo = new NodeInfo(rmNode, sched);
				if (EnumSet.Of(NodeState.Lost, NodeState.Decommissioned, NodeState.Rebooted).Contains
					(rmNode.GetState()))
				{
					nodeInfo.SetNodeHTTPAddress(Empty);
				}
				nodesInfo.Add(nodeInfo);
			}
			return nodesInfo;
		}

		[GET]
		public virtual NodeInfo GetNode(string nodeId)
		{
			Init();
			if (nodeId == null || nodeId.IsEmpty())
			{
				throw new NotFoundException("nodeId, " + nodeId + ", is empty or null");
			}
			ResourceScheduler sched = this.rm.GetResourceScheduler();
			if (sched == null)
			{
				throw new NotFoundException("Null ResourceScheduler instance");
			}
			NodeId nid = ConverterUtils.ToNodeId(nodeId);
			RMNode ni = this.rm.GetRMContext().GetRMNodes()[nid];
			bool isInactive = false;
			if (ni == null)
			{
				ni = this.rm.GetRMContext().GetInactiveRMNodes()[nid.GetHost()];
				if (ni == null)
				{
					throw new NotFoundException("nodeId, " + nodeId + ", is not found");
				}
				isInactive = true;
			}
			NodeInfo nodeInfo = new NodeInfo(ni, sched);
			if (isInactive)
			{
				nodeInfo.SetNodeHTTPAddress(Empty);
			}
			return nodeInfo;
		}

		[GET]
		public virtual AppsInfo GetApps(HttpServletRequest hsr, string stateQuery, ICollection
			<string> statesQuery, string finalStatusQuery, string userQuery, string queueQuery
			, string count, string startedBegin, string startedEnd, string finishBegin, string
			 finishEnd, ICollection<string> applicationTypes, ICollection<string> applicationTags
			)
		{
			bool checkCount = false;
			bool checkStart = false;
			bool checkEnd = false;
			bool checkAppTypes = false;
			bool checkAppStates = false;
			bool checkAppTags = false;
			long countNum = 0;
			// set values suitable in case both of begin/end not specified
			long sBegin = 0;
			long sEnd = long.MaxValue;
			long fBegin = 0;
			long fEnd = long.MaxValue;
			Init();
			if (count != null && !count.IsEmpty())
			{
				checkCount = true;
				countNum = long.Parse(count);
				if (countNum <= 0)
				{
					throw new BadRequestException("limit value must be greater then 0");
				}
			}
			if (startedBegin != null && !startedBegin.IsEmpty())
			{
				checkStart = true;
				sBegin = long.Parse(startedBegin);
				if (sBegin < 0)
				{
					throw new BadRequestException("startedTimeBegin must be greater than 0");
				}
			}
			if (startedEnd != null && !startedEnd.IsEmpty())
			{
				checkStart = true;
				sEnd = long.Parse(startedEnd);
				if (sEnd < 0)
				{
					throw new BadRequestException("startedTimeEnd must be greater than 0");
				}
			}
			if (sBegin > sEnd)
			{
				throw new BadRequestException("startedTimeEnd must be greater than startTimeBegin"
					);
			}
			if (finishBegin != null && !finishBegin.IsEmpty())
			{
				checkEnd = true;
				fBegin = long.Parse(finishBegin);
				if (fBegin < 0)
				{
					throw new BadRequestException("finishTimeBegin must be greater than 0");
				}
			}
			if (finishEnd != null && !finishEnd.IsEmpty())
			{
				checkEnd = true;
				fEnd = long.Parse(finishEnd);
				if (fEnd < 0)
				{
					throw new BadRequestException("finishTimeEnd must be greater than 0");
				}
			}
			if (fBegin > fEnd)
			{
				throw new BadRequestException("finishTimeEnd must be greater than finishTimeBegin"
					);
			}
			ICollection<string> appTypes = ParseQueries(applicationTypes, false);
			if (!appTypes.IsEmpty())
			{
				checkAppTypes = true;
			}
			ICollection<string> appTags = ParseQueries(applicationTags, false);
			if (!appTags.IsEmpty())
			{
				checkAppTags = true;
			}
			// stateQuery is deprecated.
			if (stateQuery != null && !stateQuery.IsEmpty())
			{
				statesQuery.AddItem(stateQuery);
			}
			ICollection<string> appStates = ParseQueries(statesQuery, true);
			if (!appStates.IsEmpty())
			{
				checkAppStates = true;
			}
			GetApplicationsRequest request = GetApplicationsRequest.NewInstance();
			if (checkStart)
			{
				request.SetStartRange(sBegin, sEnd);
			}
			if (checkEnd)
			{
				request.SetFinishRange(fBegin, fEnd);
			}
			if (checkCount)
			{
				request.SetLimit(countNum);
			}
			if (checkAppTypes)
			{
				request.SetApplicationTypes(appTypes);
			}
			if (checkAppTags)
			{
				request.SetApplicationTags(appTags);
			}
			if (checkAppStates)
			{
				request.SetApplicationStates(appStates);
			}
			if (queueQuery != null && !queueQuery.IsEmpty())
			{
				ResourceScheduler rs = rm.GetResourceScheduler();
				if (rs is CapacityScheduler)
				{
					CapacityScheduler cs = (CapacityScheduler)rs;
					// validate queue exists
					try
					{
						cs.GetQueueInfo(queueQuery, false, false);
					}
					catch (IOException e)
					{
						throw new BadRequestException(e.Message);
					}
				}
				ICollection<string> queues = new HashSet<string>(1);
				queues.AddItem(queueQuery);
				request.SetQueues(queues);
			}
			if (userQuery != null && !userQuery.IsEmpty())
			{
				ICollection<string> users = new HashSet<string>(1);
				users.AddItem(userQuery);
				request.SetUsers(users);
			}
			IList<ApplicationReport> appReports = null;
			try
			{
				appReports = rm.GetClientRMService().GetApplications(request, false).GetApplicationList
					();
			}
			catch (YarnException e)
			{
				Log.Error("Unable to retrieve apps from ClientRMService", e);
				throw new YarnRuntimeException("Unable to retrieve apps from ClientRMService", e);
			}
			ConcurrentMap<ApplicationId, RMApp> apps = rm.GetRMContext().GetRMApps();
			AppsInfo allApps = new AppsInfo();
			foreach (ApplicationReport report in appReports)
			{
				RMApp rmapp = apps[report.GetApplicationId()];
				if (rmapp == null)
				{
					continue;
				}
				if (finalStatusQuery != null && !finalStatusQuery.IsEmpty())
				{
					FinalApplicationStatus.ValueOf(finalStatusQuery);
					if (!Sharpen.Runtime.EqualsIgnoreCase(rmapp.GetFinalApplicationStatus().ToString(
						), finalStatusQuery))
					{
						continue;
					}
				}
				AppInfo app = new AppInfo(rm, rmapp, HasAccess(rmapp, hsr), WebAppUtils.GetHttpSchemePrefix
					(conf));
				allApps.Add(app);
			}
			return allApps;
		}

		[GET]
		public virtual ApplicationStatisticsInfo GetAppStatistics(HttpServletRequest hsr, 
			ICollection<string> stateQueries, ICollection<string> typeQueries)
		{
			Init();
			// parse the params and build the scoreboard
			// converting state/type name to lowercase
			ICollection<string> states = ParseQueries(stateQueries, true);
			ICollection<string> types = ParseQueries(typeQueries, false);
			// if no types, counts the applications of any types
			if (types.Count == 0)
			{
				types.AddItem(Any);
			}
			else
			{
				if (types.Count != 1)
				{
					throw new BadRequestException("# of applicationTypes = " + types.Count + ", we temporarily support at most one applicationType"
						);
				}
			}
			// if no states, returns the counts of all RMAppStates
			if (states.Count == 0)
			{
				foreach (YarnApplicationState state in YarnApplicationState.Values())
				{
					states.AddItem(StringUtils.ToLowerCase(state.ToString()));
				}
			}
			// in case we extend to multiple applicationTypes in the future
			IDictionary<YarnApplicationState, IDictionary<string, long>> scoreboard = BuildScoreboard
				(states, types);
			// go through the apps in RM to count the numbers, ignoring the case of
			// the state/type name
			ConcurrentMap<ApplicationId, RMApp> apps = rm.GetRMContext().GetRMApps();
			foreach (RMApp rmapp in apps.Values)
			{
				YarnApplicationState state = rmapp.CreateApplicationState();
				string type = StringUtils.ToLowerCase(rmapp.GetApplicationType().Trim());
				if (states.Contains(StringUtils.ToLowerCase(state.ToString())))
				{
					if (types.Contains(Any))
					{
						CountApp(scoreboard, state, Any);
					}
					else
					{
						if (types.Contains(type))
						{
							CountApp(scoreboard, state, type);
						}
					}
				}
			}
			// fill the response object
			ApplicationStatisticsInfo appStatInfo = new ApplicationStatisticsInfo();
			foreach (KeyValuePair<YarnApplicationState, IDictionary<string, long>> partScoreboard
				 in scoreboard)
			{
				foreach (KeyValuePair<string, long> statEntry in partScoreboard.Value)
				{
					StatisticsItemInfo statItem = new StatisticsItemInfo(partScoreboard.Key, statEntry
						.Key, statEntry.Value);
					appStatInfo.Add(statItem);
				}
			}
			return appStatInfo;
		}

		private static ICollection<string> ParseQueries(ICollection<string> queries, bool
			 isState)
		{
			ICollection<string> @params = new HashSet<string>();
			if (!queries.IsEmpty())
			{
				foreach (string query in queries)
				{
					if (query != null && !query.Trim().IsEmpty())
					{
						string[] paramStrs = query.Split(",");
						foreach (string paramStr in paramStrs)
						{
							if (paramStr != null && !paramStr.Trim().IsEmpty())
							{
								if (isState)
								{
									try
									{
										// enum string is in the uppercase
										YarnApplicationState.ValueOf(StringUtils.ToUpperCase(paramStr.Trim()));
									}
									catch (RuntimeException)
									{
										YarnApplicationState[] stateArray = YarnApplicationState.Values();
										string allAppStates = Arrays.ToString(stateArray);
										throw new BadRequestException("Invalid application-state " + paramStr.Trim() + " specified. It should be one of "
											 + allAppStates);
									}
								}
								@params.AddItem(StringUtils.ToLowerCase(paramStr.Trim()));
							}
						}
					}
				}
			}
			return @params;
		}

		private static IDictionary<YarnApplicationState, IDictionary<string, long>> BuildScoreboard
			(ICollection<string> states, ICollection<string> types)
		{
			IDictionary<YarnApplicationState, IDictionary<string, long>> scoreboard = new Dictionary
				<YarnApplicationState, IDictionary<string, long>>();
			// default states will result in enumerating all YarnApplicationStates
			System.Diagnostics.Debug.Assert(!states.IsEmpty());
			foreach (string state in states)
			{
				IDictionary<string, long> partScoreboard = new Dictionary<string, long>();
				scoreboard[YarnApplicationState.ValueOf(StringUtils.ToUpperCase(state))] = partScoreboard;
				// types is verified no to be empty
				foreach (string type in types)
				{
					partScoreboard[type] = 0L;
				}
			}
			return scoreboard;
		}

		private static void CountApp(IDictionary<YarnApplicationState, IDictionary<string
			, long>> scoreboard, YarnApplicationState state, string type)
		{
			IDictionary<string, long> partScoreboard = scoreboard[state];
			long count = partScoreboard[type];
			partScoreboard[type] = count + 1L;
		}

		[GET]
		public virtual AppInfo GetApp(HttpServletRequest hsr, string appId)
		{
			Init();
			if (appId == null || appId.IsEmpty())
			{
				throw new NotFoundException("appId, " + appId + ", is empty or null");
			}
			ApplicationId id;
			id = ConverterUtils.ToApplicationId(recordFactory, appId);
			if (id == null)
			{
				throw new NotFoundException("appId is null");
			}
			RMApp app = rm.GetRMContext().GetRMApps()[id];
			if (app == null)
			{
				throw new NotFoundException("app with id: " + appId + " not found");
			}
			return new AppInfo(rm, app, HasAccess(app, hsr), hsr.GetScheme() + "://");
		}

		[GET]
		public virtual AppAttemptsInfo GetAppAttempts(HttpServletRequest hsr, string appId
			)
		{
			Init();
			if (appId == null || appId.IsEmpty())
			{
				throw new NotFoundException("appId, " + appId + ", is empty or null");
			}
			ApplicationId id;
			id = ConverterUtils.ToApplicationId(recordFactory, appId);
			if (id == null)
			{
				throw new NotFoundException("appId is null");
			}
			RMApp app = rm.GetRMContext().GetRMApps()[id];
			if (app == null)
			{
				throw new NotFoundException("app with id: " + appId + " not found");
			}
			AppAttemptsInfo appAttemptsInfo = new AppAttemptsInfo();
			foreach (RMAppAttempt attempt in app.GetAppAttempts().Values)
			{
				AppAttemptInfo attemptInfo = new AppAttemptInfo(rm, attempt, app.GetUser(), hsr.GetScheme
					() + "://");
				appAttemptsInfo.Add(attemptInfo);
			}
			return appAttemptsInfo;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		[GET]
		public virtual AppState GetAppState(HttpServletRequest hsr, string appId)
		{
			Init();
			UserGroupInformation callerUGI = GetCallerUserGroupInformation(hsr, true);
			string userName = string.Empty;
			if (callerUGI != null)
			{
				userName = callerUGI.GetUserName();
			}
			RMApp app = null;
			try
			{
				app = GetRMAppForAppId(appId);
			}
			catch (NotFoundException e)
			{
				RMAuditLogger.LogFailure(userName, RMAuditLogger.AuditConstants.KillAppRequest, "UNKNOWN"
					, "RMWebService", "Trying to get state of an absent application " + appId);
				throw;
			}
			AppState ret = new AppState();
			ret.SetState(app.GetState().ToString());
			return ret;
		}

		// can't return POJO because we can't control the status code
		// it's always set to 200 when we need to allow it to be set
		// to 202
		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[PUT]
		public virtual Response UpdateAppState(AppState targetState, HttpServletRequest hsr
			, string appId)
		{
			Init();
			UserGroupInformation callerUGI = GetCallerUserGroupInformation(hsr, true);
			if (callerUGI == null)
			{
				string msg = "Unable to obtain user name, user not authenticated";
				throw new AuthorizationException(msg);
			}
			if (UserGroupInformation.IsSecurityEnabled() && IsStaticUser(callerUGI))
			{
				string msg = "The default static user cannot carry out this operation.";
				return Response.Status(Response.Status.Forbidden).Entity(msg).Build();
			}
			string userName = callerUGI.GetUserName();
			RMApp app = null;
			try
			{
				app = GetRMAppForAppId(appId);
			}
			catch (NotFoundException e)
			{
				RMAuditLogger.LogFailure(userName, RMAuditLogger.AuditConstants.KillAppRequest, "UNKNOWN"
					, "RMWebService", "Trying to kill an absent application " + appId);
				throw;
			}
			if (!app.GetState().ToString().Equals(targetState.GetState()))
			{
				// user is attempting to change state. right we only
				// allow users to kill the app
				if (targetState.GetState().Equals(YarnApplicationState.Killed.ToString()))
				{
					return KillApp(app, callerUGI, hsr);
				}
				throw new BadRequestException("Only '" + YarnApplicationState.Killed.ToString() +
					 "' is allowed as a target state.");
			}
			AppState ret = new AppState();
			ret.SetState(app.GetState().ToString());
			return Response.Status(Response.Status.Ok).Entity(ret).Build();
		}

		/// <exception cref="System.IO.IOException"/>
		[GET]
		public virtual NodeToLabelsInfo GetNodeToLabels(HttpServletRequest hsr)
		{
			Init();
			NodeToLabelsInfo ntl = new NodeToLabelsInfo();
			Dictionary<string, NodeLabelsInfo> ntlMap = ntl.GetNodeToLabels();
			IDictionary<NodeId, ICollection<string>> nodeIdToLabels = rm.GetRMContext().GetNodeLabelManager
				().GetNodeLabels();
			foreach (KeyValuePair<NodeId, ICollection<string>> nitle in nodeIdToLabels)
			{
				ntlMap[nitle.Key.ToString()] = new NodeLabelsInfo(nitle.Value);
			}
			return ntl;
		}

		/// <exception cref="System.IO.IOException"/>
		[POST]
		public virtual Response ReplaceLabelsOnNodes(NodeToLabelsInfo newNodeToLabels, HttpServletRequest
			 hsr)
		{
			Init();
			UserGroupInformation callerUGI = GetCallerUserGroupInformation(hsr, true);
			if (callerUGI == null)
			{
				string msg = "Unable to obtain user name, user not authenticated for" + " post to .../replace-node-to-labels";
				throw new AuthorizationException(msg);
			}
			if (!rm.GetRMContext().GetNodeLabelManager().CheckAccess(callerUGI))
			{
				string msg = "User " + callerUGI.GetShortUserName() + " not authorized" + " for post to .../replace-node-to-labels ";
				throw new AuthorizationException(msg);
			}
			IDictionary<NodeId, ICollection<string>> nodeIdToLabels = new Dictionary<NodeId, 
				ICollection<string>>();
			foreach (KeyValuePair<string, NodeLabelsInfo> nitle in newNodeToLabels.GetNodeToLabels
				())
			{
				nodeIdToLabels[ConverterUtils.ToNodeIdWithDefaultPort(nitle.Key)] = new HashSet<string
					>(nitle.Value.GetNodeLabels());
			}
			rm.GetRMContext().GetNodeLabelManager().ReplaceLabelsOnNode(nodeIdToLabels);
			return Response.Status(Response.Status.Ok).Build();
		}

		/// <exception cref="System.IO.IOException"/>
		[GET]
		public virtual NodeLabelsInfo GetClusterNodeLabels(HttpServletRequest hsr)
		{
			Init();
			NodeLabelsInfo ret = new NodeLabelsInfo(rm.GetRMContext().GetNodeLabelManager().GetClusterNodeLabels
				());
			return ret;
		}

		/// <exception cref="System.Exception"/>
		[POST]
		public virtual Response AddToClusterNodeLabels(NodeLabelsInfo newNodeLabels, HttpServletRequest
			 hsr)
		{
			Init();
			UserGroupInformation callerUGI = GetCallerUserGroupInformation(hsr, true);
			if (callerUGI == null)
			{
				string msg = "Unable to obtain user name, user not authenticated for" + " post to .../add-node-labels";
				throw new AuthorizationException(msg);
			}
			if (!rm.GetRMContext().GetNodeLabelManager().CheckAccess(callerUGI))
			{
				string msg = "User " + callerUGI.GetShortUserName() + " not authorized" + " for post to .../add-node-labels ";
				throw new AuthorizationException(msg);
			}
			rm.GetRMContext().GetNodeLabelManager().AddToCluserNodeLabels(new HashSet<string>
				(newNodeLabels.GetNodeLabels()));
			return Response.Status(Response.Status.Ok).Build();
		}

		/// <exception cref="System.Exception"/>
		[POST]
		public virtual Response RemoveFromCluserNodeLabels(NodeLabelsInfo oldNodeLabels, 
			HttpServletRequest hsr)
		{
			Init();
			UserGroupInformation callerUGI = GetCallerUserGroupInformation(hsr, true);
			if (callerUGI == null)
			{
				string msg = "Unable to obtain user name, user not authenticated for" + " post to .../remove-node-labels";
				throw new AuthorizationException(msg);
			}
			if (!rm.GetRMContext().GetNodeLabelManager().CheckAccess(callerUGI))
			{
				string msg = "User " + callerUGI.GetShortUserName() + " not authorized" + " for post to .../remove-node-labels ";
				throw new AuthorizationException(msg);
			}
			rm.GetRMContext().GetNodeLabelManager().RemoveFromClusterNodeLabels(new HashSet<string
				>(oldNodeLabels.GetNodeLabels()));
			return Response.Status(Response.Status.Ok).Build();
		}

		/// <exception cref="System.IO.IOException"/>
		[GET]
		public virtual NodeLabelsInfo GetLabelsOnNode(HttpServletRequest hsr, string nodeId
			)
		{
			Init();
			NodeId nid = ConverterUtils.ToNodeIdWithDefaultPort(nodeId);
			return new NodeLabelsInfo(rm.GetRMContext().GetNodeLabelManager().GetLabelsOnNode
				(nid));
		}

		/// <exception cref="System.Exception"/>
		[POST]
		public virtual Response ReplaceLabelsOnNode(NodeLabelsInfo newNodeLabelsInfo, HttpServletRequest
			 hsr, string nodeId)
		{
			Init();
			UserGroupInformation callerUGI = GetCallerUserGroupInformation(hsr, true);
			if (callerUGI == null)
			{
				string msg = "Unable to obtain user name, user not authenticated for" + " post to .../nodes/nodeid/replace-labels";
				throw new AuthorizationException(msg);
			}
			if (!rm.GetRMContext().GetNodeLabelManager().CheckAccess(callerUGI))
			{
				string msg = "User " + callerUGI.GetShortUserName() + " not authorized" + " for post to .../nodes/nodeid/replace-labels";
				throw new AuthorizationException(msg);
			}
			NodeId nid = ConverterUtils.ToNodeIdWithDefaultPort(nodeId);
			IDictionary<NodeId, ICollection<string>> newLabelsForNode = new Dictionary<NodeId
				, ICollection<string>>();
			newLabelsForNode[nid] = new HashSet<string>(newNodeLabelsInfo.GetNodeLabels());
			rm.GetRMContext().GetNodeLabelManager().ReplaceLabelsOnNode(newLabelsForNode);
			return Response.Status(Response.Status.Ok).Build();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual Response KillApp(RMApp app, UserGroupInformation callerUGI
			, HttpServletRequest hsr)
		{
			if (app == null)
			{
				throw new ArgumentException("app cannot be null");
			}
			string userName = callerUGI.GetUserName();
			ApplicationId appid = app.GetApplicationId();
			KillApplicationResponse resp = null;
			try
			{
				resp = callerUGI.DoAs(new _PrivilegedExceptionAction_926(this, appid));
			}
			catch (UndeclaredThrowableException ue)
			{
				// if the root cause is a permissions issue
				// bubble that up to the user
				if (ue.InnerException is YarnException)
				{
					YarnException ye = (YarnException)ue.InnerException;
					if (ye.InnerException is AccessControlException)
					{
						string appId = app.GetApplicationId().ToString();
						string msg = "Unauthorized attempt to kill appid " + appId + " by remote user " +
							 userName;
						return Response.Status(Response.Status.Forbidden).Entity(msg).Build();
					}
					else
					{
						throw;
					}
				}
				else
				{
					throw;
				}
			}
			AppState ret = new AppState();
			ret.SetState(app.GetState().ToString());
			if (resp.GetIsKillCompleted())
			{
				RMAuditLogger.LogSuccess(userName, RMAuditLogger.AuditConstants.KillAppRequest, "RMWebService"
					, app.GetApplicationId());
			}
			else
			{
				return Response.Status(Response.Status.Accepted).Entity(ret).Header(HttpHeaders.Location
					, hsr.GetRequestURL()).Build();
			}
			return Response.Status(Response.Status.Ok).Entity(ret).Build();
		}

		private sealed class _PrivilegedExceptionAction_926 : PrivilegedExceptionAction<KillApplicationResponse
			>
		{
			public _PrivilegedExceptionAction_926(RMWebServices _enclosing, ApplicationId appid
				)
			{
				this._enclosing = _enclosing;
				this.appid = appid;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public KillApplicationResponse Run()
			{
				KillApplicationRequest req = KillApplicationRequest.NewInstance(appid);
				return this._enclosing.rm.GetClientRMService().ForceKillApplication(req);
			}

			private readonly RMWebServices _enclosing;

			private readonly ApplicationId appid;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		[GET]
		public virtual AppQueue GetAppQueue(HttpServletRequest hsr, string appId)
		{
			Init();
			UserGroupInformation callerUGI = GetCallerUserGroupInformation(hsr, true);
			string userName = "UNKNOWN-USER";
			if (callerUGI != null)
			{
				userName = callerUGI.GetUserName();
			}
			RMApp app = null;
			try
			{
				app = GetRMAppForAppId(appId);
			}
			catch (NotFoundException e)
			{
				RMAuditLogger.LogFailure(userName, RMAuditLogger.AuditConstants.KillAppRequest, "UNKNOWN"
					, "RMWebService", "Trying to get state of an absent application " + appId);
				throw;
			}
			AppQueue ret = new AppQueue();
			ret.SetQueue(app.GetQueue());
			return ret;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[PUT]
		public virtual Response UpdateAppQueue(AppQueue targetQueue, HttpServletRequest hsr
			, string appId)
		{
			Init();
			UserGroupInformation callerUGI = GetCallerUserGroupInformation(hsr, true);
			if (callerUGI == null)
			{
				string msg = "Unable to obtain user name, user not authenticated";
				throw new AuthorizationException(msg);
			}
			if (UserGroupInformation.IsSecurityEnabled() && IsStaticUser(callerUGI))
			{
				string msg = "The default static user cannot carry out this operation.";
				return Response.Status(Response.Status.Forbidden).Entity(msg).Build();
			}
			string userName = callerUGI.GetUserName();
			RMApp app = null;
			try
			{
				app = GetRMAppForAppId(appId);
			}
			catch (NotFoundException e)
			{
				RMAuditLogger.LogFailure(userName, RMAuditLogger.AuditConstants.KillAppRequest, "UNKNOWN"
					, "RMWebService", "Trying to move an absent application " + appId);
				throw;
			}
			if (!app.GetQueue().Equals(targetQueue.GetQueue()))
			{
				// user is attempting to change queue.
				return MoveApp(app, callerUGI, targetQueue.GetQueue());
			}
			AppQueue ret = new AppQueue();
			ret.SetQueue(app.GetQueue());
			return Response.Status(Response.Status.Ok).Entity(ret).Build();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual Response MoveApp(RMApp app, UserGroupInformation callerUGI
			, string targetQueue)
		{
			if (app == null)
			{
				throw new ArgumentException("app cannot be null");
			}
			string userName = callerUGI.GetUserName();
			ApplicationId appid = app.GetApplicationId();
			string reqTargetQueue = targetQueue;
			try
			{
				callerUGI.DoAs(new _PrivilegedExceptionAction_1048(this, appid, reqTargetQueue));
			}
			catch (UndeclaredThrowableException ue)
			{
				// if the root cause is a permissions issue
				// bubble that up to the user
				if (ue.InnerException is YarnException)
				{
					YarnException ye = (YarnException)ue.InnerException;
					if (ye.InnerException is AccessControlException)
					{
						string appId = app.GetApplicationId().ToString();
						string msg = "Unauthorized attempt to move appid " + appId + " by remote user " +
							 userName;
						return Response.Status(Response.Status.Forbidden).Entity(msg).Build();
					}
					else
					{
						if (ye.Message.StartsWith("App in") && ye.Message.EndsWith("state cannot be moved."
							))
						{
							return Response.Status(Response.Status.BadRequest).Entity(ye.Message).Build();
						}
						else
						{
							throw;
						}
					}
				}
				else
				{
					throw;
				}
			}
			AppQueue ret = new AppQueue();
			ret.SetQueue(app.GetQueue());
			return Response.Status(Response.Status.Ok).Entity(ret).Build();
		}

		private sealed class _PrivilegedExceptionAction_1048 : PrivilegedExceptionAction<
			Void>
		{
			public _PrivilegedExceptionAction_1048(RMWebServices _enclosing, ApplicationId appid
				, string reqTargetQueue)
			{
				this._enclosing = _enclosing;
				this.appid = appid;
				this.reqTargetQueue = reqTargetQueue;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public Void Run()
			{
				MoveApplicationAcrossQueuesRequest req = MoveApplicationAcrossQueuesRequest.NewInstance
					(appid, reqTargetQueue);
				this._enclosing.rm.GetClientRMService().MoveApplicationAcrossQueues(req);
				return null;
			}

			private readonly RMWebServices _enclosing;

			private readonly ApplicationId appid;

			private readonly string reqTargetQueue;
		}

		private RMApp GetRMAppForAppId(string appId)
		{
			if (appId == null || appId.IsEmpty())
			{
				throw new NotFoundException("appId, " + appId + ", is empty or null");
			}
			ApplicationId id;
			try
			{
				id = ConverterUtils.ToApplicationId(recordFactory, appId);
			}
			catch (FormatException)
			{
				throw new NotFoundException("appId is invalid");
			}
			if (id == null)
			{
				throw new NotFoundException("appId is invalid");
			}
			RMApp app = rm.GetRMContext().GetRMApps()[id];
			if (app == null)
			{
				throw new NotFoundException("app with id: " + appId + " not found");
			}
			return app;
		}

		private UserGroupInformation GetCallerUserGroupInformation(HttpServletRequest hsr
			, bool usePrincipal)
		{
			string remoteUser = hsr.GetRemoteUser();
			if (usePrincipal)
			{
				Principal princ = hsr.GetUserPrincipal();
				remoteUser = princ == null ? null : princ.GetName();
			}
			UserGroupInformation callerUGI = null;
			if (remoteUser != null)
			{
				callerUGI = UserGroupInformation.CreateRemoteUser(remoteUser);
			}
			return callerUGI;
		}

		private bool IsStaticUser(UserGroupInformation callerUGI)
		{
			string staticUser = conf.Get(CommonConfigurationKeys.HadoopHttpStaticUser, CommonConfigurationKeys
				.DefaultHadoopHttpStaticUser);
			return staticUser.Equals(callerUGI.GetUserName());
		}

		/// <summary>Generates a new ApplicationId which is then sent to the client</summary>
		/// <param name="hsr">the servlet request</param>
		/// <returns>
		/// Response containing the app id and the maximum resource
		/// capabilities
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[POST]
		public virtual Response CreateNewApplication(HttpServletRequest hsr)
		{
			Init();
			UserGroupInformation callerUGI = GetCallerUserGroupInformation(hsr, true);
			if (callerUGI == null)
			{
				throw new AuthorizationException("Unable to obtain user name, " + "user not authenticated"
					);
			}
			if (UserGroupInformation.IsSecurityEnabled() && IsStaticUser(callerUGI))
			{
				string msg = "The default static user cannot carry out this operation.";
				return Response.Status(Response.Status.Forbidden).Entity(msg).Build();
			}
			NewApplication appId = CreateNewApplication();
			return Response.Status(Response.Status.Ok).Entity(appId).Build();
		}

		// reuse the code in ClientRMService to create new app
		// get the new app id and submit app
		// set location header with new app location
		/// <summary>Function to submit an app to the RM</summary>
		/// <param name="newApp">
		/// structure containing information to construct the
		/// ApplicationSubmissionContext
		/// </param>
		/// <param name="hsr">the servlet request</param>
		/// <returns>Response containing the status code</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[POST]
		public virtual Response SubmitApplication(ApplicationSubmissionContextInfo newApp
			, HttpServletRequest hsr)
		{
			Init();
			UserGroupInformation callerUGI = GetCallerUserGroupInformation(hsr, true);
			if (callerUGI == null)
			{
				throw new AuthorizationException("Unable to obtain user name, " + "user not authenticated"
					);
			}
			if (UserGroupInformation.IsSecurityEnabled() && IsStaticUser(callerUGI))
			{
				string msg = "The default static user cannot carry out this operation.";
				return Response.Status(Response.Status.Forbidden).Entity(msg).Build();
			}
			ApplicationSubmissionContext appContext = CreateAppSubmissionContext(newApp);
			SubmitApplicationRequest req = SubmitApplicationRequest.NewInstance(appContext);
			try
			{
				callerUGI.DoAs(new _PrivilegedExceptionAction_1207(this, req));
			}
			catch (UndeclaredThrowableException ue)
			{
				if (ue.InnerException is YarnException)
				{
					throw new BadRequestException(ue.InnerException.Message);
				}
				Log.Info("Submit app request failed", ue);
				throw;
			}
			string url = hsr.GetRequestURL() + "/" + newApp.GetApplicationId();
			return Response.Status(Response.Status.Accepted).Header(HttpHeaders.Location, url
				).Build();
		}

		private sealed class _PrivilegedExceptionAction_1207 : PrivilegedExceptionAction<
			SubmitApplicationResponse>
		{
			public _PrivilegedExceptionAction_1207(RMWebServices _enclosing, SubmitApplicationRequest
				 req)
			{
				this._enclosing = _enclosing;
				this.req = req;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public SubmitApplicationResponse Run()
			{
				return this._enclosing.rm.GetClientRMService().SubmitApplication(req);
			}

			private readonly RMWebServices _enclosing;

			private readonly SubmitApplicationRequest req;
		}

		/// <summary>
		/// Function that actually creates the ApplicationId by calling the
		/// ClientRMService
		/// </summary>
		/// <returns>
		/// returns structure containing the app-id and maximum resource
		/// capabilities
		/// </returns>
		private NewApplication CreateNewApplication()
		{
			GetNewApplicationRequest req = recordFactory.NewRecordInstance<GetNewApplicationRequest
				>();
			GetNewApplicationResponse resp;
			try
			{
				resp = rm.GetClientRMService().GetNewApplication(req);
			}
			catch (YarnException e)
			{
				string msg = "Unable to create new app from RM web service";
				Log.Error(msg, e);
				throw new YarnRuntimeException(msg, e);
			}
			NewApplication appId = new NewApplication(resp.GetApplicationId().ToString(), new 
				ResourceInfo(resp.GetMaximumResourceCapability()));
			return appId;
		}

		/// <summary>
		/// Create the actual ApplicationSubmissionContext to be submitted to the RM
		/// from the information provided by the user.
		/// </summary>
		/// <param name="newApp">the information provided by the user</param>
		/// <returns>returns the constructed ApplicationSubmissionContext</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual ApplicationSubmissionContext CreateAppSubmissionContext
			(ApplicationSubmissionContextInfo newApp)
		{
			// create local resources and app submission context
			ApplicationId appid;
			string error = "Could not parse application id " + newApp.GetApplicationId();
			try
			{
				appid = ConverterUtils.ToApplicationId(recordFactory, newApp.GetApplicationId());
			}
			catch (Exception)
			{
				throw new BadRequestException(error);
			}
			ApplicationSubmissionContext appContext = ApplicationSubmissionContext.NewInstance
				(appid, newApp.GetApplicationName(), newApp.GetQueue(), Priority.NewInstance(newApp
				.GetPriority()), CreateContainerLaunchContext(newApp), newApp.GetUnmanagedAM(), 
				newApp.GetCancelTokensWhenComplete(), newApp.GetMaxAppAttempts(), CreateAppSubmissionContextResource
				(newApp), newApp.GetApplicationType(), newApp.GetKeepContainersAcrossApplicationAttempts
				(), newApp.GetAppNodeLabelExpression(), newApp.GetAMContainerNodeLabelExpression
				());
			appContext.SetApplicationTags(newApp.GetApplicationTags());
			return appContext;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Webapp.BadRequestException"/>
		protected internal virtual Resource CreateAppSubmissionContextResource(ApplicationSubmissionContextInfo
			 newApp)
		{
			if (newApp.GetResource().GetvCores() > rm.GetConfig().GetInt(YarnConfiguration.RmSchedulerMaximumAllocationVcores
				, YarnConfiguration.DefaultRmSchedulerMaximumAllocationVcores))
			{
				string msg = "Requested more cores than configured max";
				throw new BadRequestException(msg);
			}
			if (newApp.GetResource().GetMemory() > rm.GetConfig().GetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb
				, YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb))
			{
				string msg = "Requested more memory than configured max";
				throw new BadRequestException(msg);
			}
			Resource r = Resource.NewInstance(newApp.GetResource().GetMemory(), newApp.GetResource
				().GetvCores());
			return r;
		}

		/// <summary>
		/// Create the ContainerLaunchContext required for the
		/// ApplicationSubmissionContext.
		/// </summary>
		/// <remarks>
		/// Create the ContainerLaunchContext required for the
		/// ApplicationSubmissionContext. This function takes the user information and
		/// generates the ByteBuffer structures required by the ContainerLaunchContext
		/// </remarks>
		/// <param name="newApp">the information provided by the user</param>
		/// <returns>created context</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Webapp.BadRequestException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual ContainerLaunchContext CreateContainerLaunchContext(ApplicationSubmissionContextInfo
			 newApp)
		{
			// create container launch context
			Dictionary<string, ByteBuffer> hmap = new Dictionary<string, ByteBuffer>();
			foreach (KeyValuePair<string, string> entry in newApp.GetContainerLaunchContextInfo
				().GetAuxillaryServiceData())
			{
				if (entry.Value.IsEmpty() == false)
				{
					Base64 decoder = new Base64(0, null, true);
					byte[] data = decoder.Decode(entry.Value);
					hmap[entry.Key] = ByteBuffer.Wrap(data);
				}
			}
			Dictionary<string, LocalResource> hlr = new Dictionary<string, LocalResource>();
			foreach (KeyValuePair<string, LocalResourceInfo> entry_1 in newApp.GetContainerLaunchContextInfo
				().GetResources())
			{
				LocalResourceInfo l = entry_1.Value;
				LocalResource lr = LocalResource.NewInstance(ConverterUtils.GetYarnUrlFromURI(l.GetUrl
					()), l.GetType(), l.GetVisibility(), l.GetSize(), l.GetTimestamp());
				hlr[entry_1.Key] = lr;
			}
			DataOutputBuffer @out = new DataOutputBuffer();
			Credentials cs = CreateCredentials(newApp.GetContainerLaunchContextInfo().GetCredentials
				());
			cs.WriteTokenStorageToStream(@out);
			ByteBuffer tokens = ByteBuffer.Wrap(@out.GetData());
			ContainerLaunchContext ctx = ContainerLaunchContext.NewInstance(hlr, newApp.GetContainerLaunchContextInfo
				().GetEnvironment(), newApp.GetContainerLaunchContextInfo().GetCommands(), hmap, 
				tokens, newApp.GetContainerLaunchContextInfo().GetAcls());
			return ctx;
		}

		/// <summary>
		/// Generate a Credentials object from the information in the CredentialsInfo
		/// object.
		/// </summary>
		/// <param name="credentials">the CredentialsInfo provided by the user.</param>
		/// <returns/>
		private Credentials CreateCredentials(CredentialsInfo credentials)
		{
			Credentials ret = new Credentials();
			try
			{
				foreach (KeyValuePair<string, string> entry in credentials.GetTokens())
				{
					Text alias = new Text(entry.Key);
					Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
						<TokenIdentifier>();
					token.DecodeFromUrlString(entry.Value);
					ret.AddToken(alias, token);
				}
				foreach (KeyValuePair<string, string> entry_1 in credentials.GetSecrets())
				{
					Text alias = new Text(entry_1.Key);
					Base64 decoder = new Base64(0, null, true);
					byte[] secret = decoder.Decode(entry_1.Value);
					ret.AddSecretKey(alias, secret);
				}
			}
			catch (IOException ie)
			{
				throw new BadRequestException("Could not parse credentials data; exception message = "
					 + ie.Message);
			}
			return ret;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private UserGroupInformation CreateKerberosUserGroupInformation(HttpServletRequest
			 hsr)
		{
			UserGroupInformation callerUGI = GetCallerUserGroupInformation(hsr, true);
			if (callerUGI == null)
			{
				string msg = "Unable to obtain user name, user not authenticated";
				throw new AuthorizationException(msg);
			}
			string authType = hsr.GetAuthType();
			if (!Sharpen.Runtime.EqualsIgnoreCase(KerberosAuthenticationHandler.Type, authType
				))
			{
				string msg = "Delegation token operations can only be carried out on a " + "Kerberos authenticated channel. Expected auth type is "
					 + KerberosAuthenticationHandler.Type + ", got type " + authType;
				throw new YarnException(msg);
			}
			if (hsr.GetAttribute(DelegationTokenAuthenticationHandler.DelegationTokenUgiAttribute
				) != null)
			{
				string msg = "Delegation token operations cannot be carried out using delegation"
					 + " token authentication.";
				throw new YarnException(msg);
			}
			callerUGI.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				);
			return callerUGI;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[POST]
		public virtual Response PostDelegationToken(DelegationToken tokenData, HttpServletRequest
			 hsr)
		{
			Init();
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = CreateKerberosUserGroupInformation(hsr);
			}
			catch (YarnException ye)
			{
				return Response.Status(Response.Status.Forbidden).Entity(ye.Message).Build();
			}
			return CreateDelegationToken(tokenData, hsr, callerUGI);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[POST]
		public virtual Response PostDelegationTokenExpiration(HttpServletRequest hsr)
		{
			Init();
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = CreateKerberosUserGroupInformation(hsr);
			}
			catch (YarnException ye)
			{
				return Response.Status(Response.Status.Forbidden).Entity(ye.Message).Build();
			}
			DelegationToken requestToken = new DelegationToken();
			requestToken.SetToken(ExtractToken(hsr).EncodeToUrlString());
			return RenewDelegationToken(requestToken, hsr, callerUGI);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private Response CreateDelegationToken(DelegationToken tokenData, HttpServletRequest
			 hsr, UserGroupInformation callerUGI)
		{
			string renewer = tokenData.GetRenewer();
			GetDelegationTokenResponse resp;
			try
			{
				resp = callerUGI.DoAs(new _PrivilegedExceptionAction_1475(this, renewer));
			}
			catch (Exception e)
			{
				Log.Info("Create delegation token request failed", e);
				throw;
			}
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> tk = new Org.Apache.Hadoop.Security.Token.Token
				<RMDelegationTokenIdentifier>(((byte[])resp.GetRMDelegationToken().GetIdentifier
				().Array()), ((byte[])resp.GetRMDelegationToken().GetPassword().Array()), new Text
				(resp.GetRMDelegationToken().GetKind()), new Text(resp.GetRMDelegationToken().GetService
				()));
			RMDelegationTokenIdentifier identifier = tk.DecodeIdentifier();
			long currentExpiration = rm.GetRMContext().GetRMDelegationTokenSecretManager().GetRenewDate
				(identifier);
			DelegationToken respToken = new DelegationToken(tk.EncodeToUrlString(), renewer, 
				identifier.GetOwner().ToString(), tk.GetKind().ToString(), currentExpiration, identifier
				.GetMaxDate());
			return Response.Status(Response.Status.Ok).Entity(respToken).Build();
		}

		private sealed class _PrivilegedExceptionAction_1475 : PrivilegedExceptionAction<
			GetDelegationTokenResponse>
		{
			public _PrivilegedExceptionAction_1475(RMWebServices _enclosing, string renewer)
			{
				this._enclosing = _enclosing;
				this.renewer = renewer;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public GetDelegationTokenResponse Run()
			{
				GetDelegationTokenRequest createReq = GetDelegationTokenRequest.NewInstance(renewer
					);
				return this._enclosing.rm.GetClientRMService().GetDelegationToken(createReq);
			}

			private readonly RMWebServices _enclosing;

			private readonly string renewer;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private Response RenewDelegationToken(DelegationToken tokenData, HttpServletRequest
			 hsr, UserGroupInformation callerUGI)
		{
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token = ExtractToken
				(tokenData.GetToken());
			Org.Apache.Hadoop.Yarn.Api.Records.Token dToken = BuilderUtils.NewDelegationToken
				(token.GetIdentifier(), token.GetKind().ToString(), token.GetPassword(), token.GetService
				().ToString());
			RenewDelegationTokenRequest req = RenewDelegationTokenRequest.NewInstance(dToken);
			RenewDelegationTokenResponse resp;
			try
			{
				resp = callerUGI.DoAs(new _PrivilegedExceptionAction_1523(this, req));
			}
			catch (UndeclaredThrowableException ue)
			{
				if (ue.InnerException is YarnException)
				{
					if (ue.InnerException.InnerException is SecretManager.InvalidToken)
					{
						throw new BadRequestException(ue.InnerException.InnerException.Message);
					}
					else
					{
						if (ue.InnerException.InnerException is AccessControlException)
						{
							return Response.Status(Response.Status.Forbidden).Entity(ue.InnerException.InnerException
								.Message).Build();
						}
					}
					Log.Info("Renew delegation token request failed", ue);
					throw;
				}
				Log.Info("Renew delegation token request failed", ue);
				throw;
			}
			catch (Exception e)
			{
				Log.Info("Renew delegation token request failed", e);
				throw;
			}
			long renewTime = resp.GetNextExpirationTime();
			DelegationToken respToken = new DelegationToken();
			respToken.SetNextExpirationTime(renewTime);
			return Response.Status(Response.Status.Ok).Entity(respToken).Build();
		}

		private sealed class _PrivilegedExceptionAction_1523 : PrivilegedExceptionAction<
			RenewDelegationTokenResponse>
		{
			public _PrivilegedExceptionAction_1523(RMWebServices _enclosing, RenewDelegationTokenRequest
				 req)
			{
				this._enclosing = _enclosing;
				this.req = req;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public RenewDelegationTokenResponse Run()
			{
				return this._enclosing.rm.GetClientRMService().RenewDelegationToken(req);
			}

			private readonly RMWebServices _enclosing;

			private readonly RenewDelegationTokenRequest req;
		}

		// For cancelling tokens, the encoded token is passed as a header
		// There are two reasons for this -
		// 1. Passing a request body as part of a DELETE request is not
		// allowed by Jetty
		// 2. Passing the encoded token as part of the url is not ideal
		// since urls tend to get logged and anyone with access to
		// the logs can extract tokens which are meant to be secret
		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[DELETE]
		public virtual Response CancelDelegationToken(HttpServletRequest hsr)
		{
			Init();
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = CreateKerberosUserGroupInformation(hsr);
			}
			catch (YarnException ye)
			{
				return Response.Status(Response.Status.Forbidden).Entity(ye.Message).Build();
			}
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token = ExtractToken
				(hsr);
			Org.Apache.Hadoop.Yarn.Api.Records.Token dToken = BuilderUtils.NewDelegationToken
				(token.GetIdentifier(), token.GetKind().ToString(), token.GetPassword(), token.GetService
				().ToString());
			CancelDelegationTokenRequest req = CancelDelegationTokenRequest.NewInstance(dToken
				);
			try
			{
				callerUGI.DoAs(new _PrivilegedExceptionAction_1586(this, req));
			}
			catch (UndeclaredThrowableException ue)
			{
				if (ue.InnerException is YarnException)
				{
					if (ue.InnerException.InnerException is SecretManager.InvalidToken)
					{
						throw new BadRequestException(ue.InnerException.InnerException.Message);
					}
					else
					{
						if (ue.InnerException.InnerException is AccessControlException)
						{
							return Response.Status(Response.Status.Forbidden).Entity(ue.InnerException.InnerException
								.Message).Build();
						}
					}
					Log.Info("Renew delegation token request failed", ue);
					throw;
				}
				Log.Info("Renew delegation token request failed", ue);
				throw;
			}
			catch (Exception e)
			{
				Log.Info("Renew delegation token request failed", e);
				throw;
			}
			return Response.Status(Response.Status.Ok).Build();
		}

		private sealed class _PrivilegedExceptionAction_1586 : PrivilegedExceptionAction<
			CancelDelegationTokenResponse>
		{
			public _PrivilegedExceptionAction_1586(RMWebServices _enclosing, CancelDelegationTokenRequest
				 req)
			{
				this._enclosing = _enclosing;
				this.req = req;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public CancelDelegationTokenResponse Run()
			{
				return this._enclosing.rm.GetClientRMService().CancelDelegationToken(req);
			}

			private readonly RMWebServices _enclosing;

			private readonly CancelDelegationTokenRequest req;
		}

		private Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> ExtractToken
			(HttpServletRequest request)
		{
			string encodedToken = request.GetHeader(DelegationTokenHeader);
			if (encodedToken == null)
			{
				string msg = "Header '" + DelegationTokenHeader + "' containing encoded token not found";
				throw new BadRequestException(msg);
			}
			return ExtractToken(encodedToken);
		}

		private Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> ExtractToken
			(string encodedToken)
		{
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<RMDelegationTokenIdentifier>();
			try
			{
				token.DecodeFromUrlString(encodedToken);
			}
			catch (Exception)
			{
				string msg = "Could not decode encoded token";
				throw new BadRequestException(msg);
			}
			return token;
		}
	}
}
