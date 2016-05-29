using System.Collections.Generic;
using Javax.Servlet.Http;
using Javax.WS.RS;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Webapp;
using Org.Apache.Hadoop.Yarn.Server.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Webapp
{
	public class AHSWebServices : WebServices
	{
		[Com.Google.Inject.Inject]
		public AHSWebServices(ApplicationBaseProtocol appBaseProt)
			: base(appBaseProt)
		{
		}

		[GET]
		public virtual AppsInfo Get(HttpServletRequest req, HttpServletResponse res)
		{
			return GetApps(req, res, null, Collections.EmptySet<string>(), null, null, null, 
				null, null, null, null, null, Collections.EmptySet<string>());
		}

		[GET]
		public override AppsInfo GetApps(HttpServletRequest req, HttpServletResponse res, 
			string stateQuery, ICollection<string> statesQuery, string finalStatusQuery, string
			 userQuery, string queueQuery, string count, string startedBegin, string startedEnd
			, string finishBegin, string finishEnd, ICollection<string> applicationTypes)
		{
			Init(res);
			ValidateStates(stateQuery, statesQuery);
			return base.GetApps(req, res, stateQuery, statesQuery, finalStatusQuery, userQuery
				, queueQuery, count, startedBegin, startedEnd, finishBegin, finishEnd, applicationTypes
				);
		}

		[GET]
		public override AppInfo GetApp(HttpServletRequest req, HttpServletResponse res, string
			 appId)
		{
			Init(res);
			return base.GetApp(req, res, appId);
		}

		[GET]
		public override AppAttemptsInfo GetAppAttempts(HttpServletRequest req, HttpServletResponse
			 res, string appId)
		{
			Init(res);
			return base.GetAppAttempts(req, res, appId);
		}

		[GET]
		public override AppAttemptInfo GetAppAttempt(HttpServletRequest req, HttpServletResponse
			 res, string appId, string appAttemptId)
		{
			Init(res);
			return base.GetAppAttempt(req, res, appId, appAttemptId);
		}

		[GET]
		public override ContainersInfo GetContainers(HttpServletRequest req, HttpServletResponse
			 res, string appId, string appAttemptId)
		{
			Init(res);
			return base.GetContainers(req, res, appId, appAttemptId);
		}

		[GET]
		public override ContainerInfo GetContainer(HttpServletRequest req, HttpServletResponse
			 res, string appId, string appAttemptId, string containerId)
		{
			Init(res);
			return base.GetContainer(req, res, appId, appAttemptId, containerId);
		}

		private static void ValidateStates(string stateQuery, ICollection<string> statesQuery
			)
		{
			// stateQuery is deprecated.
			if (stateQuery != null && !stateQuery.IsEmpty())
			{
				statesQuery.AddItem(stateQuery);
			}
			ICollection<string> appStates = ParseQueries(statesQuery, true);
			foreach (string appState in appStates)
			{
				switch (YarnApplicationState.ValueOf(StringUtils.ToUpperCase(appState)))
				{
					case YarnApplicationState.Finished:
					case YarnApplicationState.Failed:
					case YarnApplicationState.Killed:
					{
						continue;
					}

					default:
					{
						throw new BadRequestException("Invalid application-state " + appState + " specified. It should be a final state"
							);
					}
				}
			}
		}
	}
}
