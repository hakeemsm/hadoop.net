using System;
using System.Collections.Generic;
using Javax.Servlet.Http;
using Javax.WS.RS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Server.Webapp
{
	public class WebServices
	{
		protected internal ApplicationBaseProtocol appBaseProt;

		public WebServices(ApplicationBaseProtocol appBaseProt)
		{
			this.appBaseProt = appBaseProt;
		}

		public virtual AppsInfo GetApps(HttpServletRequest req, HttpServletResponse res, 
			string stateQuery, ICollection<string> statesQuery, string finalStatusQuery, string
			 userQuery, string queueQuery, string count, string startedBegin, string startedEnd
			, string finishBegin, string finishEnd, ICollection<string> applicationTypes)
		{
			UserGroupInformation callerUGI = GetUser(req);
			bool checkStart = false;
			bool checkEnd = false;
			bool checkAppTypes = false;
			bool checkAppStates = false;
			long countNum = long.MaxValue;
			// set values suitable in case both of begin/end not specified
			long sBegin = 0;
			long sEnd = long.MaxValue;
			long fBegin = 0;
			long fEnd = long.MaxValue;
			if (count != null && !count.IsEmpty())
			{
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
			AppsInfo allApps = new AppsInfo();
			ICollection<ApplicationReport> appReports = null;
			GetApplicationsRequest request = GetApplicationsRequest.NewInstance();
			request.SetLimit(countNum);
			try
			{
				if (callerUGI == null)
				{
					// TODO: the request should take the params like what RMWebServices does
					// in YARN-1819.
					appReports = appBaseProt.GetApplications(request).GetApplicationList();
				}
				else
				{
					appReports = callerUGI.DoAs(new _PrivilegedExceptionAction_161(this, request));
				}
			}
			catch (Exception e)
			{
				RewrapAndThrowException(e);
			}
			foreach (ApplicationReport appReport in appReports)
			{
				if (checkAppStates && !appStates.Contains(StringUtils.ToLowerCase(appReport.GetYarnApplicationState
					().ToString())))
				{
					continue;
				}
				if (finalStatusQuery != null && !finalStatusQuery.IsEmpty())
				{
					FinalApplicationStatus.ValueOf(finalStatusQuery);
					if (!Sharpen.Runtime.EqualsIgnoreCase(appReport.GetFinalApplicationStatus().ToString
						(), finalStatusQuery))
					{
						continue;
					}
				}
				if (userQuery != null && !userQuery.IsEmpty())
				{
					if (!appReport.GetUser().Equals(userQuery))
					{
						continue;
					}
				}
				if (queueQuery != null && !queueQuery.IsEmpty())
				{
					if (!appReport.GetQueue().Equals(queueQuery))
					{
						continue;
					}
				}
				if (checkAppTypes && !appTypes.Contains(StringUtils.ToLowerCase(appReport.GetApplicationType
					().Trim())))
				{
					continue;
				}
				if (checkStart && (appReport.GetStartTime() < sBegin || appReport.GetStartTime() 
					> sEnd))
				{
					continue;
				}
				if (checkEnd && (appReport.GetFinishTime() < fBegin || appReport.GetFinishTime() 
					> fEnd))
				{
					continue;
				}
				AppInfo app = new AppInfo(appReport);
				allApps.Add(app);
			}
			return allApps;
		}

		private sealed class _PrivilegedExceptionAction_161 : PrivilegedExceptionAction<ICollection
			<ApplicationReport>>
		{
			public _PrivilegedExceptionAction_161(WebServices _enclosing, GetApplicationsRequest
				 request)
			{
				this._enclosing = _enclosing;
				this.request = request;
			}

			/// <exception cref="System.Exception"/>
			public ICollection<ApplicationReport> Run()
			{
				return this._enclosing.appBaseProt.GetApplications(request).GetApplicationList();
			}

			private readonly WebServices _enclosing;

			private readonly GetApplicationsRequest request;
		}

		public virtual AppInfo GetApp(HttpServletRequest req, HttpServletResponse res, string
			 appId)
		{
			UserGroupInformation callerUGI = GetUser(req);
			ApplicationId id = ParseApplicationId(appId);
			ApplicationReport app = null;
			try
			{
				if (callerUGI == null)
				{
					GetApplicationReportRequest request = GetApplicationReportRequest.NewInstance(id);
					app = appBaseProt.GetApplicationReport(request).GetApplicationReport();
				}
				else
				{
					app = callerUGI.DoAs(new _PrivilegedExceptionAction_228(this, id));
				}
			}
			catch (Exception e)
			{
				RewrapAndThrowException(e);
			}
			if (app == null)
			{
				throw new NotFoundException("app with id: " + appId + " not found");
			}
			return new AppInfo(app);
		}

		private sealed class _PrivilegedExceptionAction_228 : PrivilegedExceptionAction<ApplicationReport
			>
		{
			public _PrivilegedExceptionAction_228(WebServices _enclosing, ApplicationId id)
			{
				this._enclosing = _enclosing;
				this.id = id;
			}

			/// <exception cref="System.Exception"/>
			public ApplicationReport Run()
			{
				GetApplicationReportRequest request = GetApplicationReportRequest.NewInstance(id);
				return this._enclosing.appBaseProt.GetApplicationReport(request).GetApplicationReport
					();
			}

			private readonly WebServices _enclosing;

			private readonly ApplicationId id;
		}

		public virtual AppAttemptsInfo GetAppAttempts(HttpServletRequest req, HttpServletResponse
			 res, string appId)
		{
			UserGroupInformation callerUGI = GetUser(req);
			ApplicationId id = ParseApplicationId(appId);
			ICollection<ApplicationAttemptReport> appAttemptReports = null;
			try
			{
				if (callerUGI == null)
				{
					GetApplicationAttemptsRequest request = GetApplicationAttemptsRequest.NewInstance
						(id);
					appAttemptReports = appBaseProt.GetApplicationAttempts(request).GetApplicationAttemptList
						();
				}
				else
				{
					appAttemptReports = callerUGI.DoAs(new _PrivilegedExceptionAction_260(this, id));
				}
			}
			catch (Exception e)
			{
				RewrapAndThrowException(e);
			}
			AppAttemptsInfo appAttemptsInfo = new AppAttemptsInfo();
			foreach (ApplicationAttemptReport appAttemptReport in appAttemptReports)
			{
				AppAttemptInfo appAttemptInfo = new AppAttemptInfo(appAttemptReport);
				appAttemptsInfo.Add(appAttemptInfo);
			}
			return appAttemptsInfo;
		}

		private sealed class _PrivilegedExceptionAction_260 : PrivilegedExceptionAction<ICollection
			<ApplicationAttemptReport>>
		{
			public _PrivilegedExceptionAction_260(WebServices _enclosing, ApplicationId id)
			{
				this._enclosing = _enclosing;
				this.id = id;
			}

			/// <exception cref="System.Exception"/>
			public ICollection<ApplicationAttemptReport> Run()
			{
				GetApplicationAttemptsRequest request = GetApplicationAttemptsRequest.NewInstance
					(id);
				return this._enclosing.appBaseProt.GetApplicationAttempts(request).GetApplicationAttemptList
					();
			}

			private readonly WebServices _enclosing;

			private readonly ApplicationId id;
		}

		public virtual AppAttemptInfo GetAppAttempt(HttpServletRequest req, HttpServletResponse
			 res, string appId, string appAttemptId)
		{
			UserGroupInformation callerUGI = GetUser(req);
			ApplicationId aid = ParseApplicationId(appId);
			ApplicationAttemptId aaid = ParseApplicationAttemptId(appAttemptId);
			ValidateIds(aid, aaid, null);
			ApplicationAttemptReport appAttempt = null;
			try
			{
				if (callerUGI == null)
				{
					GetApplicationAttemptReportRequest request = GetApplicationAttemptReportRequest.NewInstance
						(aaid);
					appAttempt = appBaseProt.GetApplicationAttemptReport(request).GetApplicationAttemptReport
						();
				}
				else
				{
					appAttempt = callerUGI.DoAs(new _PrivilegedExceptionAction_298(this, aaid));
				}
			}
			catch (Exception e)
			{
				RewrapAndThrowException(e);
			}
			if (appAttempt == null)
			{
				throw new NotFoundException("app attempt with id: " + appAttemptId + " not found"
					);
			}
			return new AppAttemptInfo(appAttempt);
		}

		private sealed class _PrivilegedExceptionAction_298 : PrivilegedExceptionAction<ApplicationAttemptReport
			>
		{
			public _PrivilegedExceptionAction_298(WebServices _enclosing, ApplicationAttemptId
				 aaid)
			{
				this._enclosing = _enclosing;
				this.aaid = aaid;
			}

			/// <exception cref="System.Exception"/>
			public ApplicationAttemptReport Run()
			{
				GetApplicationAttemptReportRequest request = GetApplicationAttemptReportRequest.NewInstance
					(aaid);
				return this._enclosing.appBaseProt.GetApplicationAttemptReport(request).GetApplicationAttemptReport
					();
			}

			private readonly WebServices _enclosing;

			private readonly ApplicationAttemptId aaid;
		}

		public virtual ContainersInfo GetContainers(HttpServletRequest req, HttpServletResponse
			 res, string appId, string appAttemptId)
		{
			UserGroupInformation callerUGI = GetUser(req);
			ApplicationId aid = ParseApplicationId(appId);
			ApplicationAttemptId aaid = ParseApplicationAttemptId(appAttemptId);
			ValidateIds(aid, aaid, null);
			ICollection<ContainerReport> containerReports = null;
			try
			{
				if (callerUGI == null)
				{
					GetContainersRequest request = GetContainersRequest.NewInstance(aaid);
					containerReports = appBaseProt.GetContainers(request).GetContainerList();
				}
				else
				{
					containerReports = callerUGI.DoAs(new _PrivilegedExceptionAction_332(this, aaid));
				}
			}
			catch (Exception e)
			{
				RewrapAndThrowException(e);
			}
			ContainersInfo containersInfo = new ContainersInfo();
			foreach (ContainerReport containerReport in containerReports)
			{
				ContainerInfo containerInfo = new ContainerInfo(containerReport);
				containersInfo.Add(containerInfo);
			}
			return containersInfo;
		}

		private sealed class _PrivilegedExceptionAction_332 : PrivilegedExceptionAction<ICollection
			<ContainerReport>>
		{
			public _PrivilegedExceptionAction_332(WebServices _enclosing, ApplicationAttemptId
				 aaid)
			{
				this._enclosing = _enclosing;
				this.aaid = aaid;
			}

			/// <exception cref="System.Exception"/>
			public ICollection<ContainerReport> Run()
			{
				GetContainersRequest request = GetContainersRequest.NewInstance(aaid);
				return this._enclosing.appBaseProt.GetContainers(request).GetContainerList();
			}

			private readonly WebServices _enclosing;

			private readonly ApplicationAttemptId aaid;
		}

		public virtual ContainerInfo GetContainer(HttpServletRequest req, HttpServletResponse
			 res, string appId, string appAttemptId, string containerId)
		{
			UserGroupInformation callerUGI = GetUser(req);
			ApplicationId aid = ParseApplicationId(appId);
			ApplicationAttemptId aaid = ParseApplicationAttemptId(appAttemptId);
			ContainerId cid = ParseContainerId(containerId);
			ValidateIds(aid, aaid, cid);
			ContainerReport container = null;
			try
			{
				if (callerUGI == null)
				{
					GetContainerReportRequest request = GetContainerReportRequest.NewInstance(cid);
					container = appBaseProt.GetContainerReport(request).GetContainerReport();
				}
				else
				{
					container = callerUGI.DoAs(new _PrivilegedExceptionAction_368(this, cid));
				}
			}
			catch (Exception e)
			{
				RewrapAndThrowException(e);
			}
			if (container == null)
			{
				throw new NotFoundException("container with id: " + containerId + " not found");
			}
			return new ContainerInfo(container);
		}

		private sealed class _PrivilegedExceptionAction_368 : PrivilegedExceptionAction<ContainerReport
			>
		{
			public _PrivilegedExceptionAction_368(WebServices _enclosing, ContainerId cid)
			{
				this._enclosing = _enclosing;
				this.cid = cid;
			}

			/// <exception cref="System.Exception"/>
			public ContainerReport Run()
			{
				GetContainerReportRequest request = GetContainerReportRequest.NewInstance(cid);
				return this._enclosing.appBaseProt.GetContainerReport(request).GetContainerReport
					();
			}

			private readonly WebServices _enclosing;

			private readonly ContainerId cid;
		}

		protected internal virtual void Init(HttpServletResponse response)
		{
			// clear content type
			response.SetContentType(null);
		}

		protected internal static ICollection<string> ParseQueries(ICollection<string> queries
			, bool isState)
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

		protected internal static ApplicationId ParseApplicationId(string appId)
		{
			if (appId == null || appId.IsEmpty())
			{
				throw new NotFoundException("appId, " + appId + ", is empty or null");
			}
			ApplicationId aid = ConverterUtils.ToApplicationId(appId);
			if (aid == null)
			{
				throw new NotFoundException("appId is null");
			}
			return aid;
		}

		protected internal static ApplicationAttemptId ParseApplicationAttemptId(string appAttemptId
			)
		{
			if (appAttemptId == null || appAttemptId.IsEmpty())
			{
				throw new NotFoundException("appAttemptId, " + appAttemptId + ", is empty or null"
					);
			}
			ApplicationAttemptId aaid = ConverterUtils.ToApplicationAttemptId(appAttemptId);
			if (aaid == null)
			{
				throw new NotFoundException("appAttemptId is null");
			}
			return aaid;
		}

		protected internal static ContainerId ParseContainerId(string containerId)
		{
			if (containerId == null || containerId.IsEmpty())
			{
				throw new NotFoundException("containerId, " + containerId + ", is empty or null");
			}
			ContainerId cid = ConverterUtils.ToContainerId(containerId);
			if (cid == null)
			{
				throw new NotFoundException("containerId is null");
			}
			return cid;
		}

		protected internal virtual void ValidateIds(ApplicationId appId, ApplicationAttemptId
			 appAttemptId, ContainerId containerId)
		{
			if (!appAttemptId.GetApplicationId().Equals(appId))
			{
				throw new NotFoundException("appId and appAttemptId don't match");
			}
			if (containerId != null && !containerId.GetApplicationAttemptId().Equals(appAttemptId
				))
			{
				throw new NotFoundException("appAttemptId and containerId don't match");
			}
		}

		protected internal static UserGroupInformation GetUser(HttpServletRequest req)
		{
			string remoteUser = req.GetRemoteUser();
			UserGroupInformation callerUGI = null;
			if (remoteUser != null)
			{
				callerUGI = UserGroupInformation.CreateRemoteUser(remoteUser);
			}
			return callerUGI;
		}

		private static void RewrapAndThrowException(Exception e)
		{
			if (e is UndeclaredThrowableException)
			{
				RewrapAndThrowThrowable(e.InnerException);
			}
			else
			{
				RewrapAndThrowThrowable(e);
			}
		}

		private static void RewrapAndThrowThrowable(Exception t)
		{
			if (t is AuthorizationException)
			{
				throw new ForbiddenException(t);
			}
			else
			{
				if (t is ApplicationNotFoundException || t is ApplicationAttemptNotFoundException
					 || t is ContainerNotFoundException)
				{
					throw new NotFoundException(t);
				}
				else
				{
					throw new WebApplicationException(t);
				}
			}
		}
	}
}
