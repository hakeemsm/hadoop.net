using System;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet.Http;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class NMWebServices
	{
		private Context nmContext;

		private ResourceView rview;

		private WebApp webapp;

		private static RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		[Context]
		private HttpServletRequest request;

		[Context]
		private HttpServletResponse response;

		[Context]
		internal UriInfo uriInfo;

		[Com.Google.Inject.Inject]
		public NMWebServices(Context nm, ResourceView view, WebApp webapp)
		{
			this.nmContext = nm;
			this.rview = view;
			this.webapp = webapp;
		}

		private void Init()
		{
			//clear content type
			response.SetContentType(null);
		}

		[GET]
		public virtual NodeInfo Get()
		{
			return GetNodeInfo();
		}

		[GET]
		public virtual NodeInfo GetNodeInfo()
		{
			Init();
			return new NodeInfo(this.nmContext, this.rview);
		}

		[GET]
		public virtual AppsInfo GetNodeApps(string stateQuery, string userQuery)
		{
			Init();
			AppsInfo allApps = new AppsInfo();
			foreach (KeyValuePair<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				> entry in this.nmContext.GetApplications())
			{
				AppInfo appInfo = new AppInfo(entry.Value);
				if (stateQuery != null && !stateQuery.IsEmpty())
				{
					ApplicationState.ValueOf(stateQuery);
					if (!Sharpen.Runtime.EqualsIgnoreCase(appInfo.GetState(), stateQuery))
					{
						continue;
					}
				}
				if (userQuery != null)
				{
					if (userQuery.IsEmpty())
					{
						string msg = "Error: You must specify a non-empty string for the user";
						throw new BadRequestException(msg);
					}
					if (!appInfo.GetUser().ToString().Equals(userQuery))
					{
						continue;
					}
				}
				allApps.Add(appInfo);
			}
			return allApps;
		}

		[GET]
		public virtual AppInfo GetNodeApp(string appId)
		{
			Init();
			ApplicationId id = ConverterUtils.ToApplicationId(recordFactory, appId);
			if (id == null)
			{
				throw new NotFoundException("app with id " + appId + " not found");
			}
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = this.nmContext.GetApplications()[id];
			if (app == null)
			{
				throw new NotFoundException("app with id " + appId + " not found");
			}
			return new AppInfo(app);
		}

		[GET]
		public virtual ContainersInfo GetNodeContainers()
		{
			Init();
			ContainersInfo allContainers = new ContainersInfo();
			foreach (KeyValuePair<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				> entry in this.nmContext.GetContainers())
			{
				if (entry.Value == null)
				{
					// just skip it
					continue;
				}
				ContainerInfo info = new ContainerInfo(this.nmContext, entry.Value, uriInfo.GetBaseUri
					().ToString(), webapp.Name());
				allContainers.Add(info);
			}
			return allContainers;
		}

		[GET]
		public virtual ContainerInfo GetNodeContainer(string id)
		{
			ContainerId containerId = null;
			Init();
			try
			{
				containerId = ConverterUtils.ToContainerId(id);
			}
			catch (Exception)
			{
				throw new BadRequestException("invalid container id, " + id);
			}
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = nmContext.GetContainers()[containerId];
			if (container == null)
			{
				throw new NotFoundException("container with id, " + id + ", not found");
			}
			return new ContainerInfo(this.nmContext, container, uriInfo.GetBaseUri().ToString
				(), webapp.Name());
		}

		/// <summary>Returns the contents of a container's log file in plain text.</summary>
		/// <remarks>
		/// Returns the contents of a container's log file in plain text.
		/// Only works for containers that are still in the NodeManager's memory, so
		/// logs are no longer available after the corresponding application is no
		/// longer running.
		/// </remarks>
		/// <param name="containerIdStr">The container ID</param>
		/// <param name="filename">The name of the log file</param>
		/// <returns>The contents of the container's log file</returns>
		[GET]
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual Response GetLogs(string containerIdStr, string filename)
		{
			ContainerId containerId;
			try
			{
				containerId = ConverterUtils.ToContainerId(containerIdStr);
			}
			catch (ArgumentException)
			{
				return Response.Status(Response.Status.BadRequest).Build();
			}
			FilePath logFile = null;
			try
			{
				logFile = ContainerLogsUtils.GetContainerLogFile(containerId, filename, request.GetRemoteUser
					(), nmContext);
			}
			catch (NotFoundException ex)
			{
				return Response.Status(Response.Status.NotFound).Entity(ex.Message).Build();
			}
			catch (YarnException ex)
			{
				return Response.ServerError().Entity(ex.Message).Build();
			}
			try
			{
				FileInputStream fis = ContainerLogsUtils.OpenLogFileForRead(containerIdStr, logFile
					, nmContext);
				StreamingOutput stream = new _StreamingOutput_240(fis);
				return Response.Ok(stream).Build();
			}
			catch (IOException ex)
			{
				return Response.ServerError().Entity(ex.Message).Build();
			}
		}

		private sealed class _StreamingOutput_240 : StreamingOutput
		{
			public _StreamingOutput_240(FileInputStream fis)
			{
				this.fis = fis;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.WS.RS.WebApplicationException"/>
			public void Write(OutputStream os)
			{
				int bufferSize = 65536;
				byte[] buf = new byte[bufferSize];
				int len;
				while ((len = fis.Read(buf, 0, bufferSize)) > 0)
				{
					os.Write(buf, 0, len);
				}
				os.Flush();
			}

			private readonly FileInputStream fis;
		}
	}
}
