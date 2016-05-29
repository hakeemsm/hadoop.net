using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	/// <summary>Contains utilities for fetching a user's log file in a secure fashion.</summary>
	public class ContainerLogsUtils
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(ContainerLogsUtils
			));

		/// <summary>
		/// Finds the local directories that logs for the given container are stored
		/// on.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public static IList<FilePath> GetContainerLogDirs(ContainerId containerId, string
			 remoteUser, Context context)
		{
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = context.GetContainers()[containerId];
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 application = GetApplicationForContainer(containerId, context);
			CheckAccess(remoteUser, application, context);
			// It is not required to have null check for container ( container == null )
			// and throw back exception.Because when container is completed, NodeManager
			// remove container information from its NMContext.Configuring log
			// aggregation to false, container log view request is forwarded to NM. NM
			// does not have completed container information,but still NM serve request for
			// reading container logs. 
			if (container != null)
			{
				CheckState(container.GetContainerState());
			}
			return GetContainerLogDirs(containerId, context.GetLocalDirsHandler());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		internal static IList<FilePath> GetContainerLogDirs(ContainerId containerId, LocalDirsHandlerService
			 dirsHandler)
		{
			IList<string> logDirs = dirsHandler.GetLogDirsForRead();
			IList<FilePath> containerLogDirs = new AList<FilePath>(logDirs.Count);
			foreach (string logDir in logDirs)
			{
				logDir = new FilePath(logDir).ToURI().GetPath();
				string appIdStr = ConverterUtils.ToString(containerId.GetApplicationAttemptId().GetApplicationId
					());
				FilePath appLogDir = new FilePath(logDir, appIdStr);
				containerLogDirs.AddItem(new FilePath(appLogDir, containerId.ToString()));
			}
			return containerLogDirs;
		}

		/// <summary>Finds the log file with the given filename for the given container.</summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public static FilePath GetContainerLogFile(ContainerId containerId, string fileName
			, string remoteUser, Context context)
		{
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = context.GetContainers()[containerId];
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 application = GetApplicationForContainer(containerId, context);
			CheckAccess(remoteUser, application, context);
			if (container != null)
			{
				CheckState(container.GetContainerState());
			}
			try
			{
				LocalDirsHandlerService dirsHandler = context.GetLocalDirsHandler();
				string relativeContainerLogDir = ContainerLaunch.GetRelativeContainerLogDir(application
					.GetAppId().ToString(), containerId.ToString());
				Path logPath = dirsHandler.GetLogPathToRead(relativeContainerLogDir + Path.Separator
					 + fileName);
				URI logPathURI = new FilePath(logPath.ToString()).ToURI();
				FilePath logFile = new FilePath(logPathURI.GetPath());
				return logFile;
			}
			catch (IOException e)
			{
				Log.Warn("Failed to find log file", e);
				throw new NotFoundException("Cannot find this log on the local disk.");
			}
		}

		private static Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 GetApplicationForContainer(ContainerId containerId, Context context)
		{
			ApplicationId applicationId = containerId.GetApplicationAttemptId().GetApplicationId
				();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 application = context.GetApplications()[applicationId];
			if (application == null)
			{
				throw new NotFoundException("Unknown container. Container either has not started or "
					 + "has already completed or " + "doesn't belong to this node at all.");
			}
			return application;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private static void CheckAccess(string remoteUser, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 application, Context context)
		{
			UserGroupInformation callerUGI = null;
			if (remoteUser != null)
			{
				callerUGI = UserGroupInformation.CreateRemoteUser(remoteUser);
			}
			if (callerUGI != null && !context.GetApplicationACLsManager().CheckAccess(callerUGI
				, ApplicationAccessType.ViewApp, application.GetUser(), application.GetAppId()))
			{
				throw new YarnException("User [" + remoteUser + "] is not authorized to view the logs for application "
					 + application.GetAppId());
			}
		}

		private static void CheckState(ContainerState state)
		{
			if (state == ContainerState.New || state == ContainerState.Localizing || state ==
				 ContainerState.Localized)
			{
				throw new NotFoundException("Container is not yet running. Current state is " + state
					);
			}
			if (state == ContainerState.LocalizationFailed)
			{
				throw new NotFoundException("Container wasn't started. Localization failed.");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static FileInputStream OpenLogFileForRead(string containerIdStr, FilePath 
			logFile, Context context)
		{
			ContainerId containerId = ConverterUtils.ToContainerId(containerIdStr);
			ApplicationId applicationId = containerId.GetApplicationAttemptId().GetApplicationId
				();
			string user = context.GetApplications()[applicationId].GetUser();
			try
			{
				return SecureIOUtils.OpenForRead(logFile, user, null);
			}
			catch (IOException e)
			{
				if (e.Message.Contains("did not match expected owner '" + user + "'"))
				{
					Log.Error("Exception reading log file " + logFile.GetAbsolutePath(), e);
					throw new IOException("Exception reading log file. Application submitted by '" + 
						user + "' doesn't own requested log file : " + logFile.GetName(), e);
				}
				else
				{
					throw new IOException("Exception reading log file. It might be because log " + "file was aggregated : "
						 + logFile.GetName(), e);
				}
			}
		}
	}
}
