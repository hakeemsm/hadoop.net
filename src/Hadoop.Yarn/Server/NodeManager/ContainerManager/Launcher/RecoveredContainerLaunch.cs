using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher
{
	/// <summary>
	/// This is a ContainerLaunch which has been recovered after an NM restart (for
	/// rolling upgrades)
	/// </summary>
	public class RecoveredContainerLaunch : ContainerLaunch
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher.RecoveredContainerLaunch
			));

		public RecoveredContainerLaunch(Context context, Configuration configuration, Dispatcher
			 dispatcher, ContainerExecutor exec, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 app, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container, LocalDirsHandlerService dirsHandler, ContainerManagerImpl containerManager
			)
			: base(context, configuration, dispatcher, exec, app, container, dirsHandler, containerManager
				)
		{
			this.shouldLaunchContainer.Set(true);
		}

		/// <summary>Wait on the process specified in pid file and return its exit code</summary>
		public override int Call()
		{
			int retCode = ContainerExecutor.ExitCode.Lost.GetExitCode();
			ContainerId containerId = container.GetContainerId();
			string appIdStr = ConverterUtils.ToString(containerId.GetApplicationAttemptId().GetApplicationId
				());
			string containerIdStr = ConverterUtils.ToString(containerId);
			dispatcher.GetEventHandler().Handle(new ContainerEvent(containerId, ContainerEventType
				.ContainerLaunched));
			bool notInterrupted = true;
			try
			{
				FilePath pidFile = LocatePidFile(appIdStr, containerIdStr);
				if (pidFile != null)
				{
					string pidPathStr = pidFile.GetPath();
					pidFilePath = new Path(pidPathStr);
					exec.ActivateContainer(containerId, pidFilePath);
					retCode = exec.ReacquireContainer(container.GetUser(), containerId);
				}
				else
				{
					Log.Warn("Unable to locate pid file for container " + containerIdStr);
				}
			}
			catch (IOException e)
			{
				Log.Error("Unable to recover container " + containerIdStr, e);
			}
			catch (Exception)
			{
				Log.Warn("Interrupted while waiting for exit code from " + containerId);
				notInterrupted = false;
			}
			finally
			{
				if (notInterrupted)
				{
					this.completed.Set(true);
					exec.DeactivateContainer(containerId);
					try
					{
						GetContext().GetNMStateStore().StoreContainerCompleted(containerId, retCode);
					}
					catch (IOException)
					{
						Log.Error("Unable to set exit code for container " + containerId);
					}
				}
			}
			if (retCode != 0)
			{
				Log.Warn("Recovered container exited with a non-zero exit code " + retCode);
				this.dispatcher.GetEventHandler().Handle(new ContainerExitEvent(containerId, ContainerEventType
					.ContainerExitedWithFailure, retCode, "Container exited with a non-zero exit code "
					 + retCode));
				return retCode;
			}
			Log.Info("Recovered container " + containerId + " succeeded");
			dispatcher.GetEventHandler().Handle(new ContainerEvent(containerId, ContainerEventType
				.ContainerExitedWithSuccess));
			return 0;
		}

		private FilePath LocatePidFile(string appIdStr, string containerIdStr)
		{
			string pidSubpath = GetPidFileSubpath(appIdStr, containerIdStr);
			foreach (string dir in GetContext().GetLocalDirsHandler().GetLocalDirsForRead())
			{
				FilePath pidFile = new FilePath(dir, pidSubpath);
				if (pidFile.Exists())
				{
					return pidFile;
				}
			}
			return null;
		}
	}
}
