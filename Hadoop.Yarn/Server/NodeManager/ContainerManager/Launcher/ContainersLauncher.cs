using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher
{
	/// <summary>The launcher for the containers.</summary>
	/// <remarks>
	/// The launcher for the containers. This service should be started only after
	/// the
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.ResourceLocalizationService
	/// 	"/>
	/// is started as it depends on creation
	/// of system directories on the local file-system.
	/// </remarks>
	public class ContainersLauncher : AbstractService, EventHandler<ContainersLauncherEvent
		>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher.ContainersLauncher
			));

		private readonly Context context;

		private readonly ContainerExecutor exec;

		private readonly Dispatcher dispatcher;

		private readonly ContainerManagerImpl containerManager;

		private LocalDirsHandlerService dirsHandler;

		[VisibleForTesting]
		public ExecutorService containerLauncher = Executors.NewCachedThreadPool(new ThreadFactoryBuilder
			().SetNameFormat("ContainersLauncher #%d").Build());

		[VisibleForTesting]
		public readonly IDictionary<ContainerId, ContainerLaunch> running = Sharpen.Collections
			.SynchronizedMap(new Dictionary<ContainerId, ContainerLaunch>());

		public ContainersLauncher(Context context, Dispatcher dispatcher, ContainerExecutor
			 exec, LocalDirsHandlerService dirsHandler, ContainerManagerImpl containerManager
			)
			: base("containers-launcher")
		{
			this.exec = exec;
			this.context = context;
			this.dispatcher = dispatcher;
			this.dirsHandler = dirsHandler;
			this.containerManager = containerManager;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			try
			{
				//TODO Is this required?
				FileContext.GetLocalFSFileContext(conf);
			}
			catch (UnsupportedFileSystemException e)
			{
				throw new YarnRuntimeException("Failed to start ContainersLauncher", e);
			}
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			containerLauncher.ShutdownNow();
			base.ServiceStop();
		}

		public virtual void Handle(ContainersLauncherEvent @event)
		{
			// TODO: ContainersLauncher launches containers one by one!!
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = @event.GetContainer();
			ContainerId containerId = container.GetContainerId();
			switch (@event.GetType())
			{
				case ContainersLauncherEventType.LaunchContainer:
				{
					Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
						 app = context.GetApplications()[containerId.GetApplicationAttemptId().GetApplicationId
						()];
					ContainerLaunch launch = new ContainerLaunch(context, GetConfig(), dispatcher, exec
						, app, @event.GetContainer(), dirsHandler, containerManager);
					containerLauncher.Submit(launch);
					running[containerId] = launch;
					break;
				}

				case ContainersLauncherEventType.RecoverContainer:
				{
					app = context.GetApplications()[containerId.GetApplicationAttemptId().GetApplicationId
						()];
					launch = new RecoveredContainerLaunch(context, GetConfig(), dispatcher, exec, app
						, @event.GetContainer(), dirsHandler, containerManager);
					containerLauncher.Submit(launch);
					running[containerId] = launch;
					break;
				}

				case ContainersLauncherEventType.CleanupContainer:
				{
					ContainerLaunch launcher = Sharpen.Collections.Remove(running, containerId);
					if (launcher == null)
					{
						// Container not launched. So nothing needs to be done.
						return;
					}
					// Cleanup a container whether it is running/killed/completed, so that
					// no sub-processes are alive.
					try
					{
						launcher.CleanupContainer();
					}
					catch (IOException)
					{
						Log.Warn("Got exception while cleaning container " + containerId + ". Ignoring.");
					}
					break;
				}
			}
		}
	}
}
