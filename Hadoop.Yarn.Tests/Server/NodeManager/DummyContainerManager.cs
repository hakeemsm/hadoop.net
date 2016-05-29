using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class DummyContainerManager : ContainerManagerImpl
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.DummyContainerManager
			));

		public DummyContainerManager(Context context, ContainerExecutor exec, DeletionService
			 deletionContext, NodeStatusUpdater nodeStatusUpdater, NodeManagerMetrics metrics
			, ApplicationACLsManager applicationACLsManager, LocalDirsHandlerService dirsHandler
			)
			: base(context, exec, deletionContext, nodeStatusUpdater, metrics, applicationACLsManager
				, dirsHandler)
		{
		}

		protected internal override ResourceLocalizationService CreateResourceLocalizationService
			(ContainerExecutor exec, DeletionService deletionContext, Context context)
		{
			return new _ResourceLocalizationService_78(base.dispatcher, exec, deletionContext
				, base.dirsHandler, context);
		}

		private sealed class _ResourceLocalizationService_78 : ResourceLocalizationService
		{
			public _ResourceLocalizationService_78(Dispatcher baseArg1, ContainerExecutor baseArg2
				, DeletionService baseArg3, LocalDirsHandlerService baseArg4, Context baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
			}

			public override void Handle(LocalizationEvent @event)
			{
				switch (@event.GetType())
				{
					case LocalizationEventType.InitApplicationResources:
					{
						Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
							 app = ((ApplicationLocalizationEvent)@event).GetApplication();
						// Simulate event from ApplicationLocalization.
						this.dispatcher.GetEventHandler().Handle(new ApplicationInitedEvent(app.GetAppId(
							)));
						break;
					}

					case LocalizationEventType.InitContainerResources:
					{
						ContainerLocalizationRequestEvent rsrcReqs = (ContainerLocalizationRequestEvent)@event;
						// simulate localization of all requested resources
						foreach (ICollection<LocalResourceRequest> rc in rsrcReqs.GetRequestedResources()
							.Values)
						{
							foreach (LocalResourceRequest req in rc)
							{
								Org.Apache.Hadoop.Yarn.Server.Nodemanager.DummyContainerManager.Log.Info("DEBUG: "
									 + req + ":" + rsrcReqs.GetContainer().GetContainerId());
								this.dispatcher.GetEventHandler().Handle(new ContainerResourceLocalizedEvent(rsrcReqs
									.GetContainer().GetContainerId(), req, new Path("file:///local" + req.GetPath().
									ToUri().GetPath())));
							}
						}
						break;
					}

					case LocalizationEventType.CleanupContainerResources:
					{
						Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
							 = ((ContainerLocalizationEvent)@event).GetContainer();
						// TODO: delete the container dir
						this.dispatcher.GetEventHandler().Handle(new ContainerEvent(container.GetContainerId
							(), ContainerEventType.ContainerResourcesCleanedup));
						break;
					}

					case LocalizationEventType.DestroyApplicationResources:
					{
						Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
							 application = ((ApplicationLocalizationEvent)@event).GetApplication();
						// decrement reference counts of all resources associated with this
						// app
						this.dispatcher.GetEventHandler().Handle(new ApplicationEvent(application.GetAppId
							(), ApplicationEventType.ApplicationResourcesCleanedup));
						break;
					}

					default:
					{
						NUnit.Framework.Assert.Fail("Unexpected event: " + @event.GetType());
						break;
					}
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		protected internal override UserGroupInformation GetRemoteUgi()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 0);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(appAttemptId.ToString
				());
			ugi.AddTokenIdentifier(new NMTokenIdentifier(appAttemptId, GetContext().GetNodeId
				(), "testuser", GetContext().GetNMTokenSecretManager().GetCurrentKey().GetKeyId(
				)));
			return ugi;
		}

		protected internal override ContainersLauncher CreateContainersLauncher(Context context
			, ContainerExecutor exec)
		{
			return new _ContainersLauncher_148(this, context, base.dispatcher, exec, base.dirsHandler
				, this);
		}

		private sealed class _ContainersLauncher_148 : ContainersLauncher
		{
			public _ContainersLauncher_148(DummyContainerManager _enclosing, Context baseArg1
				, Dispatcher baseArg2, ContainerExecutor baseArg3, LocalDirsHandlerService baseArg4
				, ContainerManagerImpl baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
				this._enclosing = _enclosing;
			}

			public override void Handle(ContainersLauncherEvent @event)
			{
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
					 = @event.GetContainer();
				ContainerId containerId = container.GetContainerId();
				switch (@event.GetType())
				{
					case ContainersLauncherEventType.LaunchContainer:
					{
						this._enclosing.dispatcher.GetEventHandler().Handle(new ContainerEvent(containerId
							, ContainerEventType.ContainerLaunched));
						break;
					}

					case ContainersLauncherEventType.CleanupContainer:
					{
						this._enclosing.dispatcher.GetEventHandler().Handle(new ContainerExitEvent(containerId
							, ContainerEventType.ContainerKilledOnRequest, 0, "Container exited with exit code 0."
							));
						break;
					}
				}
			}

			private readonly DummyContainerManager _enclosing;
		}

		protected internal override LogHandler CreateLogHandler(Configuration conf, Context
			 context, DeletionService deletionService)
		{
			return new _LogHandler_173();
		}

		private sealed class _LogHandler_173 : LogHandler
		{
			public _LogHandler_173()
			{
			}

			public void Handle(LogHandlerEvent @event)
			{
				switch (@event.GetType())
				{
					case LogHandlerEventType.ApplicationStarted:
					{
						break;
					}

					case LogHandlerEventType.ContainerFinished:
					{
						break;
					}

					case LogHandlerEventType.ApplicationFinished:
					{
						break;
					}

					default:
					{
						break;
					}
				}
			}
		}

		// Ignore
		public override void SetBlockNewContainerRequests(bool blockNewContainerRequests)
		{
		}

		// do nothing
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		protected internal override void AuthorizeStartRequest(NMTokenIdentifier nmTokenIdentifier
			, ContainerTokenIdentifier containerTokenIdentifier)
		{
		}

		// do nothing
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		protected internal override void AuthorizeGetAndStopContainerRequest(ContainerId 
			containerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container, bool stopRequest, NMTokenIdentifier identifier)
		{
		}
		// do nothing
	}
}
