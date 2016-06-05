using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Amlauncher;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class MockRMWithCustomAMLauncher : MockRM
	{
		private readonly ContainerManagementProtocol containerManager;

		public MockRMWithCustomAMLauncher(ContainerManagementProtocol containerManager)
			: this(new Configuration(), containerManager)
		{
		}

		public MockRMWithCustomAMLauncher(Configuration conf, ContainerManagementProtocol
			 containerManager)
			: base(conf)
		{
			this.containerManager = containerManager;
		}

		protected internal override ApplicationMasterLauncher CreateAMLauncher()
		{
			return new _ApplicationMasterLauncher_51(this, GetRMContext());
		}

		private sealed class _ApplicationMasterLauncher_51 : ApplicationMasterLauncher
		{
			public _ApplicationMasterLauncher_51(MockRMWithCustomAMLauncher _enclosing, RMContext
				 baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
			}

			protected internal override Runnable CreateRunnableLauncher(RMAppAttempt application
				, AMLauncherEventType @event)
			{
				return new _AMLauncher_55(this, this.context, application, @event, this.GetConfig
					());
			}

			private sealed class _AMLauncher_55 : AMLauncher
			{
				public _AMLauncher_55(_ApplicationMasterLauncher_51 _enclosing, RMContext baseArg1
					, RMAppAttempt baseArg2, AMLauncherEventType baseArg3, Configuration baseArg4)
					: base(baseArg1, baseArg2, baseArg3, baseArg4)
				{
					this._enclosing = _enclosing;
				}

				protected internal override ContainerManagementProtocol GetContainerMgrProxy(ContainerId
					 containerId)
				{
					return this._enclosing._enclosing.containerManager;
				}

				protected internal override Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier
					> CreateAndSetAMRMToken()
				{
					Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amRmToken = base.CreateAndSetAMRMToken
						();
					IPEndPoint serviceAddr = this._enclosing.GetConfig().GetSocketAddr(YarnConfiguration
						.RmSchedulerAddress, YarnConfiguration.DefaultRmSchedulerAddress, YarnConfiguration
						.DefaultRmSchedulerPort);
					SecurityUtil.SetTokenService(amRmToken, serviceAddr);
					return amRmToken;
				}

				private readonly _ApplicationMasterLauncher_51 _enclosing;
			}

			private readonly MockRMWithCustomAMLauncher _enclosing;
		}
	}
}
