using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager
{
	public class TestNMProxy : BaseContainerManagerTest
	{
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		public TestNMProxy()
			: base()
		{
		}

		internal int retryCount = 0;

		internal bool shouldThrowNMNotYetReadyException = false;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf.SetLong(YarnConfiguration.ClientNmConnectMaxWaitMs, 10000);
			conf.SetLong(YarnConfiguration.ClientNmConnectRetryIntervalMs, 100);
		}

		protected internal override ContainerManagerImpl CreateContainerManager(DeletionService
			 delSrvc)
		{
			return new _ContainerManagerImpl_69(this, context, exec, delSrvc, nodeStatusUpdater
				, metrics, new ApplicationACLsManager(conf), dirsHandler);
		}

		private sealed class _ContainerManagerImpl_69 : ContainerManagerImpl
		{
			public _ContainerManagerImpl_69(TestNMProxy _enclosing, Context baseArg1, ContainerExecutor
				 baseArg2, DeletionService baseArg3, NodeStatusUpdater baseArg4, NodeManagerMetrics
				 baseArg5, ApplicationACLsManager baseArg6, LocalDirsHandlerService baseArg7)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override StartContainersResponse StartContainers(StartContainersRequest requests
				)
			{
				if (this._enclosing.retryCount < 5)
				{
					this._enclosing.retryCount++;
					if (this._enclosing.shouldThrowNMNotYetReadyException)
					{
						// This causes super to throw an NMNotYetReadyException
						this._enclosing.containerManager.SetBlockNewContainerRequests(true);
					}
					else
					{
						throw new ConnectException("start container exception");
					}
				}
				else
				{
					// This stops super from throwing an NMNotYetReadyException
					this._enclosing.containerManager.SetBlockNewContainerRequests(false);
				}
				return base.StartContainers(requests);
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override StopContainersResponse StopContainers(StopContainersRequest requests
				)
			{
				if (this._enclosing.retryCount < 5)
				{
					this._enclosing.retryCount++;
					throw new ConnectException("stop container exception");
				}
				return base.StopContainers(requests);
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override GetContainerStatusesResponse GetContainerStatuses(GetContainerStatusesRequest
				 request)
			{
				if (this._enclosing.retryCount < 5)
				{
					this._enclosing.retryCount++;
					throw new ConnectException("get container status exception");
				}
				return base.GetContainerStatuses(request);
			}

			private readonly TestNMProxy _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNMProxyRetry()
		{
			containerManager.Start();
			containerManager.SetBlockNewContainerRequests(false);
			StartContainersRequest allRequests = Records.NewRecord<StartContainersRequest>();
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 1);
			Token nmToken = context.GetNMTokenSecretManager().CreateNMToken(attemptId, context
				.GetNodeId(), user);
			IPEndPoint address = conf.GetSocketAddr(YarnConfiguration.NmBindHost, YarnConfiguration
				.NmAddress, YarnConfiguration.DefaultNmAddress, YarnConfiguration.DefaultNmPort);
			Org.Apache.Hadoop.Security.Token.Token<NMTokenIdentifier> token = ConverterUtils.
				ConvertFromYarn(nmToken, SecurityUtil.BuildTokenService(address));
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(user);
			ugi.AddToken(token);
			ContainerManagementProtocol proxy = NMProxy.CreateNMProxy<ContainerManagementProtocol
				>(conf, ugi, YarnRPC.Create(conf), address);
			retryCount = 0;
			shouldThrowNMNotYetReadyException = false;
			proxy.StartContainers(allRequests);
			NUnit.Framework.Assert.AreEqual(5, retryCount);
			retryCount = 0;
			shouldThrowNMNotYetReadyException = false;
			proxy.StopContainers(Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<StopContainersRequest
				>());
			NUnit.Framework.Assert.AreEqual(5, retryCount);
			retryCount = 0;
			shouldThrowNMNotYetReadyException = false;
			proxy.GetContainerStatuses(Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetContainerStatusesRequest
				>());
			NUnit.Framework.Assert.AreEqual(5, retryCount);
			retryCount = 0;
			shouldThrowNMNotYetReadyException = true;
			proxy.StartContainers(allRequests);
			NUnit.Framework.Assert.AreEqual(5, retryCount);
		}
	}
}
