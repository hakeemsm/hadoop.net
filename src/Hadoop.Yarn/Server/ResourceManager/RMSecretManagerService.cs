using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class RMSecretManagerService : AbstractService
	{
		internal AMRMTokenSecretManager amRmTokenSecretManager;

		internal NMTokenSecretManagerInRM nmTokenSecretManager;

		internal ClientToAMTokenSecretManagerInRM clientToAMSecretManager;

		internal RMContainerTokenSecretManager containerTokenSecretManager;

		internal RMDelegationTokenSecretManager rmDTSecretManager;

		internal RMContextImpl rmContext;

		/// <summary>Construct the service.</summary>
		public RMSecretManagerService(Configuration conf, RMContextImpl rmContext)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.RMSecretManagerService
				).FullName)
		{
			this.rmContext = rmContext;
			// To initialize correctly, these managers should be created before
			// being called serviceInit().
			nmTokenSecretManager = CreateNMTokenSecretManager(conf);
			rmContext.SetNMTokenSecretManager(nmTokenSecretManager);
			containerTokenSecretManager = CreateContainerTokenSecretManager(conf);
			rmContext.SetContainerTokenSecretManager(containerTokenSecretManager);
			clientToAMSecretManager = CreateClientToAMTokenSecretManager();
			rmContext.SetClientToAMTokenSecretManager(clientToAMSecretManager);
			amRmTokenSecretManager = CreateAMRMTokenSecretManager(conf, this.rmContext);
			rmContext.SetAMRMTokenSecretManager(amRmTokenSecretManager);
			rmDTSecretManager = CreateRMDelegationTokenSecretManager(conf, rmContext);
			rmContext.SetRMDelegationTokenSecretManager(rmDTSecretManager);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			amRmTokenSecretManager.Start();
			containerTokenSecretManager.Start();
			nmTokenSecretManager.Start();
			try
			{
				rmDTSecretManager.StartThreads();
			}
			catch (IOException ie)
			{
				throw new YarnRuntimeException("Failed to start secret manager threads", ie);
			}
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (rmDTSecretManager != null)
			{
				rmDTSecretManager.StopThreads();
			}
			if (amRmTokenSecretManager != null)
			{
				amRmTokenSecretManager.Stop();
			}
			if (containerTokenSecretManager != null)
			{
				containerTokenSecretManager.Stop();
			}
			if (nmTokenSecretManager != null)
			{
				nmTokenSecretManager.Stop();
			}
			base.ServiceStop();
		}

		protected internal virtual RMContainerTokenSecretManager CreateContainerTokenSecretManager
			(Configuration conf)
		{
			return new RMContainerTokenSecretManager(conf);
		}

		protected internal virtual NMTokenSecretManagerInRM CreateNMTokenSecretManager(Configuration
			 conf)
		{
			return new NMTokenSecretManagerInRM(conf);
		}

		protected internal virtual AMRMTokenSecretManager CreateAMRMTokenSecretManager(Configuration
			 conf, RMContext rmContext)
		{
			return new AMRMTokenSecretManager(conf, rmContext);
		}

		protected internal virtual ClientToAMTokenSecretManagerInRM CreateClientToAMTokenSecretManager
			()
		{
			return new ClientToAMTokenSecretManagerInRM();
		}

		[VisibleForTesting]
		protected internal virtual RMDelegationTokenSecretManager CreateRMDelegationTokenSecretManager
			(Configuration conf, RMContext rmContext)
		{
			long secretKeyInterval = conf.GetLong(YarnConfiguration.RmDelegationKeyUpdateIntervalKey
				, YarnConfiguration.RmDelegationKeyUpdateIntervalDefault);
			long tokenMaxLifetime = conf.GetLong(YarnConfiguration.RmDelegationTokenMaxLifetimeKey
				, YarnConfiguration.RmDelegationTokenMaxLifetimeDefault);
			long tokenRenewInterval = conf.GetLong(YarnConfiguration.RmDelegationTokenRenewIntervalKey
				, YarnConfiguration.RmDelegationTokenRenewIntervalDefault);
			return new RMDelegationTokenSecretManager(secretKeyInterval, tokenMaxLifetime, tokenRenewInterval
				, 3600000, rmContext);
		}
	}
}
