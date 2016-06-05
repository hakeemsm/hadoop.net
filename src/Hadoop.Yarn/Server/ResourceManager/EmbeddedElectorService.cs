using System;
using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Zookeeper.Data;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class EmbeddedElectorService : AbstractService, ActiveStandbyElector.ActiveStandbyElectorCallback
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.EmbeddedElectorService
			).FullName);

		private static readonly HAServiceProtocol.StateChangeRequestInfo req = new HAServiceProtocol.StateChangeRequestInfo
			(HAServiceProtocol.RequestSource.RequestByZkfc);

		private RMContext rmContext;

		private byte[] localActiveNodeInfo;

		private ActiveStandbyElector elector;

		internal EmbeddedElectorService(RMContext rmContext)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.EmbeddedElectorService
				).FullName)
		{
			this.rmContext = rmContext;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			conf = conf is YarnConfiguration ? conf : new YarnConfiguration(conf);
			string zkQuorum = conf.Get(YarnConfiguration.RmZkAddress);
			if (zkQuorum == null)
			{
				throw new YarnRuntimeException("Embedded automatic failover " + "is enabled, but "
					 + YarnConfiguration.RmZkAddress + " is not set");
			}
			string rmId = HAUtil.GetRMHAId(conf);
			string clusterId = YarnConfiguration.GetClusterId(conf);
			localActiveNodeInfo = CreateActiveNodeInfo(clusterId, rmId);
			string zkBasePath = conf.Get(YarnConfiguration.AutoFailoverZkBasePath, YarnConfiguration
				.DefaultAutoFailoverZkBasePath);
			string electionZNode = zkBasePath + "/" + clusterId;
			long zkSessionTimeout = conf.GetLong(YarnConfiguration.RmZkTimeoutMs, YarnConfiguration
				.DefaultRmZkTimeoutMs);
			IList<ACL> zkAcls = RMZKUtils.GetZKAcls(conf);
			IList<ZKUtil.ZKAuthInfo> zkAuths = RMZKUtils.GetZKAuths(conf);
			int maxRetryNum = conf.GetInt(CommonConfigurationKeys.HaFcElectorZkOpRetriesKey, 
				CommonConfigurationKeys.HaFcElectorZkOpRetriesDefault);
			elector = new ActiveStandbyElector(zkQuorum, (int)zkSessionTimeout, electionZNode
				, zkAcls, zkAuths, this, maxRetryNum);
			elector.EnsureParentZNode();
			if (!IsParentZnodeSafe(clusterId))
			{
				NotifyFatalError(electionZNode + " znode has invalid data! " + "Might need formatting!"
					);
			}
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			elector.JoinElection(localActiveNodeInfo);
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (elector != null)
			{
				elector.QuitElection(false);
				elector.TerminateConnection();
			}
			base.ServiceStop();
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		public virtual void BecomeActive()
		{
			try
			{
				rmContext.GetRMAdminService().TransitionToActive(req);
			}
			catch (Exception e)
			{
				throw new ServiceFailedException("RM could not transition to Active", e);
			}
		}

		public virtual void BecomeStandby()
		{
			try
			{
				rmContext.GetRMAdminService().TransitionToStandby(req);
			}
			catch (Exception e)
			{
				Log.Error("RM could not transition to Standby", e);
			}
		}

		public virtual void EnterNeutralMode()
		{
		}

		public virtual void NotifyFatalError(string errorMessage)
		{
			rmContext.GetDispatcher().GetEventHandler().Handle(new RMFatalEvent(RMFatalEventType
				.EmbeddedElectorFailed, errorMessage));
		}

		public virtual void FenceOldActive(byte[] oldActiveData)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Request to fence old active being ignored, " + "as embedded leader election doesn't support fencing"
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static byte[] CreateActiveNodeInfo(string clusterId, string rmId)
		{
			return ((YarnServerResourceManagerServiceProtos.ActiveRMInfoProto)YarnServerResourceManagerServiceProtos.ActiveRMInfoProto
				.NewBuilder().SetClusterId(clusterId).SetRmId(rmId).Build()).ToByteArray();
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		private bool IsParentZnodeSafe(string clusterId)
		{
			byte[] data;
			try
			{
				data = elector.GetActiveData();
			}
			catch (ActiveStandbyElector.ActiveNotFoundException)
			{
				// no active found, parent znode is safe
				return true;
			}
			YarnServerResourceManagerServiceProtos.ActiveRMInfoProto proto;
			try
			{
				proto = YarnServerResourceManagerServiceProtos.ActiveRMInfoProto.ParseFrom(data);
			}
			catch (InvalidProtocolBufferException)
			{
				Log.Error("Invalid data in ZK: " + StringUtils.ByteToHexString(data));
				return false;
			}
			// Check if the passed proto corresponds to an RM in the same cluster
			if (!proto.GetClusterId().Equals(clusterId))
			{
				Log.Error("Mismatched cluster! The other RM seems " + "to be from a different cluster. Current cluster = "
					 + clusterId + "Other RM's cluster = " + proto.GetClusterId());
				return false;
			}
			return true;
		}

		public virtual void ResetLeaderElection()
		{
			elector.QuitElection(false);
			elector.JoinElection(localActiveNodeInfo);
		}

		public virtual string GetHAZookeeperConnectionState()
		{
			return elector.GetHAZookeeperConnectionState();
		}
	}
}
