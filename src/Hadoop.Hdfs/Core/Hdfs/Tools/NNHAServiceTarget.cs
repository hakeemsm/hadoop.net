using System;
using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>
	/// One of the NN NameNodes acting as the target of an administrative command
	/// (e.g.
	/// </summary>
	/// <remarks>
	/// One of the NN NameNodes acting as the target of an administrative command
	/// (e.g. failover).
	/// </remarks>
	public class NNHAServiceTarget : HAServiceTarget
	{
		private const string NameserviceIdKey = "nameserviceid";

		private const string NamenodeIdKey = "namenodeid";

		private readonly IPEndPoint addr;

		private IPEndPoint zkfcAddr;

		private NodeFencer fencer;

		private BadFencingConfigurationException fenceConfigError;

		private readonly string nnId;

		private readonly string nsId;

		private readonly bool autoFailoverEnabled;

		public NNHAServiceTarget(Configuration conf, string nsId, string nnId)
		{
			// Keys added to the fencing script environment
			Preconditions.CheckNotNull(nnId);
			if (nsId == null)
			{
				nsId = DFSUtil.GetOnlyNameServiceIdOrNull(conf);
				if (nsId == null)
				{
					throw new ArgumentException("Unable to determine the nameservice id.");
				}
			}
			System.Diagnostics.Debug.Assert(nsId != null);
			// Make a copy of the conf, and override configs based on the
			// target node -- not the node we happen to be running on.
			HdfsConfiguration targetConf = new HdfsConfiguration(conf);
			NameNode.InitializeGenericKeys(targetConf, nsId, nnId);
			string serviceAddr = DFSUtil.GetNamenodeServiceAddr(targetConf, nsId, nnId);
			if (serviceAddr == null)
			{
				throw new ArgumentException("Unable to determine service address for namenode '" 
					+ nnId + "'");
			}
			this.addr = NetUtils.CreateSocketAddr(serviceAddr, NameNode.DefaultPort);
			this.autoFailoverEnabled = targetConf.GetBoolean(DFSConfigKeys.DfsHaAutoFailoverEnabledKey
				, DFSConfigKeys.DfsHaAutoFailoverEnabledDefault);
			if (autoFailoverEnabled)
			{
				int port = DFSZKFailoverController.GetZkfcPort(targetConf);
				if (port != 0)
				{
					SetZkfcPort(port);
				}
			}
			try
			{
				this.fencer = NodeFencer.Create(targetConf, DFSConfigKeys.DfsHaFenceMethodsKey);
			}
			catch (BadFencingConfigurationException e)
			{
				this.fenceConfigError = e;
			}
			this.nnId = nnId;
			this.nsId = nsId;
		}

		/// <returns>the NN's IPC address.</returns>
		public override IPEndPoint GetAddress()
		{
			return addr;
		}

		public override IPEndPoint GetZKFCAddress()
		{
			Preconditions.CheckState(autoFailoverEnabled, "ZKFC address not relevant when auto failover is off"
				);
			System.Diagnostics.Debug.Assert(zkfcAddr != null);
			return zkfcAddr;
		}

		internal virtual void SetZkfcPort(int port)
		{
			System.Diagnostics.Debug.Assert(autoFailoverEnabled);
			this.zkfcAddr = new IPEndPoint(addr.Address, port);
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		public override void CheckFencingConfigured()
		{
			if (fenceConfigError != null)
			{
				throw fenceConfigError;
			}
			if (fencer == null)
			{
				throw new BadFencingConfigurationException("No fencer configured for " + this);
			}
		}

		public override NodeFencer GetFencer()
		{
			return fencer;
		}

		public override string ToString()
		{
			return "NameNode at " + addr;
		}

		public virtual string GetNameServiceId()
		{
			return this.nsId;
		}

		public virtual string GetNameNodeId()
		{
			return this.nnId;
		}

		protected override void AddFencingParameters(IDictionary<string, string> ret)
		{
			base.AddFencingParameters(ret);
			ret[NameserviceIdKey] = GetNameServiceId();
			ret[NamenodeIdKey] = GetNameNodeId();
		}

		public override bool IsAutoFailoverEnabled()
		{
			return autoFailoverEnabled;
		}
	}
}
