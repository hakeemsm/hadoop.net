using System;
using System.Net;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.Proto;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	public class DFSZKFailoverController : ZKFailoverController
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Tools.DFSZKFailoverController
			));

		private readonly AccessControlList adminAcl;

		private readonly NNHAServiceTarget localNNTarget;

		/* the same as superclass's localTarget, but with the more specfic NN type */
		protected override HAServiceTarget DataToTarget(byte[] data)
		{
			HAZKInfoProtos.ActiveNodeInfo proto;
			try
			{
				proto = HAZKInfoProtos.ActiveNodeInfo.ParseFrom(data);
			}
			catch (InvalidProtocolBufferException)
			{
				throw new RuntimeException("Invalid data in ZK: " + StringUtils.ByteToHexString(data
					));
			}
			NNHAServiceTarget ret = new NNHAServiceTarget(conf, proto.GetNameserviceId(), proto
				.GetNamenodeId());
			IPEndPoint addressFromProtobuf = new IPEndPoint(proto.GetHostname(), proto.GetPort
				());
			if (!addressFromProtobuf.Equals(ret.GetAddress()))
			{
				throw new RuntimeException("Mismatched address stored in ZK for " + ret + ": Stored protobuf was "
					 + proto + ", address from our own " + "configuration for this NameNode was " + 
					ret.GetAddress());
			}
			ret.SetZkfcPort(proto.GetZkfcPort());
			return ret;
		}

		protected override byte[] TargetToData(HAServiceTarget target)
		{
			IPEndPoint addr = target.GetAddress();
			return ((HAZKInfoProtos.ActiveNodeInfo)HAZKInfoProtos.ActiveNodeInfo.NewBuilder()
				.SetHostname(addr.GetHostName()).SetPort(addr.Port).SetZkfcPort(target.GetZKFCAddress
				().Port).SetNameserviceId(localNNTarget.GetNameServiceId()).SetNamenodeId(localNNTarget
				.GetNameNodeId()).Build()).ToByteArray();
		}

		protected override IPEndPoint GetRpcAddressToBindTo()
		{
			int zkfcPort = GetZkfcPort(conf);
			return new IPEndPoint(localTarget.GetAddress().Address, zkfcPort);
		}

		protected override PolicyProvider GetPolicyProvider()
		{
			return new HDFSPolicyProvider();
		}

		internal static int GetZkfcPort(Configuration conf)
		{
			return conf.GetInt(DFSConfigKeys.DfsHaZkfcPortKey, DFSConfigKeys.DfsHaZkfcPortDefault
				);
		}

		public static Org.Apache.Hadoop.Hdfs.Tools.DFSZKFailoverController Create(Configuration
			 conf)
		{
			Configuration localNNConf = DFSHAAdmin.AddSecurityConfiguration(conf);
			string nsId = DFSUtil.GetNamenodeNameServiceId(conf);
			if (!HAUtil.IsHAEnabled(localNNConf, nsId))
			{
				throw new HadoopIllegalArgumentException("HA is not enabled for this namenode.");
			}
			string nnId = HAUtil.GetNameNodeId(localNNConf, nsId);
			if (nnId == null)
			{
				string msg = "Could not get the namenode ID of this node. " + "You may run zkfc on the node other than namenode.";
				throw new HadoopIllegalArgumentException(msg);
			}
			NameNode.InitializeGenericKeys(localNNConf, nsId, nnId);
			DFSUtil.SetGenericConf(localNNConf, nsId, nnId, ZkfcConfKeys);
			NNHAServiceTarget localTarget = new NNHAServiceTarget(localNNConf, nsId, nnId);
			return new Org.Apache.Hadoop.Hdfs.Tools.DFSZKFailoverController(localNNConf, localTarget
				);
		}

		private DFSZKFailoverController(Configuration conf, NNHAServiceTarget localTarget
			)
			: base(conf, localTarget)
		{
			this.localNNTarget = localTarget;
			// Setup ACLs
			adminAcl = new AccessControlList(conf.Get(DFSConfigKeys.DfsAdmin, " "));
			Log.Info("Failover controller configured for NameNode " + localTarget);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void InitRPC()
		{
			base.InitRPC();
			localNNTarget.SetZkfcPort(rpcServer.GetAddress().Port);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void LoginAsFCUser()
		{
			IPEndPoint socAddr = NameNode.GetAddress(conf);
			SecurityUtil.Login(conf, DFSConfigKeys.DfsNamenodeKeytabFileKey, DFSConfigKeys.DfsNamenodeKerberosPrincipalKey
				, socAddr.GetHostName());
		}

		protected override string GetScopeInsideParentNode()
		{
			return localNNTarget.GetNameServiceId();
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			if (DFSUtil.ParseHelpArgument(args, ZKFailoverController.Usage, System.Console.Out
				, true))
			{
				System.Environment.Exit(0);
			}
			GenericOptionsParser parser = new GenericOptionsParser(new HdfsConfiguration(), args
				);
			Org.Apache.Hadoop.Hdfs.Tools.DFSZKFailoverController zkfc = Org.Apache.Hadoop.Hdfs.Tools.DFSZKFailoverController
				.Create(parser.GetConfiguration());
			int retCode = 0;
			try
			{
				retCode = zkfc.Run(parser.GetRemainingArgs());
			}
			catch (Exception t)
			{
				Log.Fatal("Got a fatal error, exiting now", t);
			}
			System.Environment.Exit(retCode);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		protected override void CheckRpcAdminAccess()
		{
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			UserGroupInformation zkfcUgi = UserGroupInformation.GetLoginUser();
			if (adminAcl.IsUserAllowed(ugi) || ugi.GetShortUserName().Equals(zkfcUgi.GetShortUserName
				()))
			{
				Log.Info("Allowed RPC access from " + ugi + " at " + Org.Apache.Hadoop.Ipc.Server
					.GetRemoteAddress());
				return;
			}
			string msg = "Disallowed RPC access from " + ugi + " at " + Org.Apache.Hadoop.Ipc.Server
				.GetRemoteAddress() + ". Not listed in " + DFSConfigKeys.DfsAdmin;
			Log.Warn(msg);
			throw new AccessControlException(msg);
		}
	}
}
