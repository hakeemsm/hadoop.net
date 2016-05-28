using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Security
{
	public class MRDelegationTokenRenewer : TokenRenewer
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(MRDelegationTokenRenewer
			));

		public override bool HandleKind(Text kind)
		{
			return MRDelegationTokenIdentifier.KindName.Equals(kind);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override long Renew<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
			, Configuration conf)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Token dToken = Org.Apache.Hadoop.Yarn.Api.Records.Token
				.NewInstance(token.GetIdentifier(), token.GetKind().ToString(), token.GetPassword
				(), token.GetService().ToString());
			MRClientProtocol histProxy = InstantiateHistoryProxy(conf, SecurityUtil.GetTokenServiceAddr
				(token));
			try
			{
				RenewDelegationTokenRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<RenewDelegationTokenRequest>();
				request.SetDelegationToken(dToken);
				return histProxy.RenewDelegationToken(request).GetNextExpirationTime();
			}
			finally
			{
				StopHistoryProxy(histProxy);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Cancel<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
			, Configuration conf)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Token dToken = Org.Apache.Hadoop.Yarn.Api.Records.Token
				.NewInstance(token.GetIdentifier(), token.GetKind().ToString(), token.GetPassword
				(), token.GetService().ToString());
			MRClientProtocol histProxy = InstantiateHistoryProxy(conf, SecurityUtil.GetTokenServiceAddr
				(token));
			try
			{
				CancelDelegationTokenRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<CancelDelegationTokenRequest>();
				request.SetDelegationToken(dToken);
				histProxy.CancelDelegationToken(request);
			}
			finally
			{
				StopHistoryProxy(histProxy);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsManaged<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
			)
		{
			return true;
		}

		protected internal virtual void StopHistoryProxy(MRClientProtocol proxy)
		{
			RPC.StopProxy(proxy);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual MRClientProtocol InstantiateHistoryProxy(Configuration
			 conf, IPEndPoint hsAddress)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Connecting to MRHistoryServer at: " + hsAddress);
			}
			YarnRPC rpc = YarnRPC.Create(conf);
			UserGroupInformation currentUser = UserGroupInformation.GetCurrentUser();
			return currentUser.DoAs(new _PrivilegedAction_113(rpc, hsAddress, conf));
		}

		private sealed class _PrivilegedAction_113 : PrivilegedAction<MRClientProtocol>
		{
			public _PrivilegedAction_113(YarnRPC rpc, IPEndPoint hsAddress, Configuration conf
				)
			{
				this.rpc = rpc;
				this.hsAddress = hsAddress;
				this.conf = conf;
			}

			public MRClientProtocol Run()
			{
				return (MRClientProtocol)rpc.GetProxy(typeof(HSClientProtocol), hsAddress, conf);
			}

			private readonly YarnRPC rpc;

			private readonly IPEndPoint hsAddress;

			private readonly Configuration conf;
		}
	}
}
