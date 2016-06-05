using System.IO;
using System.Net;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security.Client
{
	/// <summary>
	/// Delegation Token Identifier that identifies the delegation tokens from the
	/// Resource Manager.
	/// </summary>
	public class RMDelegationTokenIdentifier : YARNDelegationTokenIdentifier
	{
		public static readonly Text KindName = new Text("RM_DELEGATION_TOKEN");

		public RMDelegationTokenIdentifier()
		{
		}

		/// <summary>Create a new delegation token identifier</summary>
		/// <param name="owner">the effective username of the token owner</param>
		/// <param name="renewer">the username of the renewer</param>
		/// <param name="realUser">the real username of the token owner</param>
		public RMDelegationTokenIdentifier(Text owner, Text renewer, Text realUser)
			: base(owner, renewer, realUser)
		{
		}

		public override Text GetKind()
		{
			return KindName;
		}

		public class Renewer : TokenRenewer
		{
			public override bool HandleKind(Text kind)
			{
				return KindName.Equals(kind);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool IsManaged<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				)
			{
				return true;
			}

			private static AbstractDelegationTokenSecretManager<RMDelegationTokenIdentifier> 
				localSecretManager;

			private static IPEndPoint localServiceAddress;

			[InterfaceAudience.Private]
			public static void SetSecretManager(AbstractDelegationTokenSecretManager<RMDelegationTokenIdentifier
				> secretManager, IPEndPoint serviceAddress)
			{
				localSecretManager = secretManager;
				localServiceAddress = serviceAddress;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override long Renew<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				, Configuration conf)
			{
				ApplicationClientProtocol rmClient = GetRmClient(token, conf);
				if (rmClient != null)
				{
					try
					{
						RenewDelegationTokenRequest request = Records.NewRecord<RenewDelegationTokenRequest
							>();
						request.SetDelegationToken(ConvertToProtoToken(token));
						return rmClient.RenewDelegationToken(request).GetNextExpirationTime();
					}
					catch (YarnException e)
					{
						throw new IOException(e);
					}
					finally
					{
						RPC.StopProxy(rmClient);
					}
				}
				else
				{
					return localSecretManager.RenewToken((Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier
						>)token, GetRenewer(token));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Cancel<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				, Configuration conf)
			{
				ApplicationClientProtocol rmClient = GetRmClient(token, conf);
				if (rmClient != null)
				{
					try
					{
						CancelDelegationTokenRequest request = Records.NewRecord<CancelDelegationTokenRequest
							>();
						request.SetDelegationToken(ConvertToProtoToken(token));
						rmClient.CancelDelegationToken(request);
					}
					catch (YarnException e)
					{
						throw new IOException(e);
					}
					finally
					{
						RPC.StopProxy(rmClient);
					}
				}
				else
				{
					localSecretManager.CancelToken((Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier
						>)token, GetRenewer(token));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private static ApplicationClientProtocol GetRmClient<_T0>(Org.Apache.Hadoop.Security.Token.Token
				<_T0> token, Configuration conf)
				where _T0 : TokenIdentifier
			{
				string[] services = token.GetService().ToString().Split(",");
				foreach (string service in services)
				{
					IPEndPoint addr = NetUtils.CreateSocketAddr(service);
					if (localSecretManager != null)
					{
						// return null if it's our token
						if (localServiceAddress.Address.IsAnyLocalAddress())
						{
							if (NetUtils.IsLocalAddress(addr.Address) && addr.Port == localServiceAddress.Port)
							{
								return null;
							}
						}
						else
						{
							if (addr.Equals(localServiceAddress))
							{
								return null;
							}
						}
					}
				}
				return ClientRMProxy.CreateRMProxy<ApplicationClientProtocol>(conf);
			}

			// get renewer so we can always renew our own tokens
			/// <exception cref="System.IO.IOException"/>
			private static string GetRenewer<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0>
				 token)
				where _T0 : TokenIdentifier
			{
				return ((Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier>)token
					).DecodeIdentifier().GetRenewer().ToString();
			}

			private static Org.Apache.Hadoop.Yarn.Api.Records.Token ConvertToProtoToken<_T0>(
				Org.Apache.Hadoop.Security.Token.Token<_T0> token)
				where _T0 : TokenIdentifier
			{
				return Org.Apache.Hadoop.Yarn.Api.Records.Token.NewInstance(token.GetIdentifier()
					, token.GetKind().ToString(), token.GetPassword(), token.GetService().ToString()
					);
			}
		}
	}
}
