using System;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>
	/// This class implements the aspects that relate to delegation tokens for all
	/// HTTP-based file system.
	/// </summary>
	internal sealed class TokenAspect<T>
		where T : FileSystem
	{
		public class TokenManager : TokenRenewer
		{
			/// <exception cref="System.IO.IOException"/>
			public override void Cancel<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				, Configuration conf)
			{
				GetInstance(token, conf).CancelDelegationToken(token);
			}

			public override bool HandleKind(Text kind)
			{
				return kind.Equals(HftpFileSystem.TokenKind) || kind.Equals(HsftpFileSystem.TokenKind
					) || kind.Equals(WebHdfsFileSystem.TokenKind) || kind.Equals(SWebHdfsFileSystem.
					TokenKind);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool IsManaged<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				)
			{
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long Renew<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				, Configuration conf)
			{
				return GetInstance(token, conf).RenewDelegationToken(token);
			}

			/// <exception cref="System.IO.IOException"/>
			private TokenAspect.TokenManagementDelegator GetInstance<_T0>(Org.Apache.Hadoop.Security.Token.Token
				<_T0> token, Configuration conf)
				where _T0 : TokenIdentifier
			{
				URI uri;
				string scheme = GetSchemeByKind(token.GetKind());
				if (HAUtil.IsTokenForLogicalUri(token))
				{
					uri = HAUtil.GetServiceUriFromToken(scheme, token);
				}
				else
				{
					IPEndPoint address = SecurityUtil.GetTokenServiceAddr(token);
					uri = URI.Create(scheme + "://" + NetUtils.GetHostPortString(address));
				}
				return (TokenAspect.TokenManagementDelegator)FileSystem.Get(uri, conf);
			}

			private static string GetSchemeByKind(Text kind)
			{
				if (kind.Equals(HftpFileSystem.TokenKind))
				{
					return HftpFileSystem.Scheme;
				}
				else
				{
					if (kind.Equals(HsftpFileSystem.TokenKind))
					{
						return HsftpFileSystem.Scheme;
					}
					else
					{
						if (kind.Equals(WebHdfsFileSystem.TokenKind))
						{
							return WebHdfsFileSystem.Scheme;
						}
						else
						{
							if (kind.Equals(SWebHdfsFileSystem.TokenKind))
							{
								return SWebHdfsFileSystem.Scheme;
							}
							else
							{
								throw new ArgumentException("Unsupported scheme");
							}
						}
					}
				}
			}
		}

		private class DTSelecorByKind : AbstractDelegationTokenSelector<DelegationTokenIdentifier
			>
		{
			public DTSelecorByKind(Text kind)
				: base(kind)
			{
			}
		}

		/// <summary>Callbacks for token management</summary>
		internal interface TokenManagementDelegator
		{
			/// <exception cref="System.IO.IOException"/>
			void CancelDelegationToken<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				)
				where _T0 : TokenIdentifier;

			/// <exception cref="System.IO.IOException"/>
			long RenewDelegationToken<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token)
				where _T0 : TokenIdentifier;
		}

		private DelegationTokenRenewer.RenewAction<object> action;

		private DelegationTokenRenewer dtRenewer = null;

		private readonly TokenAspect.DTSelecorByKind dtSelector;

		private readonly T fs;

		private bool hasInitedToken;

		private readonly Log Log;

		private readonly Text serviceName;

		internal TokenAspect(T fs, Text serviceName, Text kind)
		{
			this.Log = LogFactory.GetLog(fs.GetType());
			this.fs = fs;
			this.dtSelector = new TokenAspect.DTSelecorByKind(kind);
			this.serviceName = serviceName;
		}

		/// <exception cref="System.IO.IOException"/>
		internal void EnsureTokenInitialized()
		{
			lock (this)
			{
				// we haven't inited yet, or we used to have a token but it expired
				if (!hasInitedToken || (action != null && !action.IsValid()))
				{
					//since we don't already have a token, go get one
					Org.Apache.Hadoop.Security.Token.Token<object> token = fs.GetDelegationToken(null
						);
					// security might be disabled
					if (token != null)
					{
						fs.SetDelegationToken(token);
						AddRenewAction(fs);
						Log.Debug("Created new DT for " + token.GetService());
					}
					hasInitedToken = true;
				}
			}
		}

		public void Reset()
		{
			lock (this)
			{
				hasInitedToken = false;
			}
		}

		internal void InitDelegationToken(UserGroupInformation ugi)
		{
			lock (this)
			{
				Org.Apache.Hadoop.Security.Token.Token<object> token = SelectDelegationToken(ugi);
				if (token != null)
				{
					Log.Debug("Found existing DT for " + token.GetService());
					fs.SetDelegationToken(token);
					hasInitedToken = true;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal void RemoveRenewAction()
		{
			lock (this)
			{
				if (dtRenewer != null)
				{
					dtRenewer.RemoveRenewAction(fs);
				}
			}
		}

		[VisibleForTesting]
		internal Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> SelectDelegationToken
			(UserGroupInformation ugi)
		{
			return dtSelector.SelectToken(serviceName, ugi.GetTokens());
		}

		private void AddRenewAction(T webhdfs)
		{
			lock (this)
			{
				if (dtRenewer == null)
				{
					dtRenewer = DelegationTokenRenewer.GetInstance();
				}
				action = dtRenewer.AddRenewAction(webhdfs);
			}
		}
	}
}
