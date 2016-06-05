using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Hadoop.Common.Core.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security.Token.Delegation
{
	public abstract class AbstractDelegationTokenSecretManager<TokenIdent> : SecretManager
		<TokenIdent>
		where TokenIdent : AbstractDelegationTokenIdentifier
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.Token.Delegation.AbstractDelegationTokenSecretManager
			));

		/// <summary>
		/// Cache of currently valid tokens, mapping from DelegationTokenIdentifier
		/// to DelegationTokenInformation.
		/// </summary>
		/// <remarks>
		/// Cache of currently valid tokens, mapping from DelegationTokenIdentifier
		/// to DelegationTokenInformation. Protected by this object lock.
		/// </remarks>
		protected internal readonly IDictionary<TokenIdent, AbstractDelegationTokenSecretManager.DelegationTokenInformation
			> currentTokens = new Dictionary<TokenIdent, AbstractDelegationTokenSecretManager.DelegationTokenInformation
			>();

		/// <summary>Sequence number to create DelegationTokenIdentifier.</summary>
		/// <remarks>
		/// Sequence number to create DelegationTokenIdentifier.
		/// Protected by this object lock.
		/// </remarks>
		protected internal int delegationTokenSequenceNumber = 0;

		/// <summary>Access to allKeys is protected by this object lock</summary>
		protected internal readonly IDictionary<int, DelegationKey> allKeys = new Dictionary
			<int, DelegationKey>();

		/// <summary>Access to currentId is protected by this object lock.</summary>
		protected internal int currentId = 0;

		/// <summary>Access to currentKey is protected by this object lock</summary>
		private DelegationKey currentKey;

		private long keyUpdateInterval;

		private long tokenMaxLifetime;

		private long tokenRemoverScanInterval;

		private long tokenRenewInterval;

		/// <summary>Whether to store a token's tracking ID in its TokenInformation.</summary>
		/// <remarks>
		/// Whether to store a token's tracking ID in its TokenInformation.
		/// Can be overridden by a subclass.
		/// </remarks>
		protected internal bool storeTokenTrackingId;

		private Thread tokenRemoverThread;

		protected internal volatile bool running;

		/// <summary>
		/// If the delegation token update thread holds this lock, it will
		/// not get interrupted.
		/// </summary>
		protected internal object noInterruptsLock = new object();

		public AbstractDelegationTokenSecretManager(long delegationKeyUpdateInterval, long
			 delegationTokenMaxLifetime, long delegationTokenRenewInterval, long delegationTokenRemoverScanInterval
			)
		{
			this.keyUpdateInterval = delegationKeyUpdateInterval;
			this.tokenMaxLifetime = delegationTokenMaxLifetime;
			this.tokenRenewInterval = delegationTokenRenewInterval;
			this.tokenRemoverScanInterval = delegationTokenRemoverScanInterval;
			this.storeTokenTrackingId = false;
		}

		/// <summary>should be called before this object is used</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void StartThreads()
		{
			Preconditions.CheckState(!running);
			UpdateCurrentKey();
			lock (this)
			{
				running = true;
				tokenRemoverThread = new Daemon(new AbstractDelegationTokenSecretManager.ExpiredTokenRemover
					(this));
				tokenRemoverThread.Start();
			}
		}

		/// <summary>Reset all data structures and mutable state.</summary>
		public virtual void Reset()
		{
			lock (this)
			{
				SetCurrentKeyId(0);
				allKeys.Clear();
				SetDelegationTokenSeqNum(0);
				currentTokens.Clear();
			}
		}

		/// <summary>
		/// Add a previously used master key to cache (when NN restarts),
		/// should be called before activate().
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AddKey(DelegationKey key)
		{
			lock (this)
			{
				if (running)
				{
					// a safety check
					throw new IOException("Can't add delegation key to a running SecretManager.");
				}
				if (key.GetKeyId() > GetCurrentKeyId())
				{
					SetCurrentKeyId(key.GetKeyId());
				}
				allKeys[key.GetKeyId()] = key;
			}
		}

		public virtual DelegationKey[] GetAllKeys()
		{
			lock (this)
			{
				return Collections.ToArray(allKeys.Values, new DelegationKey[0]);
			}
		}

		// HDFS
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void LogUpdateMasterKey(DelegationKey key)
		{
			return;
		}

		// HDFS
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void LogExpireToken(TokenIdent ident)
		{
			return;
		}

		// RM
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void StoreNewMasterKey(DelegationKey key)
		{
			return;
		}

		// RM
		protected internal virtual void RemoveStoredMasterKey(DelegationKey key)
		{
			return;
		}

		// RM
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void StoreNewToken(TokenIdent ident, long renewDate)
		{
			return;
		}

		// RM
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void RemoveStoredToken(TokenIdent ident)
		{
		}

		// RM
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void UpdateStoredToken(TokenIdent ident, long renewDate
			)
		{
			return;
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		protected internal virtual int GetCurrentKeyId()
		{
			lock (this)
			{
				return currentId;
			}
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		protected internal virtual int IncrementCurrentKeyId()
		{
			lock (this)
			{
				return ++currentId;
			}
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		protected internal virtual void SetCurrentKeyId(int keyId)
		{
			lock (this)
			{
				currentId = keyId;
			}
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		protected internal virtual int GetDelegationTokenSeqNum()
		{
			lock (this)
			{
				return delegationTokenSequenceNumber;
			}
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		protected internal virtual int IncrementDelegationTokenSeqNum()
		{
			lock (this)
			{
				return ++delegationTokenSequenceNumber;
			}
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		protected internal virtual void SetDelegationTokenSeqNum(int seqNum)
		{
			lock (this)
			{
				delegationTokenSequenceNumber = seqNum;
			}
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		protected internal virtual DelegationKey GetDelegationKey(int keyId)
		{
			return allKeys[keyId];
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void StoreDelegationKey(DelegationKey key)
		{
			allKeys[key.GetKeyId()] = key;
			StoreNewMasterKey(key);
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void UpdateDelegationKey(DelegationKey key)
		{
			allKeys[key.GetKeyId()] = key;
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		protected internal virtual AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 GetTokenInfo(TokenIdent ident)
		{
			return currentTokens[ident];
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void StoreToken(TokenIdent ident, AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 tokenInfo)
		{
			currentTokens[ident] = tokenInfo;
			StoreNewToken(ident, tokenInfo.GetRenewDate());
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void UpdateToken(TokenIdent ident, AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 tokenInfo)
		{
			currentTokens[ident] = tokenInfo;
			UpdateStoredToken(ident, tokenInfo.GetRenewDate());
		}

		/// <summary>
		/// This method is intended to be used for recovering persisted delegation
		/// tokens
		/// This method must be called before this secret manager is activated (before
		/// startThreads() is called)
		/// </summary>
		/// <param name="identifier">identifier read from persistent storage</param>
		/// <param name="renewDate">token renew time</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AddPersistedDelegationToken(TokenIdent identifier, long renewDate
			)
		{
			lock (this)
			{
				if (running)
				{
					// a safety check
					throw new IOException("Can't add persisted delegation token to a running SecretManager."
						);
				}
				int keyId = identifier.GetMasterKeyId();
				DelegationKey dKey = allKeys[keyId];
				if (dKey == null)
				{
					Log.Warn("No KEY found for persisted identifier " + identifier.ToString());
					return;
				}
				byte[] password = CreatePassword(identifier.GetBytes(), dKey.GetKey());
				if (identifier.GetSequenceNumber() > GetDelegationTokenSeqNum())
				{
					SetDelegationTokenSeqNum(identifier.GetSequenceNumber());
				}
				if (GetTokenInfo(identifier) == null)
				{
					currentTokens[identifier] = new AbstractDelegationTokenSecretManager.DelegationTokenInformation
						(renewDate, password, GetTrackingIdIfEnabled(identifier));
				}
				else
				{
					throw new IOException("Same delegation token being added twice.");
				}
			}
		}

		/// <summary>
		/// Update the current master key
		/// This is called once by startThreads before tokenRemoverThread is created,
		/// and only by tokenRemoverThread afterwards.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void UpdateCurrentKey()
		{
			Log.Info("Updating the current master key for generating delegation tokens");
			/* Create a new currentKey with an estimated expiry date. */
			int newCurrentId;
			lock (this)
			{
				newCurrentId = IncrementCurrentKeyId();
			}
			DelegationKey newKey = new DelegationKey(newCurrentId, Runtime.CurrentTimeMillis(
				) + keyUpdateInterval + tokenMaxLifetime, GenerateSecret());
			//Log must be invoked outside the lock on 'this'
			LogUpdateMasterKey(newKey);
			lock (this)
			{
				currentKey = newKey;
				StoreDelegationKey(currentKey);
			}
		}

		/// <summary>
		/// Update the current master key for generating delegation tokens
		/// It should be called only by tokenRemoverThread.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void RollMasterKey()
		{
			lock (this)
			{
				RemoveExpiredKeys();
				/* set final expiry date for retiring currentKey */
				currentKey.SetExpiryDate(Time.Now() + tokenMaxLifetime);
				/*
				* currentKey might have been removed by removeExpiredKeys(), if
				* updateMasterKey() isn't called at expected interval. Add it back to
				* allKeys just in case.
				*/
				UpdateDelegationKey(currentKey);
			}
			UpdateCurrentKey();
		}

		private void RemoveExpiredKeys()
		{
			lock (this)
			{
				long now = Time.Now();
				for (IEnumerator<KeyValuePair<int, DelegationKey>> it = allKeys.GetEnumerator(); 
					it.HasNext(); )
				{
					KeyValuePair<int, DelegationKey> e = it.Next();
					if (e.Value.GetExpiryDate() < now)
					{
						it.Remove();
						// ensure the tokens generated by this current key can be recovered
						// with this current key after this current key is rolled
						if (!e.Value.Equals(currentKey))
						{
							RemoveStoredMasterKey(e.Value);
						}
					}
				}
			}
		}

		protected internal override byte[] CreatePassword(TokenIdent identifier)
		{
			lock (this)
			{
				int sequenceNum;
				long now = Time.Now();
				sequenceNum = IncrementDelegationTokenSeqNum();
				identifier.SetIssueDate(now);
				identifier.SetMaxDate(now + tokenMaxLifetime);
				identifier.SetMasterKeyId(currentKey.GetKeyId());
				identifier.SetSequenceNumber(sequenceNum);
				Log.Info("Creating password for identifier: " + identifier + ", currentKey: " + currentKey
					.GetKeyId());
				byte[] password = CreatePassword(identifier.GetBytes(), currentKey.GetKey());
				AbstractDelegationTokenSecretManager.DelegationTokenInformation tokenInfo = new AbstractDelegationTokenSecretManager.DelegationTokenInformation
					(now + tokenRenewInterval, password, GetTrackingIdIfEnabled(identifier));
				try
				{
					StoreToken(identifier, tokenInfo);
				}
				catch (IOException ioe)
				{
					Log.Error("Could not store token !!", ioe);
				}
				return password;
			}
		}

		/// <summary>
		/// Find the DelegationTokenInformation for the given token id, and verify that
		/// if the token is expired.
		/// </summary>
		/// <remarks>
		/// Find the DelegationTokenInformation for the given token id, and verify that
		/// if the token is expired. Note that this method should be called with
		/// acquiring the secret manager's monitor.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		protected internal virtual AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 CheckToken(TokenIdent identifier)
		{
			System.Diagnostics.Debug.Assert(Thread.HoldsLock(this));
			AbstractDelegationTokenSecretManager.DelegationTokenInformation info = GetTokenInfo
				(identifier);
			if (info == null)
			{
				throw new SecretManager.InvalidToken("token (" + identifier.ToString() + ") can't be found in cache"
					);
			}
			if (info.GetRenewDate() < Time.Now())
			{
				throw new SecretManager.InvalidToken("token (" + identifier.ToString() + ") is expired"
					);
			}
			return info;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public override byte[] RetrievePassword(TokenIdent identifier)
		{
			lock (this)
			{
				return CheckToken(identifier).GetPassword();
			}
		}

		protected internal virtual string GetTrackingIdIfEnabled(TokenIdent ident)
		{
			if (storeTokenTrackingId)
			{
				return ident.GetTrackingId();
			}
			return null;
		}

		public virtual string GetTokenTrackingId(TokenIdent identifier)
		{
			lock (this)
			{
				AbstractDelegationTokenSecretManager.DelegationTokenInformation info = GetTokenInfo
					(identifier);
				if (info == null)
				{
					return null;
				}
				return info.GetTrackingId();
			}
		}

		/// <summary>Verifies that the given identifier and password are valid and match.</summary>
		/// <param name="identifier">Token identifier.</param>
		/// <param name="password">Password in the token.</param>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public virtual void VerifyToken(TokenIdent identifier, byte[] password)
		{
			lock (this)
			{
				byte[] storedPassword = RetrievePassword(identifier);
				if (!Arrays.Equals(password, storedPassword))
				{
					throw new SecretManager.InvalidToken("token (" + identifier + ") is invalid, password doesn't match"
						);
				}
			}
		}

		/// <summary>Renew a delegation token.</summary>
		/// <param name="token">the token to renew</param>
		/// <param name="renewer">the full principal name of the user doing the renewal</param>
		/// <returns>the new expiration time</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken">if the token is invalid
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if the user can't renew token
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual long RenewToken(Org.Apache.Hadoop.Security.Token.Token<TokenIdent>
			 token, string renewer)
		{
			lock (this)
			{
				ByteArrayInputStream buf = new ByteArrayInputStream(token.GetIdentifier());
				DataInputStream @in = new DataInputStream(buf);
				TokenIdent id = CreateIdentifier();
				id.ReadFields(@in);
				Log.Info("Token renewal for identifier: " + id + "; total currentTokens " + currentTokens
					.Count);
				long now = Time.Now();
				if (id.GetMaxDate() < now)
				{
					throw new SecretManager.InvalidToken(renewer + " tried to renew an expired token"
						);
				}
				if ((id.GetRenewer() == null) || (id.GetRenewer().ToString().IsEmpty()))
				{
					throw new AccessControlException(renewer + " tried to renew a token without a renewer"
						);
				}
				if (!id.GetRenewer().ToString().Equals(renewer))
				{
					throw new AccessControlException(renewer + " tries to renew a token with renewer "
						 + id.GetRenewer());
				}
				DelegationKey key = GetDelegationKey(id.GetMasterKeyId());
				if (key == null)
				{
					throw new SecretManager.InvalidToken("Unable to find master key for keyId=" + id.
						GetMasterKeyId() + " from cache. Failed to renew an unexpired token" + " with sequenceNumber="
						 + id.GetSequenceNumber());
				}
				byte[] password = CreatePassword(token.GetIdentifier(), key.GetKey());
				if (!Arrays.Equals(password, token.GetPassword()))
				{
					throw new AccessControlException(renewer + " is trying to renew a token with wrong password"
						);
				}
				long renewTime = Math.Min(id.GetMaxDate(), now + tokenRenewInterval);
				string trackingId = GetTrackingIdIfEnabled(id);
				AbstractDelegationTokenSecretManager.DelegationTokenInformation info = new AbstractDelegationTokenSecretManager.DelegationTokenInformation
					(renewTime, password, trackingId);
				if (GetTokenInfo(id) == null)
				{
					throw new SecretManager.InvalidToken("Renewal request for unknown token");
				}
				UpdateToken(id, info);
				return renewTime;
			}
		}

		/// <summary>Cancel a token by removing it from cache.</summary>
		/// <returns>Identifier of the canceled token</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken">for invalid token
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if the user isn't allowed to cancel
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual TokenIdent CancelToken(Org.Apache.Hadoop.Security.Token.Token<TokenIdent
			> token, string canceller)
		{
			lock (this)
			{
				ByteArrayInputStream buf = new ByteArrayInputStream(token.GetIdentifier());
				DataInputStream @in = new DataInputStream(buf);
				TokenIdent id = CreateIdentifier();
				id.ReadFields(@in);
				Log.Info("Token cancelation requested for identifier: " + id);
				if (id.GetUser() == null)
				{
					throw new SecretManager.InvalidToken("Token with no owner");
				}
				string owner = id.GetUser().GetUserName();
				Text renewer = id.GetRenewer();
				HadoopKerberosName cancelerKrbName = new HadoopKerberosName(canceller);
				string cancelerShortName = cancelerKrbName.GetShortName();
				if (!canceller.Equals(owner) && (renewer == null || renewer.ToString().IsEmpty() 
					|| !cancelerShortName.Equals(renewer.ToString())))
				{
					throw new AccessControlException(canceller + " is not authorized to cancel the token"
						);
				}
				AbstractDelegationTokenSecretManager.DelegationTokenInformation info = Collections.Remove
					(currentTokens, id);
				if (info == null)
				{
					throw new SecretManager.InvalidToken("Token not found");
				}
				RemoveStoredToken(id);
				return id;
			}
		}

		/// <summary>Convert the byte[] to a secret key</summary>
		/// <param name="key">the byte[] to create the secret key from</param>
		/// <returns>the secret key</returns>
		protected internal static SecretKey CreateSecretKey(byte[] key)
		{
			return SecretManager.CreateSecretKey(key);
		}

		/// <summary>Class to encapsulate a token's renew date and password.</summary>
		public class DelegationTokenInformation
		{
			internal long renewDate;

			internal byte[] password;

			internal string trackingId;

			public DelegationTokenInformation(long renewDate, byte[] password)
				: this(renewDate, password, null)
			{
			}

			public DelegationTokenInformation(long renewDate, byte[] password, string trackingId
				)
			{
				this.renewDate = renewDate;
				this.password = password;
				this.trackingId = trackingId;
			}

			/// <summary>returns renew date</summary>
			public virtual long GetRenewDate()
			{
				return renewDate;
			}

			/// <summary>returns password</summary>
			internal virtual byte[] GetPassword()
			{
				return password;
			}

			/// <summary>returns tracking id</summary>
			public virtual string GetTrackingId()
			{
				return trackingId;
			}
		}

		/// <summary>Remove expired delegation tokens from cache</summary>
		/// <exception cref="System.IO.IOException"/>
		private void RemoveExpiredToken()
		{
			long now = Time.Now();
			ICollection<TokenIdent> expiredTokens = new HashSet<TokenIdent>();
			lock (this)
			{
				IEnumerator<KeyValuePair<TokenIdent, AbstractDelegationTokenSecretManager.DelegationTokenInformation
					>> i = currentTokens.GetEnumerator();
				while (i.HasNext())
				{
					KeyValuePair<TokenIdent, AbstractDelegationTokenSecretManager.DelegationTokenInformation
						> entry = i.Next();
					long renewDate = entry.Value.GetRenewDate();
					if (renewDate < now)
					{
						expiredTokens.AddItem(entry.Key);
						i.Remove();
					}
				}
			}
			// don't hold lock on 'this' to avoid edit log updates blocking token ops
			foreach (TokenIdent ident in expiredTokens)
			{
				LogExpireToken(ident);
				RemoveStoredToken(ident);
			}
		}

		public virtual void StopThreads()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Stopping expired delegation token remover thread");
			}
			running = false;
			if (tokenRemoverThread != null)
			{
				lock (noInterruptsLock)
				{
					tokenRemoverThread.Interrupt();
				}
				try
				{
					tokenRemoverThread.Join();
				}
				catch (Exception e)
				{
					throw new RuntimeException("Unable to join on token removal thread", e);
				}
			}
		}

		/// <summary>is secretMgr running</summary>
		/// <returns>true if secret mgr is running</returns>
		public virtual bool IsRunning()
		{
			lock (this)
			{
				return running;
			}
		}

		private class ExpiredTokenRemover : Thread
		{
			private long lastMasterKeyUpdate;

			private long lastTokenCacheCleanup;

			public override void Run()
			{
				AbstractDelegationTokenSecretManager.Log.Info("Starting expired delegation token remover thread, "
					 + "tokenRemoverScanInterval=" + this._enclosing.tokenRemoverScanInterval / (60 
					* 1000) + " min(s)");
				try
				{
					while (this._enclosing.running)
					{
						long now = Time.Now();
						if (this.lastMasterKeyUpdate + this._enclosing.keyUpdateInterval < now)
						{
							try
							{
								this._enclosing.RollMasterKey();
								this.lastMasterKeyUpdate = now;
							}
							catch (IOException e)
							{
								AbstractDelegationTokenSecretManager.Log.Error("Master key updating failed: ", e);
							}
						}
						if (this.lastTokenCacheCleanup + this._enclosing.tokenRemoverScanInterval < now)
						{
							this._enclosing.RemoveExpiredToken();
							this.lastTokenCacheCleanup = now;
						}
						try
						{
							Thread.Sleep(Math.Min(5000, this._enclosing.keyUpdateInterval));
						}
						catch (Exception ie)
						{
							// 5 seconds
							AbstractDelegationTokenSecretManager.Log.Error("ExpiredTokenRemover received " + 
								ie);
						}
					}
				}
				catch (Exception t)
				{
					AbstractDelegationTokenSecretManager.Log.Error("ExpiredTokenRemover thread received unexpected exception"
						, t);
					Runtime.GetRuntime().Exit(-1);
				}
			}

			internal ExpiredTokenRemover(AbstractDelegationTokenSecretManager<TokenIdent> _enclosing
				)
			{
				this._enclosing = _enclosing;
			}

			private readonly AbstractDelegationTokenSecretManager<TokenIdent> _enclosing;
		}

		/// <summary>Decode the token identifier.</summary>
		/// <remarks>
		/// Decode the token identifier. The subclass can customize the way to decode
		/// the token identifier.
		/// </remarks>
		/// <param name="token">the token where to extract the identifier</param>
		/// <returns>the delegation token identifier</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual TokenIdent DecodeTokenIdentifier(Org.Apache.Hadoop.Security.Token.Token
			<TokenIdent> token)
		{
			return token.DecodeIdentifier();
		}
	}
}
