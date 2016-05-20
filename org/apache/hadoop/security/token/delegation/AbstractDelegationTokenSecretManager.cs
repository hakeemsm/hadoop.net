using Sharpen;

namespace org.apache.hadoop.security.token.delegation
{
	public abstract class AbstractDelegationTokenSecretManager<TokenIdent> : org.apache.hadoop.security.token.SecretManager
		<TokenIdent>
		where TokenIdent : org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager
			)));

		/// <summary>
		/// Cache of currently valid tokens, mapping from DelegationTokenIdentifier
		/// to DelegationTokenInformation.
		/// </summary>
		/// <remarks>
		/// Cache of currently valid tokens, mapping from DelegationTokenIdentifier
		/// to DelegationTokenInformation. Protected by this object lock.
		/// </remarks>
		protected internal readonly System.Collections.Generic.IDictionary<TokenIdent, org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
			> currentTokens = new System.Collections.Generic.Dictionary<TokenIdent, org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
			>();

		/// <summary>Sequence number to create DelegationTokenIdentifier.</summary>
		/// <remarks>
		/// Sequence number to create DelegationTokenIdentifier.
		/// Protected by this object lock.
		/// </remarks>
		protected internal int delegationTokenSequenceNumber = 0;

		/// <summary>Access to allKeys is protected by this object lock</summary>
		protected internal readonly System.Collections.Generic.IDictionary<int, org.apache.hadoop.security.token.delegation.DelegationKey
			> allKeys = new System.Collections.Generic.Dictionary<int, org.apache.hadoop.security.token.delegation.DelegationKey
			>();

		/// <summary>Access to currentId is protected by this object lock.</summary>
		protected internal int currentId = 0;

		/// <summary>Access to currentKey is protected by this object lock</summary>
		private org.apache.hadoop.security.token.delegation.DelegationKey currentKey;

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

		private java.lang.Thread tokenRemoverThread;

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
		public virtual void startThreads()
		{
			com.google.common.@base.Preconditions.checkState(!running);
			updateCurrentKey();
			lock (this)
			{
				running = true;
				tokenRemoverThread = new org.apache.hadoop.util.Daemon(new org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.ExpiredTokenRemover
					(this));
				tokenRemoverThread.start();
			}
		}

		/// <summary>Reset all data structures and mutable state.</summary>
		public virtual void reset()
		{
			lock (this)
			{
				setCurrentKeyId(0);
				allKeys.clear();
				setDelegationTokenSeqNum(0);
				currentTokens.clear();
			}
		}

		/// <summary>
		/// Add a previously used master key to cache (when NN restarts),
		/// should be called before activate().
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void addKey(org.apache.hadoop.security.token.delegation.DelegationKey
			 key)
		{
			lock (this)
			{
				if (running)
				{
					// a safety check
					throw new System.IO.IOException("Can't add delegation key to a running SecretManager."
						);
				}
				if (key.getKeyId() > getCurrentKeyId())
				{
					setCurrentKeyId(key.getKeyId());
				}
				allKeys[key.getKeyId()] = key;
			}
		}

		public virtual org.apache.hadoop.security.token.delegation.DelegationKey[] getAllKeys
			()
		{
			lock (this)
			{
				return Sharpen.Collections.ToArray(allKeys.Values, new org.apache.hadoop.security.token.delegation.DelegationKey
					[0]);
			}
		}

		// HDFS
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void logUpdateMasterKey(org.apache.hadoop.security.token.delegation.DelegationKey
			 key)
		{
			return;
		}

		// HDFS
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void logExpireToken(TokenIdent ident)
		{
			return;
		}

		// RM
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void storeNewMasterKey(org.apache.hadoop.security.token.delegation.DelegationKey
			 key)
		{
			return;
		}

		// RM
		protected internal virtual void removeStoredMasterKey(org.apache.hadoop.security.token.delegation.DelegationKey
			 key)
		{
			return;
		}

		// RM
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void storeNewToken(TokenIdent ident, long renewDate)
		{
			return;
		}

		// RM
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void removeStoredToken(TokenIdent ident)
		{
		}

		// RM
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void updateStoredToken(TokenIdent ident, long renewDate
			)
		{
			return;
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		protected internal virtual int getCurrentKeyId()
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
		protected internal virtual int incrementCurrentKeyId()
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
		protected internal virtual void setCurrentKeyId(int keyId)
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
		protected internal virtual int getDelegationTokenSeqNum()
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
		protected internal virtual int incrementDelegationTokenSeqNum()
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
		protected internal virtual void setDelegationTokenSeqNum(int seqNum)
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
		protected internal virtual org.apache.hadoop.security.token.delegation.DelegationKey
			 getDelegationKey(int keyId)
		{
			return allKeys[keyId];
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void storeDelegationKey(org.apache.hadoop.security.token.delegation.DelegationKey
			 key)
		{
			allKeys[key.getKeyId()] = key;
			storeNewMasterKey(key);
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void updateDelegationKey(org.apache.hadoop.security.token.delegation.DelegationKey
			 key)
		{
			allKeys[key.getKeyId()] = key;
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		protected internal virtual org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 getTokenInfo(TokenIdent ident)
		{
			return currentTokens[ident];
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void storeToken(TokenIdent ident, org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 tokenInfo)
		{
			currentTokens[ident] = tokenInfo;
			storeNewToken(ident, tokenInfo.getRenewDate());
		}

		/// <summary>
		/// For subclasses externalizing the storage, for example Zookeeper
		/// based implementations
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void updateToken(TokenIdent ident, org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 tokenInfo)
		{
			currentTokens[ident] = tokenInfo;
			updateStoredToken(ident, tokenInfo.getRenewDate());
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
		public virtual void addPersistedDelegationToken(TokenIdent identifier, long renewDate
			)
		{
			lock (this)
			{
				if (running)
				{
					// a safety check
					throw new System.IO.IOException("Can't add persisted delegation token to a running SecretManager."
						);
				}
				int keyId = identifier.getMasterKeyId();
				org.apache.hadoop.security.token.delegation.DelegationKey dKey = allKeys[keyId];
				if (dKey == null)
				{
					LOG.warn("No KEY found for persisted identifier " + identifier.ToString());
					return;
				}
				byte[] password = createPassword(identifier.getBytes(), dKey.getKey());
				if (identifier.getSequenceNumber() > getDelegationTokenSeqNum())
				{
					setDelegationTokenSeqNum(identifier.getSequenceNumber());
				}
				if (getTokenInfo(identifier) == null)
				{
					currentTokens[identifier] = new org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
						(renewDate, password, getTrackingIdIfEnabled(identifier));
				}
				else
				{
					throw new System.IO.IOException("Same delegation token being added twice.");
				}
			}
		}

		/// <summary>
		/// Update the current master key
		/// This is called once by startThreads before tokenRemoverThread is created,
		/// and only by tokenRemoverThread afterwards.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void updateCurrentKey()
		{
			LOG.info("Updating the current master key for generating delegation tokens");
			/* Create a new currentKey with an estimated expiry date. */
			int newCurrentId;
			lock (this)
			{
				newCurrentId = incrementCurrentKeyId();
			}
			org.apache.hadoop.security.token.delegation.DelegationKey newKey = new org.apache.hadoop.security.token.delegation.DelegationKey
				(newCurrentId, Sharpen.Runtime.currentTimeMillis() + keyUpdateInterval + tokenMaxLifetime
				, generateSecret());
			//Log must be invoked outside the lock on 'this'
			logUpdateMasterKey(newKey);
			lock (this)
			{
				currentKey = newKey;
				storeDelegationKey(currentKey);
			}
		}

		/// <summary>
		/// Update the current master key for generating delegation tokens
		/// It should be called only by tokenRemoverThread.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void rollMasterKey()
		{
			lock (this)
			{
				removeExpiredKeys();
				/* set final expiry date for retiring currentKey */
				currentKey.setExpiryDate(org.apache.hadoop.util.Time.now() + tokenMaxLifetime);
				/*
				* currentKey might have been removed by removeExpiredKeys(), if
				* updateMasterKey() isn't called at expected interval. Add it back to
				* allKeys just in case.
				*/
				updateDelegationKey(currentKey);
			}
			updateCurrentKey();
		}

		private void removeExpiredKeys()
		{
			lock (this)
			{
				long now = org.apache.hadoop.util.Time.now();
				for (System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair
					<int, org.apache.hadoop.security.token.delegation.DelegationKey>> it = allKeys.GetEnumerator
					(); it.MoveNext(); )
				{
					System.Collections.Generic.KeyValuePair<int, org.apache.hadoop.security.token.delegation.DelegationKey
						> e = it.Current;
					if (e.Value.getExpiryDate() < now)
					{
						it.remove();
						// ensure the tokens generated by this current key can be recovered
						// with this current key after this current key is rolled
						if (!e.Value.Equals(currentKey))
						{
							removeStoredMasterKey(e.Value);
						}
					}
				}
			}
		}

		protected internal override byte[] createPassword(TokenIdent identifier)
		{
			lock (this)
			{
				int sequenceNum;
				long now = org.apache.hadoop.util.Time.now();
				sequenceNum = incrementDelegationTokenSeqNum();
				identifier.setIssueDate(now);
				identifier.setMaxDate(now + tokenMaxLifetime);
				identifier.setMasterKeyId(currentKey.getKeyId());
				identifier.setSequenceNumber(sequenceNum);
				LOG.info("Creating password for identifier: " + identifier + ", currentKey: " + currentKey
					.getKeyId());
				byte[] password = createPassword(identifier.getBytes(), currentKey.getKey());
				org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
					 tokenInfo = new org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
					(now + tokenRenewInterval, password, getTrackingIdIfEnabled(identifier));
				try
				{
					storeToken(identifier, tokenInfo);
				}
				catch (System.IO.IOException ioe)
				{
					LOG.error("Could not store token !!", ioe);
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
		/// <exception cref="org.apache.hadoop.security.token.SecretManager.InvalidToken"/>
		protected internal virtual org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 checkToken(TokenIdent identifier)
		{
			System.Diagnostics.Debug.Assert(java.lang.Thread.holdsLock(this));
			org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
				 info = getTokenInfo(identifier);
			if (info == null)
			{
				throw new org.apache.hadoop.security.token.SecretManager.InvalidToken("token (" +
					 identifier.ToString() + ") can't be found in cache");
			}
			if (info.getRenewDate() < org.apache.hadoop.util.Time.now())
			{
				throw new org.apache.hadoop.security.token.SecretManager.InvalidToken("token (" +
					 identifier.ToString() + ") is expired");
			}
			return info;
		}

		/// <exception cref="org.apache.hadoop.security.token.SecretManager.InvalidToken"/>
		public override byte[] retrievePassword(TokenIdent identifier)
		{
			lock (this)
			{
				return checkToken(identifier).getPassword();
			}
		}

		protected internal virtual string getTrackingIdIfEnabled(TokenIdent ident)
		{
			if (storeTokenTrackingId)
			{
				return ident.getTrackingId();
			}
			return null;
		}

		public virtual string getTokenTrackingId(TokenIdent identifier)
		{
			lock (this)
			{
				org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
					 info = getTokenInfo(identifier);
				if (info == null)
				{
					return null;
				}
				return info.getTrackingId();
			}
		}

		/// <summary>Verifies that the given identifier and password are valid and match.</summary>
		/// <param name="identifier">Token identifier.</param>
		/// <param name="password">Password in the token.</param>
		/// <exception cref="org.apache.hadoop.security.token.SecretManager.InvalidToken"/>
		public virtual void verifyToken(TokenIdent identifier, byte[] password)
		{
			lock (this)
			{
				byte[] storedPassword = retrievePassword(identifier);
				if (!java.util.Arrays.equals(password, storedPassword))
				{
					throw new org.apache.hadoop.security.token.SecretManager.InvalidToken("token (" +
						 identifier + ") is invalid, password doesn't match");
				}
			}
		}

		/// <summary>Renew a delegation token.</summary>
		/// <param name="token">the token to renew</param>
		/// <param name="renewer">the full principal name of the user doing the renewal</param>
		/// <returns>the new expiration time</returns>
		/// <exception cref="org.apache.hadoop.security.token.SecretManager.InvalidToken">if the token is invalid
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">if the user can't renew token
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual long renewToken(org.apache.hadoop.security.token.Token<TokenIdent>
			 token, string renewer)
		{
			lock (this)
			{
				java.io.ByteArrayInputStream buf = new java.io.ByteArrayInputStream(token.getIdentifier
					());
				java.io.DataInputStream @in = new java.io.DataInputStream(buf);
				TokenIdent id = createIdentifier();
				id.readFields(@in);
				LOG.info("Token renewal for identifier: " + id + "; total currentTokens " + currentTokens
					.Count);
				long now = org.apache.hadoop.util.Time.now();
				if (id.getMaxDate() < now)
				{
					throw new org.apache.hadoop.security.token.SecretManager.InvalidToken(renewer + " tried to renew an expired token"
						);
				}
				if ((id.getRenewer() == null) || (id.getRenewer().ToString().isEmpty()))
				{
					throw new org.apache.hadoop.security.AccessControlException(renewer + " tried to renew a token without a renewer"
						);
				}
				if (!id.getRenewer().ToString().Equals(renewer))
				{
					throw new org.apache.hadoop.security.AccessControlException(renewer + " tries to renew a token with renewer "
						 + id.getRenewer());
				}
				org.apache.hadoop.security.token.delegation.DelegationKey key = getDelegationKey(
					id.getMasterKeyId());
				if (key == null)
				{
					throw new org.apache.hadoop.security.token.SecretManager.InvalidToken("Unable to find master key for keyId="
						 + id.getMasterKeyId() + " from cache. Failed to renew an unexpired token" + " with sequenceNumber="
						 + id.getSequenceNumber());
				}
				byte[] password = createPassword(token.getIdentifier(), key.getKey());
				if (!java.util.Arrays.equals(password, token.getPassword()))
				{
					throw new org.apache.hadoop.security.AccessControlException(renewer + " is trying to renew a token with wrong password"
						);
				}
				long renewTime = System.Math.min(id.getMaxDate(), now + tokenRenewInterval);
				string trackingId = getTrackingIdIfEnabled(id);
				org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
					 info = new org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
					(renewTime, password, trackingId);
				if (getTokenInfo(id) == null)
				{
					throw new org.apache.hadoop.security.token.SecretManager.InvalidToken("Renewal request for unknown token"
						);
				}
				updateToken(id, info);
				return renewTime;
			}
		}

		/// <summary>Cancel a token by removing it from cache.</summary>
		/// <returns>Identifier of the canceled token</returns>
		/// <exception cref="org.apache.hadoop.security.token.SecretManager.InvalidToken">for invalid token
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">if the user isn't allowed to cancel
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual TokenIdent cancelToken(org.apache.hadoop.security.token.Token<TokenIdent
			> token, string canceller)
		{
			lock (this)
			{
				java.io.ByteArrayInputStream buf = new java.io.ByteArrayInputStream(token.getIdentifier
					());
				java.io.DataInputStream @in = new java.io.DataInputStream(buf);
				TokenIdent id = createIdentifier();
				id.readFields(@in);
				LOG.info("Token cancelation requested for identifier: " + id);
				if (id.getUser() == null)
				{
					throw new org.apache.hadoop.security.token.SecretManager.InvalidToken("Token with no owner"
						);
				}
				string owner = id.getUser().getUserName();
				org.apache.hadoop.io.Text renewer = id.getRenewer();
				org.apache.hadoop.security.HadoopKerberosName cancelerKrbName = new org.apache.hadoop.security.HadoopKerberosName
					(canceller);
				string cancelerShortName = cancelerKrbName.getShortName();
				if (!canceller.Equals(owner) && (renewer == null || renewer.ToString().isEmpty() 
					|| !cancelerShortName.Equals(renewer.ToString())))
				{
					throw new org.apache.hadoop.security.AccessControlException(canceller + " is not authorized to cancel the token"
						);
				}
				org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
					 info = Sharpen.Collections.Remove(currentTokens, id);
				if (info == null)
				{
					throw new org.apache.hadoop.security.token.SecretManager.InvalidToken("Token not found"
						);
				}
				removeStoredToken(id);
				return id;
			}
		}

		/// <summary>Convert the byte[] to a secret key</summary>
		/// <param name="key">the byte[] to create the secret key from</param>
		/// <returns>the secret key</returns>
		protected internal static javax.crypto.SecretKey createSecretKey(byte[] key)
		{
			return org.apache.hadoop.security.token.SecretManager.createSecretKey(key);
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
			public virtual long getRenewDate()
			{
				return renewDate;
			}

			/// <summary>returns password</summary>
			internal virtual byte[] getPassword()
			{
				return password;
			}

			/// <summary>returns tracking id</summary>
			public virtual string getTrackingId()
			{
				return trackingId;
			}
		}

		/// <summary>Remove expired delegation tokens from cache</summary>
		/// <exception cref="System.IO.IOException"/>
		private void removeExpiredToken()
		{
			long now = org.apache.hadoop.util.Time.now();
			System.Collections.Generic.ICollection<TokenIdent> expiredTokens = new java.util.HashSet
				<TokenIdent>();
			lock (this)
			{
				System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<TokenIdent
					, org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
					>> i = currentTokens.GetEnumerator();
				while (i.MoveNext())
				{
					System.Collections.Generic.KeyValuePair<TokenIdent, org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
						> entry = i.Current;
					long renewDate = entry.Value.getRenewDate();
					if (renewDate < now)
					{
						expiredTokens.add(entry.Key);
						i.remove();
					}
				}
			}
			// don't hold lock on 'this' to avoid edit log updates blocking token ops
			foreach (TokenIdent ident in expiredTokens)
			{
				logExpireToken(ident);
				removeStoredToken(ident);
			}
		}

		public virtual void stopThreads()
		{
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Stopping expired delegation token remover thread");
			}
			running = false;
			if (tokenRemoverThread != null)
			{
				lock (noInterruptsLock)
				{
					tokenRemoverThread.interrupt();
				}
				try
				{
					tokenRemoverThread.join();
				}
				catch (System.Exception e)
				{
					throw new System.Exception("Unable to join on token removal thread", e);
				}
			}
		}

		/// <summary>is secretMgr running</summary>
		/// <returns>true if secret mgr is running</returns>
		public virtual bool isRunning()
		{
			lock (this)
			{
				return running;
			}
		}

		private class ExpiredTokenRemover : java.lang.Thread
		{
			private long lastMasterKeyUpdate;

			private long lastTokenCacheCleanup;

			public override void run()
			{
				org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.
					LOG.info("Starting expired delegation token remover thread, " + "tokenRemoverScanInterval="
					 + this._enclosing.tokenRemoverScanInterval / (60 * 1000) + " min(s)");
				try
				{
					while (this._enclosing.running)
					{
						long now = org.apache.hadoop.util.Time.now();
						if (this.lastMasterKeyUpdate + this._enclosing.keyUpdateInterval < now)
						{
							try
							{
								this._enclosing.rollMasterKey();
								this.lastMasterKeyUpdate = now;
							}
							catch (System.IO.IOException e)
							{
								org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.
									LOG.error("Master key updating failed: ", e);
							}
						}
						if (this.lastTokenCacheCleanup + this._enclosing.tokenRemoverScanInterval < now)
						{
							this._enclosing.removeExpiredToken();
							this.lastTokenCacheCleanup = now;
						}
						try
						{
							java.lang.Thread.sleep(System.Math.min(5000, this._enclosing.keyUpdateInterval));
						}
						catch (System.Exception ie)
						{
							// 5 seconds
							org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.
								LOG.error("ExpiredTokenRemover received " + ie);
						}
					}
				}
				catch (System.Exception t)
				{
					org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.
						LOG.error("ExpiredTokenRemover thread received unexpected exception", t);
					java.lang.Runtime.getRuntime().exit(-1);
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
		public virtual TokenIdent decodeTokenIdentifier(org.apache.hadoop.security.token.Token
			<TokenIdent> token)
		{
			return token.decodeIdentifier();
		}
	}
}
