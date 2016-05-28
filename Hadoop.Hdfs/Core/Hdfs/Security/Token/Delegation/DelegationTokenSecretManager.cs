using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security.Token.Delegation
{
	/// <summary>A HDFS specific delegation token secret manager.</summary>
	/// <remarks>
	/// A HDFS specific delegation token secret manager.
	/// The secret manager is responsible for generating and accepting the password
	/// for each token.
	/// </remarks>
	public class DelegationTokenSecretManager : AbstractDelegationTokenSecretManager<
		DelegationTokenIdentifier>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Security.Token.Delegation.DelegationTokenSecretManager
			));

		private readonly FSNamesystem namesystem;

		private readonly DelegationTokenSecretManager.SerializerCompat serializerCompat;

		public DelegationTokenSecretManager(long delegationKeyUpdateInterval, long delegationTokenMaxLifetime
			, long delegationTokenRenewInterval, long delegationTokenRemoverScanInterval, FSNamesystem
			 namesystem)
			: this(delegationKeyUpdateInterval, delegationTokenMaxLifetime, delegationTokenRenewInterval
				, delegationTokenRemoverScanInterval, false, namesystem)
		{
			serializerCompat = new DelegationTokenSecretManager.SerializerCompat(this);
		}

		/// <summary>Create a secret manager</summary>
		/// <param name="delegationKeyUpdateInterval">
		/// the number of seconds for rolling new
		/// secret keys.
		/// </param>
		/// <param name="delegationTokenMaxLifetime">
		/// the maximum lifetime of the delegation
		/// tokens
		/// </param>
		/// <param name="delegationTokenRenewInterval">how often the tokens must be renewed</param>
		/// <param name="delegationTokenRemoverScanInterval">
		/// how often the tokens are scanned
		/// for expired tokens
		/// </param>
		/// <param name="storeTokenTrackingId">whether to store the token's tracking id</param>
		public DelegationTokenSecretManager(long delegationKeyUpdateInterval, long delegationTokenMaxLifetime
			, long delegationTokenRenewInterval, long delegationTokenRemoverScanInterval, bool
			 storeTokenTrackingId, FSNamesystem namesystem)
			: base(delegationKeyUpdateInterval, delegationTokenMaxLifetime, delegationTokenRenewInterval
				, delegationTokenRemoverScanInterval)
		{
			serializerCompat = new DelegationTokenSecretManager.SerializerCompat(this);
			this.namesystem = namesystem;
			this.storeTokenTrackingId = storeTokenTrackingId;
		}

		public override DelegationTokenIdentifier CreateIdentifier()
		{
			//SecretManager
			return new DelegationTokenIdentifier();
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public override byte[] RetrievePassword(DelegationTokenIdentifier identifier)
		{
			try
			{
				// this check introduces inconsistency in the authentication to a
				// HA standby NN.  non-token auths are allowed into the namespace which
				// decides whether to throw a StandbyException.  tokens are a bit
				// different in that a standby may be behind and thus not yet know
				// of all tokens issued by the active NN.  the following check does
				// not allow ANY token auth, however it should allow known tokens in
				namesystem.CheckOperation(NameNode.OperationCategory.Read);
			}
			catch (StandbyException se)
			{
				// FIXME: this is a hack to get around changing method signatures by
				// tunneling a non-InvalidToken exception as the cause which the
				// RPC server will unwrap before returning to the client
				SecretManager.InvalidToken wrappedStandby = new SecretManager.InvalidToken("StandbyException"
					);
				Sharpen.Extensions.InitCause(wrappedStandby, se);
				throw wrappedStandby;
			}
			return base.RetrievePassword(identifier);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.RetriableException"/>
		/// <exception cref="System.IO.IOException"/>
		public override byte[] RetriableRetrievePassword(DelegationTokenIdentifier identifier
			)
		{
			namesystem.CheckOperation(NameNode.OperationCategory.Read);
			try
			{
				return base.RetrievePassword(identifier);
			}
			catch (SecretManager.InvalidToken it)
			{
				if (namesystem.InTransitionToActive())
				{
					// if the namesystem is currently in the middle of transition to 
					// active state, let client retry since the corresponding editlog may 
					// have not been applied yet
					throw new RetriableException(it);
				}
				else
				{
					throw;
				}
			}
		}

		/// <summary>Returns expiry time of a token given its identifier.</summary>
		/// <param name="dtId">DelegationTokenIdentifier of a token</param>
		/// <returns>Expiry time of the token</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetTokenExpiryTime(DelegationTokenIdentifier dtId)
		{
			lock (this)
			{
				AbstractDelegationTokenSecretManager.DelegationTokenInformation info = currentTokens
					[dtId];
				if (info != null)
				{
					return info.GetRenewDate();
				}
				else
				{
					throw new IOException("No delegation token found for this identifier");
				}
			}
		}

		/// <summary>Load SecretManager state from fsimage.</summary>
		/// <param name="in">input stream to read fsimage</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void LoadSecretManagerStateCompat(DataInput @in)
		{
			lock (this)
			{
				if (running)
				{
					// a safety check
					throw new IOException("Can't load state from image in a running SecretManager.");
				}
				serializerCompat.Load(@in);
			}
		}

		public class SecretManagerState
		{
			public readonly FsImageProto.SecretManagerSection section;

			public readonly IList<FsImageProto.SecretManagerSection.DelegationKey> keys;

			public readonly IList<FsImageProto.SecretManagerSection.PersistToken> tokens;

			public SecretManagerState(FsImageProto.SecretManagerSection s, IList<FsImageProto.SecretManagerSection.DelegationKey
				> keys, IList<FsImageProto.SecretManagerSection.PersistToken> tokens)
			{
				this.section = s;
				this.keys = keys;
				this.tokens = tokens;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void LoadSecretManagerState(DelegationTokenSecretManager.SecretManagerState
			 state)
		{
			lock (this)
			{
				Preconditions.CheckState(!running, "Can't load state from image in a running SecretManager."
					);
				currentId = state.section.GetCurrentId();
				delegationTokenSequenceNumber = state.section.GetTokenSequenceNumber();
				foreach (FsImageProto.SecretManagerSection.DelegationKey k in state.keys)
				{
					AddKey(new DelegationKey(k.GetId(), k.GetExpiryDate(), k.HasKey() ? k.GetKey().ToByteArray
						() : null));
				}
				foreach (FsImageProto.SecretManagerSection.PersistToken t in state.tokens)
				{
					DelegationTokenIdentifier id = new DelegationTokenIdentifier(new Text(t.GetOwner(
						)), new Text(t.GetRenewer()), new Text(t.GetRealUser()));
					id.SetIssueDate(t.GetIssueDate());
					id.SetMaxDate(t.GetMaxDate());
					id.SetSequenceNumber(t.GetSequenceNumber());
					id.SetMasterKeyId(t.GetMasterKeyId());
					AddPersistedDelegationToken(id, t.GetExpiryDate());
				}
			}
		}

		/// <summary>Store the current state of the SecretManager for persistence</summary>
		/// <param name="out">Output stream for writing into fsimage.</param>
		/// <param name="sdPath">String storage directory path</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SaveSecretManagerStateCompat(DataOutputStream @out, string sdPath
			)
		{
			lock (this)
			{
				serializerCompat.Save(@out, sdPath);
			}
		}

		public virtual DelegationTokenSecretManager.SecretManagerState SaveSecretManagerState
			()
		{
			lock (this)
			{
				FsImageProto.SecretManagerSection s = ((FsImageProto.SecretManagerSection)FsImageProto.SecretManagerSection
					.NewBuilder().SetCurrentId(currentId).SetTokenSequenceNumber(delegationTokenSequenceNumber
					).SetNumKeys(allKeys.Count).SetNumTokens(currentTokens.Count).Build());
				AList<FsImageProto.SecretManagerSection.DelegationKey> keys = Lists.NewArrayListWithCapacity
					(allKeys.Count);
				AList<FsImageProto.SecretManagerSection.PersistToken> tokens = Lists.NewArrayListWithCapacity
					(currentTokens.Count);
				foreach (DelegationKey v in allKeys.Values)
				{
					FsImageProto.SecretManagerSection.DelegationKey.Builder b = FsImageProto.SecretManagerSection.DelegationKey
						.NewBuilder().SetId(v.GetKeyId()).SetExpiryDate(v.GetExpiryDate());
					if (v.GetEncodedKey() != null)
					{
						b.SetKey(ByteString.CopyFrom(v.GetEncodedKey()));
					}
					keys.AddItem(((FsImageProto.SecretManagerSection.DelegationKey)b.Build()));
				}
				foreach (KeyValuePair<DelegationTokenIdentifier, AbstractDelegationTokenSecretManager.DelegationTokenInformation
					> e in currentTokens)
				{
					DelegationTokenIdentifier id = e.Key;
					FsImageProto.SecretManagerSection.PersistToken.Builder b = FsImageProto.SecretManagerSection.PersistToken
						.NewBuilder().SetOwner(id.GetOwner().ToString()).SetRenewer(id.GetRenewer().ToString
						()).SetRealUser(id.GetRealUser().ToString()).SetIssueDate(id.GetIssueDate()).SetMaxDate
						(id.GetMaxDate()).SetSequenceNumber(id.GetSequenceNumber()).SetMasterKeyId(id.GetMasterKeyId
						()).SetExpiryDate(e.Value.GetRenewDate());
					tokens.AddItem(((FsImageProto.SecretManagerSection.PersistToken)b.Build()));
				}
				return new DelegationTokenSecretManager.SecretManagerState(s, keys, tokens);
			}
		}

		/// <summary>This method is intended to be used only while reading edit logs.</summary>
		/// <param name="identifier">
		/// DelegationTokenIdentifier read from the edit logs or
		/// fsimage
		/// </param>
		/// <param name="expiryTime">token expiry time</param>
		/// <exception cref="System.IO.IOException"/>
		public override void AddPersistedDelegationToken(DelegationTokenIdentifier identifier
			, long expiryTime)
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
				if (identifier.GetSequenceNumber() > this.delegationTokenSequenceNumber)
				{
					this.delegationTokenSequenceNumber = identifier.GetSequenceNumber();
				}
				if (currentTokens[identifier] == null)
				{
					currentTokens[identifier] = new AbstractDelegationTokenSecretManager.DelegationTokenInformation
						(expiryTime, password, GetTrackingIdIfEnabled(identifier));
				}
				else
				{
					throw new IOException("Same delegation token being added twice; invalid entry in fsimage or editlogs"
						);
				}
			}
		}

		/// <summary>Add a MasterKey to the list of keys.</summary>
		/// <param name="key">DelegationKey</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void UpdatePersistedMasterKey(DelegationKey key)
		{
			lock (this)
			{
				AddKey(key);
			}
		}

		/// <summary>Update the token cache with renewal record in edit logs.</summary>
		/// <param name="identifier">DelegationTokenIdentifier of the renewed token</param>
		/// <param name="expiryTime">expirty time in milliseconds</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void UpdatePersistedTokenRenewal(DelegationTokenIdentifier identifier
			, long expiryTime)
		{
			lock (this)
			{
				if (running)
				{
					// a safety check
					throw new IOException("Can't update persisted delegation token renewal to a running SecretManager."
						);
				}
				AbstractDelegationTokenSecretManager.DelegationTokenInformation info = null;
				info = currentTokens[identifier];
				if (info != null)
				{
					int keyId = identifier.GetMasterKeyId();
					byte[] password = CreatePassword(identifier.GetBytes(), allKeys[keyId].GetKey());
					currentTokens[identifier] = new AbstractDelegationTokenSecretManager.DelegationTokenInformation
						(expiryTime, password, GetTrackingIdIfEnabled(identifier));
				}
			}
		}

		/// <summary>Update the token cache with the cancel record in edit logs</summary>
		/// <param name="identifier">DelegationTokenIdentifier of the canceled token</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void UpdatePersistedTokenCancellation(DelegationTokenIdentifier identifier
			)
		{
			lock (this)
			{
				if (running)
				{
					// a safety check
					throw new IOException("Can't update persisted delegation token renewal to a running SecretManager."
						);
				}
				Sharpen.Collections.Remove(currentTokens, identifier);
			}
		}

		/// <summary>Returns the number of delegation keys currently stored.</summary>
		/// <returns>number of delegation keys</returns>
		public virtual int GetNumberOfKeys()
		{
			lock (this)
			{
				return allKeys.Count;
			}
		}

		/// <summary>Call namesystem to update editlogs for new master key.</summary>
		/// <exception cref="System.IO.IOException"/>
		protected override void LogUpdateMasterKey(DelegationKey key)
		{
			//AbstractDelegationTokenManager
			lock (noInterruptsLock)
			{
				// The edit logging code will fail catastrophically if it
				// is interrupted during a logSync, since the interrupt
				// closes the edit log files. Doing this inside the
				// above lock and then checking interruption status
				// prevents this bug.
				if (Sharpen.Thread.Interrupted())
				{
					throw new ThreadInterruptedException("Interrupted before updating master key");
				}
				namesystem.LogUpdateMasterKey(key);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void LogExpireToken(DelegationTokenIdentifier dtId)
		{
			//AbstractDelegationTokenManager
			lock (noInterruptsLock)
			{
				// The edit logging code will fail catastrophically if it
				// is interrupted during a logSync, since the interrupt
				// closes the edit log files. Doing this inside the
				// above lock and then checking interruption status
				// prevents this bug.
				if (Sharpen.Thread.Interrupted())
				{
					throw new ThreadInterruptedException("Interrupted before expiring delegation token"
						);
				}
				namesystem.LogExpireDelegationToken(dtId);
			}
		}

		/// <summary>A utility method for creating credentials.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static Credentials CreateCredentials(NameNode namenode, UserGroupInformation
			 ugi, string renewer)
		{
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = namenode
				.GetRpcServer().GetDelegationToken(new Text(renewer));
			if (token == null)
			{
				return null;
			}
			IPEndPoint addr = namenode.GetNameNodeAddress();
			SecurityUtil.SetTokenService(token, addr);
			Credentials c = new Credentials();
			c.AddToken(new Text(ugi.GetShortUserName()), token);
			return c;
		}

		private sealed class SerializerCompat
		{
			/// <exception cref="System.IO.IOException"/>
			private void Load(DataInput @in)
			{
				this._enclosing.currentId = @in.ReadInt();
				this.LoadAllKeys(@in);
				this._enclosing.delegationTokenSequenceNumber = @in.ReadInt();
				this.LoadCurrentTokens(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			private void Save(DataOutputStream @out, string sdPath)
			{
				@out.WriteInt(this._enclosing.currentId);
				this.SaveAllKeys(@out, sdPath);
				@out.WriteInt(this._enclosing.delegationTokenSequenceNumber);
				this.SaveCurrentTokens(@out, sdPath);
			}

			/// <summary>Private helper methods to save delegation keys and tokens in fsimage</summary>
			/// <exception cref="System.IO.IOException"/>
			private void SaveCurrentTokens(DataOutputStream @out, string sdPath)
			{
				lock (this)
				{
					StartupProgress prog = NameNode.GetStartupProgress();
					Step step = new Step(StepType.DelegationTokens, sdPath);
					prog.BeginStep(Phase.SavingCheckpoint, step);
					prog.SetTotal(Phase.SavingCheckpoint, step, this._enclosing.currentTokens.Count);
					StartupProgress.Counter counter = prog.GetCounter(Phase.SavingCheckpoint, step);
					@out.WriteInt(this._enclosing.currentTokens.Count);
					IEnumerator<DelegationTokenIdentifier> iter = this._enclosing.currentTokens.Keys.
						GetEnumerator();
					while (iter.HasNext())
					{
						DelegationTokenIdentifier id = iter.Next();
						id.Write(@out);
						AbstractDelegationTokenSecretManager.DelegationTokenInformation info = this._enclosing
							.currentTokens[id];
						@out.WriteLong(info.GetRenewDate());
						counter.Increment();
					}
					prog.EndStep(Phase.SavingCheckpoint, step);
				}
			}

			/*
			* Save the current state of allKeys
			*/
			/// <exception cref="System.IO.IOException"/>
			private void SaveAllKeys(DataOutputStream @out, string sdPath)
			{
				lock (this)
				{
					StartupProgress prog = NameNode.GetStartupProgress();
					Step step = new Step(StepType.DelegationKeys, sdPath);
					prog.BeginStep(Phase.SavingCheckpoint, step);
					prog.SetTotal(Phase.SavingCheckpoint, step, this._enclosing.currentTokens.Count);
					StartupProgress.Counter counter = prog.GetCounter(Phase.SavingCheckpoint, step);
					@out.WriteInt(this._enclosing.allKeys.Count);
					IEnumerator<int> iter = this._enclosing.allKeys.Keys.GetEnumerator();
					while (iter.HasNext())
					{
						int key = iter.Next();
						this._enclosing.allKeys[key].Write(@out);
						counter.Increment();
					}
					prog.EndStep(Phase.SavingCheckpoint, step);
				}
			}

			/// <summary>Private helper methods to load Delegation tokens from fsimage</summary>
			/// <exception cref="System.IO.IOException"/>
			private void LoadCurrentTokens(DataInput @in)
			{
				lock (this)
				{
					StartupProgress prog = NameNode.GetStartupProgress();
					Step step = new Step(StepType.DelegationTokens);
					prog.BeginStep(Phase.LoadingFsimage, step);
					int numberOfTokens = @in.ReadInt();
					prog.SetTotal(Phase.LoadingFsimage, step, numberOfTokens);
					StartupProgress.Counter counter = prog.GetCounter(Phase.LoadingFsimage, step);
					for (int i = 0; i < numberOfTokens; i++)
					{
						DelegationTokenIdentifier id = new DelegationTokenIdentifier();
						id.ReadFields(@in);
						long expiryTime = @in.ReadLong();
						this._enclosing.AddPersistedDelegationToken(id, expiryTime);
						counter.Increment();
					}
					prog.EndStep(Phase.LoadingFsimage, step);
				}
			}

			/// <summary>Private helper method to load delegation keys from fsimage.</summary>
			/// <exception cref="System.IO.IOException">on error</exception>
			private void LoadAllKeys(DataInput @in)
			{
				lock (this)
				{
					StartupProgress prog = NameNode.GetStartupProgress();
					Step step = new Step(StepType.DelegationKeys);
					prog.BeginStep(Phase.LoadingFsimage, step);
					int numberOfKeys = @in.ReadInt();
					prog.SetTotal(Phase.LoadingFsimage, step, numberOfKeys);
					StartupProgress.Counter counter = prog.GetCounter(Phase.LoadingFsimage, step);
					for (int i = 0; i < numberOfKeys; i++)
					{
						DelegationKey value = new DelegationKey();
						value.ReadFields(@in);
						this._enclosing.AddKey(value);
						counter.Increment();
					}
					prog.EndStep(Phase.LoadingFsimage, step);
				}
			}

			internal SerializerCompat(DelegationTokenSecretManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly DelegationTokenSecretManager _enclosing;
		}
	}
}
