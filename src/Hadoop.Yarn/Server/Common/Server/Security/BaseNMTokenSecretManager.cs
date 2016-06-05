using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Security
{
	public class BaseNMTokenSecretManager : SecretManager<NMTokenIdentifier>
	{
		private static Log Log = LogFactory.GetLog(typeof(BaseNMTokenSecretManager));

		protected internal int serialNo = new SecureRandom().Next();

		protected internal readonly ReadWriteLock readWriteLock = new ReentrantReadWriteLock
			();

		protected internal readonly Lock readLock = readWriteLock.ReadLock();

		protected internal readonly Lock writeLock = readWriteLock.WriteLock();

		protected internal MasterKeyData currentMasterKey;

		protected internal virtual MasterKeyData CreateNewMasterKey()
		{
			this.writeLock.Lock();
			try
			{
				return new MasterKeyData(serialNo++, GenerateSecret());
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		[InterfaceAudience.Private]
		public virtual MasterKey GetCurrentKey()
		{
			this.readLock.Lock();
			try
			{
				return this.currentMasterKey.GetMasterKey();
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		protected override byte[] CreatePassword(NMTokenIdentifier identifier)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("creating password for " + identifier.GetApplicationAttemptId() + " for user "
					 + identifier.GetApplicationSubmitter() + " to run on NM " + identifier.GetNodeId
					());
			}
			readLock.Lock();
			try
			{
				return CreatePassword(identifier.GetBytes(), currentMasterKey.GetSecretKey());
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public override byte[] RetrievePassword(NMTokenIdentifier identifier)
		{
			readLock.Lock();
			try
			{
				return RetrivePasswordInternal(identifier, currentMasterKey);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		protected internal virtual byte[] RetrivePasswordInternal(NMTokenIdentifier identifier
			, MasterKeyData masterKey)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("creating password for " + identifier.GetApplicationAttemptId() + " for user "
					 + identifier.GetApplicationSubmitter() + " to run on NM " + identifier.GetNodeId
					());
			}
			return CreatePassword(identifier.GetBytes(), masterKey.GetSecretKey());
		}

		/// <summary>It is required for RPC</summary>
		public override NMTokenIdentifier CreateIdentifier()
		{
			return new NMTokenIdentifier();
		}

		/// <summary>Helper function for creating NMTokens.</summary>
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Token CreateNMToken(ApplicationAttemptId
			 applicationAttemptId, NodeId nodeId, string applicationSubmitter)
		{
			byte[] password;
			NMTokenIdentifier identifier;
			this.readLock.Lock();
			try
			{
				identifier = new NMTokenIdentifier(applicationAttemptId, nodeId, applicationSubmitter
					, this.currentMasterKey.GetMasterKey().GetKeyId());
				password = this.CreatePassword(identifier);
			}
			finally
			{
				this.readLock.Unlock();
			}
			return NewInstance(password, identifier);
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Token NewInstance(byte[] password
			, NMTokenIdentifier identifier)
		{
			NodeId nodeId = identifier.GetNodeId();
			// RPC layer client expects ip:port as service for tokens
			IPEndPoint addr = NetUtils.CreateSocketAddrForHost(nodeId.GetHost(), nodeId.GetPort
				());
			Org.Apache.Hadoop.Yarn.Api.Records.Token nmToken = Org.Apache.Hadoop.Yarn.Api.Records.Token
				.NewInstance(identifier.GetBytes(), NMTokenIdentifier.Kind.ToString(), password, 
				SecurityUtil.BuildTokenService(addr).ToString());
			return nmToken;
		}
	}
}
