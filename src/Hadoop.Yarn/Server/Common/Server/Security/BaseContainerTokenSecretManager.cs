using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Security
{
	/// <summary>SecretManager for ContainerTokens.</summary>
	/// <remarks>
	/// SecretManager for ContainerTokens. Extended by both RM and NM and hence is
	/// present in yarn-server-common package.
	/// </remarks>
	public class BaseContainerTokenSecretManager : SecretManager<ContainerTokenIdentifier
		>
	{
		private static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Security.BaseContainerTokenSecretManager
			));

		protected internal int serialNo = new SecureRandom().Next();

		protected internal readonly ReadWriteLock readWriteLock = new ReentrantReadWriteLock
			();

		protected internal readonly Lock readLock = readWriteLock.ReadLock();

		protected internal readonly Lock writeLock = readWriteLock.WriteLock();

		/// <summary>THE masterKey.</summary>
		/// <remarks>
		/// THE masterKey. ResourceManager should persist this and recover it on
		/// restart instead of generating a new key. The NodeManagers get it from the
		/// ResourceManager and use it for validating container-tokens.
		/// </remarks>
		protected internal MasterKeyData currentMasterKey;

		protected internal readonly long containerTokenExpiryInterval;

		public BaseContainerTokenSecretManager(Configuration conf)
		{
			this.containerTokenExpiryInterval = conf.GetInt(YarnConfiguration.RmContainerAllocExpiryIntervalMs
				, YarnConfiguration.DefaultRmContainerAllocExpiryIntervalMs);
		}

		// Need lock as we increment serialNo etc.
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

		protected override byte[] CreatePassword(ContainerTokenIdentifier identifier)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Creating password for " + identifier.GetContainerID() + " for user " +
					 identifier.GetUser() + " to be run on NM " + identifier.GetNmHostAddress());
			}
			this.readLock.Lock();
			try
			{
				return CreatePassword(identifier.GetBytes(), this.currentMasterKey.GetSecretKey()
					);
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public override byte[] RetrievePassword(ContainerTokenIdentifier identifier)
		{
			this.readLock.Lock();
			try
			{
				return RetrievePasswordInternal(identifier, this.currentMasterKey);
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		protected internal virtual byte[] RetrievePasswordInternal(ContainerTokenIdentifier
			 identifier, MasterKeyData masterKey)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Retrieving password for " + identifier.GetContainerID() + " for user "
					 + identifier.GetUser() + " to be run on NM " + identifier.GetNmHostAddress());
			}
			return CreatePassword(identifier.GetBytes(), masterKey.GetSecretKey());
		}

		/// <summary>Used by the RPC layer.</summary>
		public override ContainerTokenIdentifier CreateIdentifier()
		{
			return new ContainerTokenIdentifier();
		}
	}
}
