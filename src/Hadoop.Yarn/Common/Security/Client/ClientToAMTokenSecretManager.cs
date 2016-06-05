using System;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security.Client
{
	/// <summary>
	/// A simple
	/// <see cref="Org.Apache.Hadoop.Security.Token.SecretManager{T}"/>
	/// for AMs to validate Client-RM tokens issued to
	/// clients by the RM using the underlying master-key shared by RM to the AMs on
	/// their launch. All the methods are called by either Hadoop RPC or YARN, so
	/// this class is strictly for the purpose of inherit/extend and register with
	/// Hadoop RPC.
	/// </summary>
	public class ClientToAMTokenSecretManager : BaseClientToAMTokenSecretManager
	{
		private const int MasterKeyWaitMsec = 10 * 1000;

		private volatile SecretKey masterKey;

		public ClientToAMTokenSecretManager(ApplicationAttemptId applicationAttemptID, byte
			[] key)
			: base()
		{
			// Only one master-key for AM
			if (key != null)
			{
				this.masterKey = SecretManager.CreateSecretKey(key);
			}
			else
			{
				this.masterKey = null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public override byte[] RetrievePassword(ClientToAMTokenIdentifier identifier)
		{
			if (this.masterKey == null)
			{
				lock (this)
				{
					while (masterKey == null)
					{
						try
						{
							Sharpen.Runtime.Wait(this, MasterKeyWaitMsec);
							break;
						}
						catch (Exception)
						{
						}
					}
				}
			}
			return base.RetrievePassword(identifier);
		}

		public override SecretKey GetMasterKey(ApplicationAttemptId applicationAttemptID)
		{
			// Only one master-key for AM, just return that.
			return this.masterKey;
		}

		public virtual void SetMasterKey(byte[] key)
		{
			lock (this)
			{
				this.masterKey = SecretManager.CreateSecretKey(key);
				Sharpen.Runtime.NotifyAll(this);
			}
		}
	}
}
