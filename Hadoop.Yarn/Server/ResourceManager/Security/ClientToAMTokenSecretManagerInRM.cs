using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security
{
	public class ClientToAMTokenSecretManagerInRM : BaseClientToAMTokenSecretManager
	{
		private IDictionary<ApplicationAttemptId, SecretKey> masterKeys = new Dictionary<
			ApplicationAttemptId, SecretKey>();

		// Per application master-keys for managing client-tokens
		public virtual SecretKey CreateMasterKey(ApplicationAttemptId applicationAttemptID
			)
		{
			lock (this)
			{
				return GenerateSecret();
			}
		}

		public virtual void RegisterApplication(ApplicationAttemptId applicationAttemptID
			, SecretKey key)
		{
			lock (this)
			{
				this.masterKeys[applicationAttemptID] = key;
			}
		}

		// Only for RM recovery
		public virtual SecretKey RegisterMasterKey(ApplicationAttemptId applicationAttemptID
			, byte[] keyData)
		{
			lock (this)
			{
				SecretKey key = CreateSecretKey(keyData);
				RegisterApplication(applicationAttemptID, key);
				return key;
			}
		}

		public virtual void UnRegisterApplication(ApplicationAttemptId applicationAttemptID
			)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(this.masterKeys, applicationAttemptID);
			}
		}

		public override SecretKey GetMasterKey(ApplicationAttemptId applicationAttemptID)
		{
			lock (this)
			{
				return this.masterKeys[applicationAttemptID];
			}
		}

		[VisibleForTesting]
		public virtual bool HasMasterKey(ApplicationAttemptId applicationAttemptID)
		{
			lock (this)
			{
				return this.masterKeys.Contains(applicationAttemptID);
			}
		}
	}
}
