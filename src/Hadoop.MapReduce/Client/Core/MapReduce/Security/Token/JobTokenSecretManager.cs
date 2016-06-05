using System.Collections.Generic;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security.Token
{
	/// <summary>SecretManager for job token.</summary>
	/// <remarks>SecretManager for job token. It can be used to cache generated job tokens.
	/// 	</remarks>
	public class JobTokenSecretManager : SecretManager<JobTokenIdentifier>
	{
		private readonly SecretKey masterKey;

		private readonly IDictionary<string, SecretKey> currentJobTokens;

		/// <summary>Convert the byte[] to a secret key</summary>
		/// <param name="key">the byte[] to create the secret key from</param>
		/// <returns>the secret key</returns>
		protected static SecretKey CreateSecretKey(byte[] key)
		{
			return SecretManager.CreateSecretKey(key);
		}

		/// <summary>Compute the HMAC hash of the message using the key</summary>
		/// <param name="msg">the message to hash</param>
		/// <param name="key">the key to use</param>
		/// <returns>the computed hash</returns>
		public static byte[] ComputeHash(byte[] msg, SecretKey key)
		{
			return CreatePassword(msg, key);
		}

		/// <summary>Default constructor</summary>
		public JobTokenSecretManager()
		{
			this.masterKey = GenerateSecret();
			this.currentJobTokens = new SortedDictionary<string, SecretKey>();
		}

		/// <summary>Create a new password/secret for the given job token identifier.</summary>
		/// <param name="identifier">the job token identifier</param>
		/// <returns>token password/secret</returns>
		protected override byte[] CreatePassword(JobTokenIdentifier identifier)
		{
			byte[] result = CreatePassword(identifier.GetBytes(), masterKey);
			return result;
		}

		/// <summary>Add the job token of a job to cache</summary>
		/// <param name="jobId">the job that owns the token</param>
		/// <param name="token">the job token</param>
		public virtual void AddTokenForJob(string jobId, Org.Apache.Hadoop.Security.Token.Token
			<JobTokenIdentifier> token)
		{
			SecretKey tokenSecret = CreateSecretKey(token.GetPassword());
			lock (currentJobTokens)
			{
				currentJobTokens[jobId] = tokenSecret;
			}
		}

		/// <summary>Remove the cached job token of a job from cache</summary>
		/// <param name="jobId">the job whose token is to be removed</param>
		public virtual void RemoveTokenForJob(string jobId)
		{
			lock (currentJobTokens)
			{
				Sharpen.Collections.Remove(currentJobTokens, jobId);
			}
		}

		/// <summary>Look up the token password/secret for the given jobId.</summary>
		/// <param name="jobId">the jobId to look up</param>
		/// <returns>token password/secret as SecretKey</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public virtual SecretKey RetrieveTokenSecret(string jobId)
		{
			SecretKey tokenSecret = null;
			lock (currentJobTokens)
			{
				tokenSecret = currentJobTokens[jobId];
			}
			if (tokenSecret == null)
			{
				throw new SecretManager.InvalidToken("Can't find job token for job " + jobId + " !!"
					);
			}
			return tokenSecret;
		}

		/// <summary>Look up the token password/secret for the given job token identifier.</summary>
		/// <param name="identifier">the job token identifier to look up</param>
		/// <returns>token password/secret as byte[]</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public override byte[] RetrievePassword(JobTokenIdentifier identifier)
		{
			return RetrieveTokenSecret(identifier.GetJobId().ToString()).GetEncoded();
		}

		/// <summary>Create an empty job token identifier</summary>
		/// <returns>a newly created empty job token identifier</returns>
		public override JobTokenIdentifier CreateIdentifier()
		{
			return new JobTokenIdentifier();
		}
	}
}
