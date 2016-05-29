using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security.Client
{
	/// <summary>
	/// A base
	/// <see cref="Org.Apache.Hadoop.Security.Token.SecretManager{T}"/>
	/// for AMs to extend and validate Client-RM tokens
	/// issued to clients by the RM using the underlying master-key shared by RM to
	/// the AMs on their launch. All the methods are called by either Hadoop RPC or
	/// YARN, so this class is strictly for the purpose of inherit/extend and
	/// register with Hadoop RPC.
	/// </summary>
	public abstract class BaseClientToAMTokenSecretManager : SecretManager<ClientToAMTokenIdentifier
		>
	{
		[InterfaceAudience.Private]
		public abstract SecretKey GetMasterKey(ApplicationAttemptId applicationAttemptId);

		[InterfaceAudience.Private]
		protected override byte[] CreatePassword(ClientToAMTokenIdentifier identifier)
		{
			lock (this)
			{
				return CreatePassword(identifier.GetBytes(), GetMasterKey(identifier.GetApplicationAttemptID
					()));
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		[InterfaceAudience.Private]
		public override byte[] RetrievePassword(ClientToAMTokenIdentifier identifier)
		{
			SecretKey masterKey = GetMasterKey(identifier.GetApplicationAttemptID());
			if (masterKey == null)
			{
				throw new SecretManager.InvalidToken("Illegal client-token!");
			}
			return CreatePassword(identifier.GetBytes(), masterKey);
		}

		[InterfaceAudience.Private]
		public override ClientToAMTokenIdentifier CreateIdentifier()
		{
			return new ClientToAMTokenIdentifier();
		}
	}
}
