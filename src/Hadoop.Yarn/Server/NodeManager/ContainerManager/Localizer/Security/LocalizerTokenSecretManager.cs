using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Security
{
	public class LocalizerTokenSecretManager : SecretManager<LocalizerTokenIdentifier
		>
	{
		private readonly SecretKey secretKey;

		public LocalizerTokenSecretManager()
		{
			this.secretKey = GenerateSecret();
		}

		protected override byte[] CreatePassword(LocalizerTokenIdentifier identifier)
		{
			return CreatePassword(identifier.GetBytes(), secretKey);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public override byte[] RetrievePassword(LocalizerTokenIdentifier identifier)
		{
			return CreatePassword(identifier.GetBytes(), secretKey);
		}

		public override LocalizerTokenIdentifier CreateIdentifier()
		{
			return new LocalizerTokenIdentifier();
		}
	}
}
