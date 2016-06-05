using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <p><code>Token</code> is the security entity used by the framework
	/// to verify authenticity of any resource.</p>
	/// </summary>
	public abstract class Token
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static Token NewInstance(byte[] identifier, string kind, byte[] password, 
			string service)
		{
			Token token = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Token>();
			token.SetIdentifier(ByteBuffer.Wrap(identifier));
			token.SetKind(kind);
			token.SetPassword(ByteBuffer.Wrap(password));
			token.SetService(service);
			return token;
		}

		/// <summary>Get the token identifier.</summary>
		/// <returns>token identifier</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ByteBuffer GetIdentifier();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetIdentifier(ByteBuffer identifier);

		/// <summary>Get the token password</summary>
		/// <returns>token password</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ByteBuffer GetPassword();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetPassword(ByteBuffer password);

		/// <summary>Get the token kind.</summary>
		/// <returns>token kind</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetKind();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetKind(string kind);

		/// <summary>Get the service to which the token is allocated.</summary>
		/// <returns>service to which the token is allocated</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetService();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetService(string service);
	}
}
