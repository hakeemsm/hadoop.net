using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl
{
	/// <summary>
	/// Indicates that SASL protocol negotiation expected to read a pre-defined magic
	/// number, but the expected value was not seen.
	/// </summary>
	[System.Serializable]
	public class InvalidMagicNumberException : IOException
	{
		private const long serialVersionUID = 1L;

		private readonly bool handshake4Encryption;

		/// <summary>Creates a new InvalidMagicNumberException.</summary>
		/// <param name="magicNumber">expected value</param>
		public InvalidMagicNumberException(int magicNumber, bool handshake4Encryption)
			: base(string.Format("Received %x instead of %x from client.", magicNumber, DataTransferSaslUtil
				.SaslTransferMagicNumber))
		{
			this.handshake4Encryption = handshake4Encryption;
		}

		/// <summary>Return true if it's handshake for encryption</summary>
		/// <returns>boolean true if it's handshake for encryption</returns>
		public virtual bool IsHandshake4Encryption()
		{
			return handshake4Encryption;
		}
	}
}
