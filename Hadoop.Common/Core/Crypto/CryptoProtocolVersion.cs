using Sharpen;

namespace Org.Apache.Hadoop.Crypto
{
	/// <summary>Versions of the client/server protocol used for HDFS encryption.</summary>
	[System.Serializable]
	public sealed class CryptoProtocolVersion
	{
		public static readonly Org.Apache.Hadoop.Crypto.CryptoProtocolVersion Unknown = new 
			Org.Apache.Hadoop.Crypto.CryptoProtocolVersion("Unknown", 1);

		public static readonly Org.Apache.Hadoop.Crypto.CryptoProtocolVersion EncryptionZones
			 = new Org.Apache.Hadoop.Crypto.CryptoProtocolVersion("Encryption zones", 2);

		private readonly string description;

		private readonly int version;

		private int unknownValue = null;

		private static Org.Apache.Hadoop.Crypto.CryptoProtocolVersion[] supported = new Org.Apache.Hadoop.Crypto.CryptoProtocolVersion
			[] { Org.Apache.Hadoop.Crypto.CryptoProtocolVersion.EncryptionZones };

		/// <returns>Array of supported protocol versions.</returns>
		public static Org.Apache.Hadoop.Crypto.CryptoProtocolVersion[] Supported()
		{
			return Org.Apache.Hadoop.Crypto.CryptoProtocolVersion.supported;
		}

		internal CryptoProtocolVersion(string description, int version)
		{
			this.description = description;
			this.version = version;
		}

		/// <summary>Returns if a given protocol version is supported.</summary>
		/// <param name="version">version number</param>
		/// <returns>true if the version is supported, else false</returns>
		public static bool Supports(Org.Apache.Hadoop.Crypto.CryptoProtocolVersion version
			)
		{
			if (version.GetVersion() == Org.Apache.Hadoop.Crypto.CryptoProtocolVersion.Unknown
				.GetVersion())
			{
				return false;
			}
			foreach (Org.Apache.Hadoop.Crypto.CryptoProtocolVersion v in Org.Apache.Hadoop.Crypto.CryptoProtocolVersion
				.Values())
			{
				if (v.GetVersion() == version.GetVersion())
				{
					return true;
				}
			}
			return false;
		}

		public void SetUnknownValue(int unknown)
		{
			this.unknownValue = unknown;
		}

		public int GetUnknownValue()
		{
			return Org.Apache.Hadoop.Crypto.CryptoProtocolVersion.unknownValue;
		}

		public string GetDescription()
		{
			return Org.Apache.Hadoop.Crypto.CryptoProtocolVersion.description;
		}

		public int GetVersion()
		{
			return Org.Apache.Hadoop.Crypto.CryptoProtocolVersion.version;
		}

		public override string ToString()
		{
			return "CryptoProtocolVersion{" + "description='" + Org.Apache.Hadoop.Crypto.CryptoProtocolVersion
				.description + '\'' + ", version=" + Org.Apache.Hadoop.Crypto.CryptoProtocolVersion
				.version + ", unknownValue=" + Org.Apache.Hadoop.Crypto.CryptoProtocolVersion.unknownValue
				 + '}';
		}
	}
}
