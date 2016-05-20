using Sharpen;

namespace org.apache.hadoop.crypto
{
	/// <summary>Versions of the client/server protocol used for HDFS encryption.</summary>
	[System.Serializable]
	public sealed class CryptoProtocolVersion
	{
		public static readonly org.apache.hadoop.crypto.CryptoProtocolVersion UNKNOWN = new 
			org.apache.hadoop.crypto.CryptoProtocolVersion("Unknown", 1);

		public static readonly org.apache.hadoop.crypto.CryptoProtocolVersion ENCRYPTION_ZONES
			 = new org.apache.hadoop.crypto.CryptoProtocolVersion("Encryption zones", 2);

		private readonly string description;

		private readonly int version;

		private int unknownValue = null;

		private static org.apache.hadoop.crypto.CryptoProtocolVersion[] supported = new org.apache.hadoop.crypto.CryptoProtocolVersion
			[] { org.apache.hadoop.crypto.CryptoProtocolVersion.ENCRYPTION_ZONES };

		/// <returns>Array of supported protocol versions.</returns>
		public static org.apache.hadoop.crypto.CryptoProtocolVersion[] supported()
		{
			return org.apache.hadoop.crypto.CryptoProtocolVersion.supported;
		}

		internal CryptoProtocolVersion(string description, int version)
		{
			this.description = description;
			this.version = version;
		}

		/// <summary>Returns if a given protocol version is supported.</summary>
		/// <param name="version">version number</param>
		/// <returns>true if the version is supported, else false</returns>
		public static bool supports(org.apache.hadoop.crypto.CryptoProtocolVersion version
			)
		{
			if (version.getVersion() == org.apache.hadoop.crypto.CryptoProtocolVersion.UNKNOWN
				.getVersion())
			{
				return false;
			}
			foreach (org.apache.hadoop.crypto.CryptoProtocolVersion v in org.apache.hadoop.crypto.CryptoProtocolVersion
				.values())
			{
				if (v.getVersion() == version.getVersion())
				{
					return true;
				}
			}
			return false;
		}

		public void setUnknownValue(int unknown)
		{
			this.unknownValue = unknown;
		}

		public int getUnknownValue()
		{
			return org.apache.hadoop.crypto.CryptoProtocolVersion.unknownValue;
		}

		public string getDescription()
		{
			return org.apache.hadoop.crypto.CryptoProtocolVersion.description;
		}

		public int getVersion()
		{
			return org.apache.hadoop.crypto.CryptoProtocolVersion.version;
		}

		public override string ToString()
		{
			return "CryptoProtocolVersion{" + "description='" + org.apache.hadoop.crypto.CryptoProtocolVersion
				.description + '\'' + ", version=" + org.apache.hadoop.crypto.CryptoProtocolVersion
				.version + ", unknownValue=" + org.apache.hadoop.crypto.CryptoProtocolVersion.unknownValue
				 + '}';
		}
	}
}
