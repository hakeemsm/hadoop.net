using Sharpen;

namespace org.apache.hadoop.crypto
{
	/// <summary>Defines properties of a CipherSuite.</summary>
	/// <remarks>
	/// Defines properties of a CipherSuite. Modeled after the ciphers in
	/// <see cref="javax.crypto.Cipher"/>
	/// .
	/// </remarks>
	[System.Serializable]
	public sealed class CipherSuite
	{
		public static readonly org.apache.hadoop.crypto.CipherSuite UNKNOWN = new org.apache.hadoop.crypto.CipherSuite
			("Unknown", 0);

		public static readonly org.apache.hadoop.crypto.CipherSuite AES_CTR_NOPADDING = new 
			org.apache.hadoop.crypto.CipherSuite("AES/CTR/NoPadding", 16);

		private readonly string name;

		private readonly int algoBlockSize;

		private int unknownValue = null;

		internal CipherSuite(string name, int algoBlockSize)
		{
			this.name = name;
			this.algoBlockSize = algoBlockSize;
		}

		public void setUnknownValue(int unknown)
		{
			this.unknownValue = unknown;
		}

		public int getUnknownValue()
		{
			return org.apache.hadoop.crypto.CipherSuite.unknownValue;
		}

		/// <returns>
		/// name of cipher suite, as in
		/// <see cref="javax.crypto.Cipher"/>
		/// </returns>
		public string getName()
		{
			return org.apache.hadoop.crypto.CipherSuite.name;
		}

		/// <returns>size of an algorithm block in bytes</returns>
		public int getAlgorithmBlockSize()
		{
			return org.apache.hadoop.crypto.CipherSuite.algoBlockSize;
		}

		public override string ToString()
		{
			java.lang.StringBuilder builder = new java.lang.StringBuilder("{");
			builder.Append("name: " + org.apache.hadoop.crypto.CipherSuite.name);
			builder.Append(", algorithmBlockSize: " + org.apache.hadoop.crypto.CipherSuite.algoBlockSize
				);
			if (org.apache.hadoop.crypto.CipherSuite.unknownValue != null)
			{
				builder.Append(", unknownValue: " + org.apache.hadoop.crypto.CipherSuite.unknownValue
					);
			}
			builder.Append("}");
			return builder.ToString();
		}

		/// <summary>
		/// Convert to CipherSuite from name,
		/// <see cref="algoBlockSize"/>
		/// is fixed for
		/// certain cipher suite, just need to compare the name.
		/// </summary>
		/// <param name="name">cipher suite name</param>
		/// <returns>CipherSuite cipher suite</returns>
		public static org.apache.hadoop.crypto.CipherSuite convert(string name)
		{
			org.apache.hadoop.crypto.CipherSuite[] suites = org.apache.hadoop.crypto.CipherSuite
				.values();
			foreach (org.apache.hadoop.crypto.CipherSuite suite in suites)
			{
				if (suite.getName().Equals(name))
				{
					return suite;
				}
			}
			throw new System.ArgumentException("Invalid cipher suite name: " + name);
		}

		/// <summary>Returns suffix of cipher suite configuration.</summary>
		/// <returns>String configuration suffix</returns>
		public string getConfigSuffix()
		{
			string[] parts = org.apache.hadoop.crypto.CipherSuite.name.split("/");
			java.lang.StringBuilder suffix = new java.lang.StringBuilder();
			foreach (string part in parts)
			{
				suffix.Append(".").Append(org.apache.hadoop.util.StringUtils.toLowerCase(part));
			}
			return suffix.ToString();
		}
	}
}
