using System;
using System.Text;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto
{
	/// <summary>Defines properties of a CipherSuite.</summary>
	/// <remarks>
	/// Defines properties of a CipherSuite. Modeled after the ciphers in
	/// <see cref="Sharpen.Cipher"/>
	/// .
	/// </remarks>
	[System.Serializable]
	public sealed class CipherSuite
	{
		public static readonly Org.Apache.Hadoop.Crypto.CipherSuite Unknown = new Org.Apache.Hadoop.Crypto.CipherSuite
			("Unknown", 0);

		public static readonly Org.Apache.Hadoop.Crypto.CipherSuite AesCtrNopadding = new 
			Org.Apache.Hadoop.Crypto.CipherSuite("AES/CTR/NoPadding", 16);

		private readonly string name;

		private readonly int algoBlockSize;

		private int unknownValue = null;

		internal CipherSuite(string name, int algoBlockSize)
		{
			this.name = name;
			this.algoBlockSize = algoBlockSize;
		}

		public void SetUnknownValue(int unknown)
		{
			this.unknownValue = unknown;
		}

		public int GetUnknownValue()
		{
			return Org.Apache.Hadoop.Crypto.CipherSuite.unknownValue;
		}

		/// <returns>
		/// name of cipher suite, as in
		/// <see cref="Sharpen.Cipher"/>
		/// </returns>
		public string GetName()
		{
			return Org.Apache.Hadoop.Crypto.CipherSuite.name;
		}

		/// <returns>size of an algorithm block in bytes</returns>
		public int GetAlgorithmBlockSize()
		{
			return Org.Apache.Hadoop.Crypto.CipherSuite.algoBlockSize;
		}

		public override string ToString()
		{
			StringBuilder builder = new StringBuilder("{");
			builder.Append("name: " + Org.Apache.Hadoop.Crypto.CipherSuite.name);
			builder.Append(", algorithmBlockSize: " + Org.Apache.Hadoop.Crypto.CipherSuite.algoBlockSize
				);
			if (Org.Apache.Hadoop.Crypto.CipherSuite.unknownValue != null)
			{
				builder.Append(", unknownValue: " + Org.Apache.Hadoop.Crypto.CipherSuite.unknownValue
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
		public static Org.Apache.Hadoop.Crypto.CipherSuite Convert(string name)
		{
			Org.Apache.Hadoop.Crypto.CipherSuite[] suites = Org.Apache.Hadoop.Crypto.CipherSuite
				.Values();
			foreach (Org.Apache.Hadoop.Crypto.CipherSuite suite in suites)
			{
				if (suite.GetName().Equals(name))
				{
					return suite;
				}
			}
			throw new ArgumentException("Invalid cipher suite name: " + name);
		}

		/// <summary>Returns suffix of cipher suite configuration.</summary>
		/// <returns>String configuration suffix</returns>
		public string GetConfigSuffix()
		{
			string[] parts = Org.Apache.Hadoop.Crypto.CipherSuite.name.Split("/");
			StringBuilder suffix = new StringBuilder();
			foreach (string part in parts)
			{
				suffix.Append(".").Append(StringUtils.ToLowerCase(part));
			}
			return suffix.ToString();
		}
	}
}
