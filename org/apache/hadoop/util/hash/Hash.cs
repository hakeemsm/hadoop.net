using Sharpen;

namespace org.apache.hadoop.util.hash
{
	/// <summary>This class represents a common API for hashing functions.</summary>
	public abstract class Hash
	{
		/// <summary>Constant to denote invalid hash type.</summary>
		public const int INVALID_HASH = -1;

		/// <summary>
		/// Constant to denote
		/// <see cref="JenkinsHash"/>
		/// .
		/// </summary>
		public const int JENKINS_HASH = 0;

		/// <summary>
		/// Constant to denote
		/// <see cref="MurmurHash"/>
		/// .
		/// </summary>
		public const int MURMUR_HASH = 1;

		/// <summary>
		/// This utility method converts String representation of hash function name
		/// to a symbolic constant.
		/// </summary>
		/// <remarks>
		/// This utility method converts String representation of hash function name
		/// to a symbolic constant. Currently two function types are supported,
		/// "jenkins" and "murmur".
		/// </remarks>
		/// <param name="name">hash function name</param>
		/// <returns>one of the predefined constants</returns>
		public static int parseHashType(string name)
		{
			if (Sharpen.Runtime.equalsIgnoreCase("jenkins", name))
			{
				return JENKINS_HASH;
			}
			else
			{
				if (Sharpen.Runtime.equalsIgnoreCase("murmur", name))
				{
					return MURMUR_HASH;
				}
				else
				{
					return INVALID_HASH;
				}
			}
		}

		/// <summary>
		/// This utility method converts the name of the configured
		/// hash type to a symbolic constant.
		/// </summary>
		/// <param name="conf">configuration</param>
		/// <returns>one of the predefined constants</returns>
		public static int getHashType(org.apache.hadoop.conf.Configuration conf)
		{
			string name = conf.get("hadoop.util.hash.type", "murmur");
			return parseHashType(name);
		}

		/// <summary>Get a singleton instance of hash function of a given type.</summary>
		/// <param name="type">predefined hash type</param>
		/// <returns>hash function instance, or null if type is invalid</returns>
		public static org.apache.hadoop.util.hash.Hash getInstance(int type)
		{
			switch (type)
			{
				case JENKINS_HASH:
				{
					return org.apache.hadoop.util.hash.JenkinsHash.getInstance();
				}

				case MURMUR_HASH:
				{
					return org.apache.hadoop.util.hash.MurmurHash.getInstance();
				}

				default:
				{
					return null;
				}
			}
		}

		/// <summary>
		/// Get a singleton instance of hash function of a type
		/// defined in the configuration.
		/// </summary>
		/// <param name="conf">current configuration</param>
		/// <returns>defined hash type, or null if type is invalid</returns>
		public static org.apache.hadoop.util.hash.Hash getInstance(org.apache.hadoop.conf.Configuration
			 conf)
		{
			int type = getHashType(conf);
			return getInstance(type);
		}

		/// <summary>
		/// Calculate a hash using all bytes from the input argument, and
		/// a seed of -1.
		/// </summary>
		/// <param name="bytes">input bytes</param>
		/// <returns>hash value</returns>
		public virtual int hash(byte[] bytes)
		{
			return hash(bytes, bytes.Length, -1);
		}

		/// <summary>
		/// Calculate a hash using all bytes from the input argument,
		/// and a provided seed value.
		/// </summary>
		/// <param name="bytes">input bytes</param>
		/// <param name="initval">seed value</param>
		/// <returns>hash value</returns>
		public virtual int hash(byte[] bytes, int initval)
		{
			return hash(bytes, bytes.Length, initval);
		}

		/// <summary>
		/// Calculate a hash using bytes from 0 to <code>length</code>, and
		/// the provided seed value
		/// </summary>
		/// <param name="bytes">input bytes</param>
		/// <param name="length">length of the valid bytes to consider</param>
		/// <param name="initval">seed value</param>
		/// <returns>hash value</returns>
		public abstract int hash(byte[] bytes, int length, int initval);
	}
}
