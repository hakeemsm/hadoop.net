using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Util.Hash
{
	/// <summary>This class represents a common API for hashing functions.</summary>
	public abstract class Hash
	{
		/// <summary>Constant to denote invalid hash type.</summary>
		public const int InvalidHash = -1;

		/// <summary>
		/// Constant to denote
		/// <see cref="JenkinsHash"/>
		/// .
		/// </summary>
		public const int JenkinsHash = 0;

		/// <summary>
		/// Constant to denote
		/// <see cref="MurmurHash"/>
		/// .
		/// </summary>
		public const int MurmurHash = 1;

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
		public static int ParseHashType(string name)
		{
			if (Sharpen.Runtime.EqualsIgnoreCase("jenkins", name))
			{
				return JenkinsHash;
			}
			else
			{
				if (Sharpen.Runtime.EqualsIgnoreCase("murmur", name))
				{
					return MurmurHash;
				}
				else
				{
					return InvalidHash;
				}
			}
		}

		/// <summary>
		/// This utility method converts the name of the configured
		/// hash type to a symbolic constant.
		/// </summary>
		/// <param name="conf">configuration</param>
		/// <returns>one of the predefined constants</returns>
		public static int GetHashType(Configuration conf)
		{
			string name = conf.Get("hadoop.util.hash.type", "murmur");
			return ParseHashType(name);
		}

		/// <summary>Get a singleton instance of hash function of a given type.</summary>
		/// <param name="type">predefined hash type</param>
		/// <returns>hash function instance, or null if type is invalid</returns>
		public static Org.Apache.Hadoop.Util.Hash.Hash GetInstance(int type)
		{
			switch (type)
			{
				case JenkinsHash:
				{
					return JenkinsHash.GetInstance();
				}

				case MurmurHash:
				{
					return MurmurHash.GetInstance();
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
		public static Org.Apache.Hadoop.Util.Hash.Hash GetInstance(Configuration conf)
		{
			int type = GetHashType(conf);
			return GetInstance(type);
		}

		/// <summary>
		/// Calculate a hash using all bytes from the input argument, and
		/// a seed of -1.
		/// </summary>
		/// <param name="bytes">input bytes</param>
		/// <returns>hash value</returns>
		public virtual int Hash(byte[] bytes)
		{
			return Hash(bytes, bytes.Length, -1);
		}

		/// <summary>
		/// Calculate a hash using all bytes from the input argument,
		/// and a provided seed value.
		/// </summary>
		/// <param name="bytes">input bytes</param>
		/// <param name="initval">seed value</param>
		/// <returns>hash value</returns>
		public virtual int Hash(byte[] bytes, int initval)
		{
			return Hash(bytes, bytes.Length, initval);
		}

		/// <summary>
		/// Calculate a hash using bytes from 0 to <code>length</code>, and
		/// the provided seed value
		/// </summary>
		/// <param name="bytes">input bytes</param>
		/// <param name="length">length of the valid bytes to consider</param>
		/// <param name="initval">seed value</param>
		/// <returns>hash value</returns>
		public abstract int Hash(byte[] bytes, int length, int initval);
	}
}
