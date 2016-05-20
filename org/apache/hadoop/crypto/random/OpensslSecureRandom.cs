using Sharpen;

namespace org.apache.hadoop.crypto.random
{
	/// <summary>OpenSSL secure random using JNI.</summary>
	/// <remarks>
	/// OpenSSL secure random using JNI.
	/// This implementation is thread-safe.
	/// <p/>
	/// If using an Intel chipset with RDRAND, the high-performance hardware
	/// random number generator will be used and it's much faster than
	/// <see cref="java.security.SecureRandom"/>
	/// . If RDRAND is unavailable, default
	/// OpenSSL secure random generator will be used. It's still faster
	/// and can generate strong random bytes.
	/// <p/>
	/// </remarks>
	/// <seealso>https://wiki.openssl.org/index.php/Random_Numbers</seealso>
	/// <seealso>http://en.wikipedia.org/wiki/RdRand</seealso>
	[System.Serializable]
	public class OpensslSecureRandom : java.util.Random
	{
		private const long serialVersionUID = -7828193502768789584L;

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.crypto.random.OpensslSecureRandom
			)).getName());

		/// <summary>If native SecureRandom unavailable, use java SecureRandom</summary>
		private java.security.SecureRandom fallback = null;

		private static bool nativeEnabled = false;

		static OpensslSecureRandom()
		{
			if (org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded() && org.apache.hadoop.util.NativeCodeLoader
				.buildSupportsOpenssl())
			{
				try
				{
					initSR();
					nativeEnabled = true;
				}
				catch (System.Exception t)
				{
					LOG.error("Failed to load Openssl SecureRandom", t);
				}
			}
		}

		public static bool isNativeCodeLoaded()
		{
			return nativeEnabled;
		}

		public OpensslSecureRandom()
		{
			if (!nativeEnabled)
			{
				fallback = new java.security.SecureRandom();
			}
		}

		/// <summary>Generates a user-specified number of random bytes.</summary>
		/// <remarks>
		/// Generates a user-specified number of random bytes.
		/// It's thread-safe.
		/// </remarks>
		/// <param name="bytes">the array to be filled in with random bytes.</param>
		public override void nextBytes(byte[] bytes)
		{
			if (!nativeEnabled || !nextRandBytes(bytes))
			{
				fallback.nextBytes(bytes);
			}
		}

		public override void setSeed(long seed)
		{
		}

		// Self-seeding.
		/// <summary>
		/// Generates an integer containing the user-specified number of
		/// random bits (right justified, with leading zeros).
		/// </summary>
		/// <param name="numBits">
		/// number of random bits to be generated, where
		/// 0 <= &lt;code>numBits</code> &lt;= 32.
		/// </param>
		/// <returns>
		/// int an <code>int</code> containing the user-specified number
		/// of random bits (right justified, with leading zeros).
		/// </returns>
		protected sealed override int next(int numBits)
		{
			com.google.common.@base.Preconditions.checkArgument(numBits >= 0 && numBits <= 32
				);
			int numBytes = (numBits + 7) / 8;
			byte[] b = new byte[numBytes];
			int next = 0;
			nextBytes(b);
			for (int i = 0; i < numBytes; i++)
			{
				next = (next << 8) + (b[i] & unchecked((int)(0xFF)));
			}
			return (int)(((uint)next) >> (numBytes * 8 - numBits));
		}

		private static void initSR()
		{
		}

		private bool nextRandBytes(byte[] bytes)
		{
		}
	}
}
