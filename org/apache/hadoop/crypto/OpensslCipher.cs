using Sharpen;

namespace org.apache.hadoop.crypto
{
	/// <summary>OpenSSL cipher using JNI.</summary>
	/// <remarks>
	/// OpenSSL cipher using JNI.
	/// Currently only AES-CTR is supported. It's flexible to add
	/// other crypto algorithms/modes.
	/// </remarks>
	public sealed class OpensslCipher
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.crypto.OpensslCipher
			)).getName());

		public const int ENCRYPT_MODE = 1;

		public const int DECRYPT_MODE = 0;

		/// <summary>Currently only support AES/CTR/NoPadding.</summary>
		[System.Serializable]
		private sealed class AlgMode
		{
			public static readonly org.apache.hadoop.crypto.OpensslCipher.AlgMode AES_CTR = new 
				org.apache.hadoop.crypto.OpensslCipher.AlgMode();

			/// <exception cref="java.security.NoSuchAlgorithmException"/>
			internal static int get(string algorithm, string mode)
			{
				try
				{
					return (int)(org.apache.hadoop.crypto.OpensslCipher.AlgMode.valueOf(algorithm + "_"
						 + mode));
				}
				catch (System.Exception)
				{
					throw new java.security.NoSuchAlgorithmException("Doesn't support algorithm: " + 
						algorithm + " and mode: " + mode);
				}
			}
		}

		[System.Serializable]
		private sealed class Padding
		{
			public static readonly org.apache.hadoop.crypto.OpensslCipher.Padding NoPadding = 
				new org.apache.hadoop.crypto.OpensslCipher.Padding();

			/// <exception cref="javax.crypto.NoSuchPaddingException"/>
			internal static int get(string padding)
			{
				try
				{
					return (int)(org.apache.hadoop.crypto.OpensslCipher.Padding.valueOf(padding));
				}
				catch (System.Exception)
				{
					throw new javax.crypto.NoSuchPaddingException("Doesn't support padding: " + padding
						);
				}
			}
		}

		private long context = 0;

		private readonly int alg;

		private readonly int padding;

		private static readonly string loadingFailureReason;

		static OpensslCipher()
		{
			string loadingFailure = null;
			try
			{
				if (!org.apache.hadoop.util.NativeCodeLoader.buildSupportsOpenssl())
				{
					loadingFailure = "build does not support openssl.";
				}
				else
				{
					initIDs();
				}
			}
			catch (System.Exception t)
			{
				loadingFailure = t.Message;
				LOG.debug("Failed to load OpenSSL Cipher.", t);
			}
			finally
			{
				loadingFailureReason = loadingFailure;
			}
		}

		public static string getLoadingFailureReason()
		{
			return loadingFailureReason;
		}

		private OpensslCipher(long context, int alg, int padding)
		{
			this.context = context;
			this.alg = alg;
			this.padding = padding;
		}

		/// <summary>
		/// Return an <code>OpensslCipher<code> object that implements the specified
		/// transformation.
		/// </summary>
		/// <param name="transformation">
		/// the name of the transformation, e.g.,
		/// AES/CTR/NoPadding.
		/// </param>
		/// <returns>OpensslCipher an <code>OpensslCipher<code> object</returns>
		/// <exception cref="java.security.NoSuchAlgorithmException">
		/// if <code>transformation</code> is null,
		/// empty, in an invalid format, or if Openssl doesn't implement the
		/// specified algorithm.
		/// </exception>
		/// <exception cref="javax.crypto.NoSuchPaddingException">
		/// if <code>transformation</code> contains
		/// a padding scheme that is not available.
		/// </exception>
		public static org.apache.hadoop.crypto.OpensslCipher getInstance(string transformation
			)
		{
			org.apache.hadoop.crypto.OpensslCipher.Transform transform = tokenizeTransformation
				(transformation);
			int algMode = org.apache.hadoop.crypto.OpensslCipher.AlgMode.get(transform.alg, transform
				.mode);
			int padding = org.apache.hadoop.crypto.OpensslCipher.Padding.get(transform.padding
				);
			long context = initContext(algMode, padding);
			return new org.apache.hadoop.crypto.OpensslCipher(context, algMode, padding);
		}

		/// <summary>Nested class for algorithm, mode and padding.</summary>
		private class Transform
		{
			internal readonly string alg;

			internal readonly string mode;

			internal readonly string padding;

			public Transform(string alg, string mode, string padding)
			{
				this.alg = alg;
				this.mode = mode;
				this.padding = padding;
			}
		}

		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		private static org.apache.hadoop.crypto.OpensslCipher.Transform tokenizeTransformation
			(string transformation)
		{
			if (transformation == null)
			{
				throw new java.security.NoSuchAlgorithmException("No transformation given.");
			}
			/*
			* Array containing the components of a Cipher transformation:
			*
			* index 0: algorithm (e.g., AES)
			* index 1: mode (e.g., CTR)
			* index 2: padding (e.g., NoPadding)
			*/
			string[] parts = new string[3];
			int count = 0;
			java.util.StringTokenizer parser = new java.util.StringTokenizer(transformation, 
				"/");
			while (parser.hasMoreTokens() && count < 3)
			{
				parts[count++] = parser.nextToken().Trim();
			}
			if (count != 3 || parser.hasMoreTokens())
			{
				throw new java.security.NoSuchAlgorithmException("Invalid transformation format: "
					 + transformation);
			}
			return new org.apache.hadoop.crypto.OpensslCipher.Transform(parts[0], parts[1], parts
				[2]);
		}

		/// <summary>Initialize this cipher with a key and IV.</summary>
		/// <param name="mode">
		/// 
		/// <see cref="ENCRYPT_MODE"/>
		/// or
		/// <see cref="DECRYPT_MODE"/>
		/// </param>
		/// <param name="key">crypto key</param>
		/// <param name="iv">crypto iv</param>
		public void init(int mode, byte[] key, byte[] iv)
		{
			context = init(context, mode, alg, padding, key, iv);
		}

		/// <summary>Continues a multiple-part encryption or decryption operation.</summary>
		/// <remarks>
		/// Continues a multiple-part encryption or decryption operation. The data
		/// is encrypted or decrypted, depending on how this cipher was initialized.
		/// <p/>
		/// All <code>input.remaining()</code> bytes starting at
		/// <code>input.position()</code> are processed. The result is stored in
		/// the output buffer.
		/// <p/>
		/// Upon return, the input buffer's position will be equal to its limit;
		/// its limit will not have changed. The output buffer's position will have
		/// advanced by n, when n is the value returned by this method; the output
		/// buffer's limit will not have changed.
		/// <p/>
		/// If <code>output.remaining()</code> bytes are insufficient to hold the
		/// result, a <code>ShortBufferException</code> is thrown.
		/// </remarks>
		/// <param name="input">the input ByteBuffer</param>
		/// <param name="output">the output ByteBuffer</param>
		/// <returns>int number of bytes stored in <code>output</code></returns>
		/// <exception cref="javax.crypto.ShortBufferException">
		/// if there is insufficient space in the
		/// output buffer
		/// </exception>
		public int update(java.nio.ByteBuffer input, java.nio.ByteBuffer output)
		{
			checkState();
			com.google.common.@base.Preconditions.checkArgument(input.isDirect() && output.isDirect
				(), "Direct buffers are required.");
			int len = update(context, input, input.position(), input.remaining(), output, output
				.position(), output.remaining());
			input.position(input.limit());
			output.position(output.position() + len);
			return len;
		}

		/// <summary>Finishes a multiple-part operation.</summary>
		/// <remarks>
		/// Finishes a multiple-part operation. The data is encrypted or decrypted,
		/// depending on how this cipher was initialized.
		/// <p/>
		/// The result is stored in the output buffer. Upon return, the output buffer's
		/// position will have advanced by n, where n is the value returned by this
		/// method; the output buffer's limit will not have changed.
		/// <p/>
		/// If <code>output.remaining()</code> bytes are insufficient to hold the result,
		/// a <code>ShortBufferException</code> is thrown.
		/// <p/>
		/// Upon finishing, this method resets this cipher object to the state it was
		/// in when previously initialized. That is, the object is available to encrypt
		/// or decrypt more data.
		/// <p/>
		/// If any exception is thrown, this cipher object need to be reset before it
		/// can be used again.
		/// </remarks>
		/// <param name="output">the output ByteBuffer</param>
		/// <returns>int number of bytes stored in <code>output</code></returns>
		/// <exception cref="javax.crypto.ShortBufferException"/>
		/// <exception cref="javax.crypto.IllegalBlockSizeException"/>
		/// <exception cref="javax.crypto.BadPaddingException"/>
		public int doFinal(java.nio.ByteBuffer output)
		{
			checkState();
			com.google.common.@base.Preconditions.checkArgument(output.isDirect(), "Direct buffer is required."
				);
			int len = doFinal(context, output, output.position(), output.remaining());
			output.position(output.position() + len);
			return len;
		}

		/// <summary>Forcibly clean the context.</summary>
		public void clean()
		{
			if (context != 0)
			{
				clean(context);
				context = 0;
			}
		}

		/// <summary>Check whether context is initialized.</summary>
		private void checkState()
		{
			com.google.common.@base.Preconditions.checkState(context != 0);
		}

		~OpensslCipher()
		{
			clean();
		}

		private static void initIDs()
		{
		}

		private static long initContext(int alg, int padding)
		{
		}

		private long init(long context, int mode, int alg, int padding, byte[] key, byte[]
			 iv)
		{
		}

		private int update(long context, java.nio.ByteBuffer input, int inputOffset, int 
			inputLength, java.nio.ByteBuffer output, int outputOffset, int maxOutputLength)
		{
		}

		private int doFinal(long context, java.nio.ByteBuffer output, int offset, int maxOutputLength
			)
		{
		}

		private void clean(long context)
		{
		}

		public static string getLibraryName()
		{
		}
	}
}
