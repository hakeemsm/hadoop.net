using System;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto
{
	/// <summary>OpenSSL cipher using JNI.</summary>
	/// <remarks>
	/// OpenSSL cipher using JNI.
	/// Currently only AES-CTR is supported. It's flexible to add
	/// other crypto algorithms/modes.
	/// </remarks>
	public sealed class OpensslCipher
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Crypto.OpensslCipher
			).FullName);

		public const int EncryptMode = 1;

		public const int DecryptMode = 0;

		/// <summary>Currently only support AES/CTR/NoPadding.</summary>
		[System.Serializable]
		private sealed class AlgMode
		{
			public static readonly OpensslCipher.AlgMode AesCtr = new OpensslCipher.AlgMode();

			/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
			internal static int Get(string algorithm, string mode)
			{
				try
				{
					return (int)(OpensslCipher.AlgMode.ValueOf(algorithm + "_" + mode));
				}
				catch (Exception)
				{
					throw new NoSuchAlgorithmException("Doesn't support algorithm: " + algorithm + " and mode: "
						 + mode);
				}
			}
		}

		[System.Serializable]
		private sealed class Padding
		{
			public static readonly OpensslCipher.Padding NoPadding = new OpensslCipher.Padding
				();

			/// <exception cref="Sharpen.NoSuchPaddingException"/>
			internal static int Get(string padding)
			{
				try
				{
					return (int)(OpensslCipher.Padding.ValueOf(padding));
				}
				catch (Exception)
				{
					throw new NoSuchPaddingException("Doesn't support padding: " + padding);
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
				if (!NativeCodeLoader.BuildSupportsOpenssl())
				{
					loadingFailure = "build does not support openssl.";
				}
				else
				{
					InitIDs();
				}
			}
			catch (Exception t)
			{
				loadingFailure = t.Message;
				Log.Debug("Failed to load OpenSSL Cipher.", t);
			}
			finally
			{
				loadingFailureReason = loadingFailure;
			}
		}

		public static string GetLoadingFailureReason()
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
		/// <exception cref="Sharpen.NoSuchAlgorithmException">
		/// if <code>transformation</code> is null,
		/// empty, in an invalid format, or if Openssl doesn't implement the
		/// specified algorithm.
		/// </exception>
		/// <exception cref="Sharpen.NoSuchPaddingException">
		/// if <code>transformation</code> contains
		/// a padding scheme that is not available.
		/// </exception>
		public static OpensslCipher GetInstance(string transformation)
		{
			OpensslCipher.Transform transform = TokenizeTransformation(transformation);
			int algMode = OpensslCipher.AlgMode.Get(transform.alg, transform.mode);
			int padding = OpensslCipher.Padding.Get(transform.padding);
			long context = InitContext(algMode, padding);
			return new OpensslCipher(context, algMode, padding);
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

		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		private static OpensslCipher.Transform TokenizeTransformation(string transformation
			)
		{
			if (transformation == null)
			{
				throw new NoSuchAlgorithmException("No transformation given.");
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
			StringTokenizer parser = new StringTokenizer(transformation, "/");
			while (parser.HasMoreTokens() && count < 3)
			{
				parts[count++] = parser.NextToken().Trim();
			}
			if (count != 3 || parser.HasMoreTokens())
			{
				throw new NoSuchAlgorithmException("Invalid transformation format: " + transformation
					);
			}
			return new OpensslCipher.Transform(parts[0], parts[1], parts[2]);
		}

		/// <summary>Initialize this cipher with a key and IV.</summary>
		/// <param name="mode">
		/// 
		/// <see cref="EncryptMode"/>
		/// or
		/// <see cref="DecryptMode"/>
		/// </param>
		/// <param name="key">crypto key</param>
		/// <param name="iv">crypto iv</param>
		public void Init(int mode, byte[] key, byte[] iv)
		{
			context = Init(context, mode, alg, padding, key, iv);
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
		/// <exception cref="Sharpen.ShortBufferException">
		/// if there is insufficient space in the
		/// output buffer
		/// </exception>
		public int Update(ByteBuffer input, ByteBuffer output)
		{
			CheckState();
			Preconditions.CheckArgument(input.IsDirect() && output.IsDirect(), "Direct buffers are required."
				);
			int len = Update(context, input, input.Position(), input.Remaining(), output, output
				.Position(), output.Remaining());
			input.Position(input.Limit());
			output.Position(output.Position() + len);
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
		/// <exception cref="Sharpen.ShortBufferException"/>
		/// <exception cref="Sharpen.IllegalBlockSizeException"/>
		/// <exception cref="Sharpen.BadPaddingException"/>
		public int DoFinal(ByteBuffer output)
		{
			CheckState();
			Preconditions.CheckArgument(output.IsDirect(), "Direct buffer is required.");
			int len = DoFinal(context, output, output.Position(), output.Remaining());
			output.Position(output.Position() + len);
			return len;
		}

		/// <summary>Forcibly clean the context.</summary>
		public void Clean()
		{
			if (context != 0)
			{
				Clean(context);
				context = 0;
			}
		}

		/// <summary>Check whether context is initialized.</summary>
		private void CheckState()
		{
			Preconditions.CheckState(context != 0);
		}

		~OpensslCipher()
		{
			Clean();
		}

		private static void InitIDs()
		{
		}

		private static long InitContext(int alg, int padding)
		{
		}

		private long Init(long context, int mode, int alg, int padding, byte[] key, byte[]
			 iv)
		{
		}

		private int Update(long context, ByteBuffer input, int inputOffset, int inputLength
			, ByteBuffer output, int outputOffset, int maxOutputLength)
		{
		}

		private int DoFinal(long context, ByteBuffer output, int offset, int maxOutputLength
			)
		{
		}

		private void Clean(long context)
		{
		}

		public static string GetLibraryName()
		{
		}
	}
}
