using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Primitives;
using Mono.Math;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto
{
	public class TestCryptoCodec
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestCryptoCodec));

		private static byte[] key = new byte[16];

		private static byte[] iv = new byte[16];

		private const int bufferSize = 4096;

		private Configuration conf = new Configuration();

		private int count = 10000;

		private int seed = new Random().Next();

		private readonly string jceCodecClass = "org.apache.hadoop.crypto.JceAesCtrCryptoCodec";

		private readonly string opensslCodecClass = "org.apache.hadoop.crypto.OpensslAesCtrCryptoCodec";

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Random random = new SecureRandom();
			random.NextBytes(key);
			random.NextBytes(iv);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJceAesCtrCryptoCodec()
		{
			GenericTestUtils.AssumeInNativeProfile();
			if (!NativeCodeLoader.BuildSupportsOpenssl())
			{
				Log.Warn("Skipping test since openSSL library not loaded");
				Assume.AssumeTrue(false);
			}
			Assert.Equal(null, OpensslCipher.GetLoadingFailureReason());
			CryptoCodecTest(conf, seed, 0, jceCodecClass, jceCodecClass, iv);
			CryptoCodecTest(conf, seed, count, jceCodecClass, jceCodecClass, iv);
			CryptoCodecTest(conf, seed, count, jceCodecClass, opensslCodecClass, iv);
			// Overflow test, IV: xx xx xx xx xx xx xx xx ff ff ff ff ff ff ff ff 
			for (int i = 0; i < 8; i++)
			{
				iv[8 + i] = unchecked((byte)unchecked((int)(0xff)));
			}
			CryptoCodecTest(conf, seed, count, jceCodecClass, jceCodecClass, iv);
			CryptoCodecTest(conf, seed, count, jceCodecClass, opensslCodecClass, iv);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestOpensslAesCtrCryptoCodec()
		{
			GenericTestUtils.AssumeInNativeProfile();
			if (!NativeCodeLoader.BuildSupportsOpenssl())
			{
				Log.Warn("Skipping test since openSSL library not loaded");
				Assume.AssumeTrue(false);
			}
			Assert.Equal(null, OpensslCipher.GetLoadingFailureReason());
			CryptoCodecTest(conf, seed, 0, opensslCodecClass, opensslCodecClass, iv);
			CryptoCodecTest(conf, seed, count, opensslCodecClass, opensslCodecClass, iv);
			CryptoCodecTest(conf, seed, count, opensslCodecClass, jceCodecClass, iv);
			// Overflow test, IV: xx xx xx xx xx xx xx xx ff ff ff ff ff ff ff ff 
			for (int i = 0; i < 8; i++)
			{
				iv[8 + i] = unchecked((byte)unchecked((int)(0xff)));
			}
			CryptoCodecTest(conf, seed, count, opensslCodecClass, opensslCodecClass, iv);
			CryptoCodecTest(conf, seed, count, opensslCodecClass, jceCodecClass, iv);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.GeneralSecurityException"/>
		private void CryptoCodecTest(Configuration conf, int seed, int count, string encCodecClass
			, string decCodecClass, byte[] iv)
		{
			CryptoCodec encCodec = null;
			try
			{
				encCodec = (CryptoCodec)ReflectionUtils.NewInstance(conf.GetClassByName(encCodecClass
					), conf);
			}
			catch (TypeLoadException)
			{
				throw new IOException("Illegal crypto codec!");
			}
			Log.Info("Created a Codec object of type: " + encCodecClass);
			// Generate data
			DataOutputBuffer data = new DataOutputBuffer();
			RandomDatum.Generator generator = new RandomDatum.Generator(seed);
			for (int i = 0; i < count; ++i)
			{
				generator.Next();
				RandomDatum key = generator.GetKey();
				RandomDatum value = generator.GetValue();
				key.Write(data);
				value.Write(data);
			}
			Log.Info("Generated " + count + " records");
			// Encrypt data
			DataOutputBuffer encryptedDataBuffer = new DataOutputBuffer();
			CryptoOutputStream @out = new CryptoOutputStream(encryptedDataBuffer, encCodec, bufferSize
				, key, iv);
			@out.Write(data.GetData(), 0, data.GetLength());
			@out.Flush();
			@out.Close();
			Log.Info("Finished encrypting data");
			CryptoCodec decCodec = null;
			try
			{
				decCodec = (CryptoCodec)ReflectionUtils.NewInstance(conf.GetClassByName(decCodecClass
					), conf);
			}
			catch (TypeLoadException)
			{
				throw new IOException("Illegal crypto codec!");
			}
			Log.Info("Created a Codec object of type: " + decCodecClass);
			// Decrypt data
			DataInputBuffer decryptedDataBuffer = new DataInputBuffer();
			decryptedDataBuffer.Reset(encryptedDataBuffer.GetData(), 0, encryptedDataBuffer.GetLength
				());
			CryptoInputStream @in = new CryptoInputStream(decryptedDataBuffer, decCodec, bufferSize
				, key, iv);
			DataInputStream dataIn = new DataInputStream(new BufferedInputStream(@in));
			// Check
			DataInputBuffer originalData = new DataInputBuffer();
			originalData.Reset(data.GetData(), 0, data.GetLength());
			DataInputStream originalIn = new DataInputStream(new BufferedInputStream(originalData
				));
			for (int i_1 = 0; i_1 < count; ++i_1)
			{
				RandomDatum k1 = new RandomDatum();
				RandomDatum v1 = new RandomDatum();
				k1.ReadFields(originalIn);
				v1.ReadFields(originalIn);
				RandomDatum k2 = new RandomDatum();
				RandomDatum v2 = new RandomDatum();
				k2.ReadFields(dataIn);
				v2.ReadFields(dataIn);
				Assert.True("original and encrypted-then-decrypted-output not equal"
					, k1.Equals(k2) && v1.Equals(v2));
				// original and encrypted-then-decrypted-output have the same hashCode
				IDictionary<RandomDatum, string> m = new Dictionary<RandomDatum, string>();
				m[k1] = k1.ToString();
				m[v1] = v1.ToString();
				string result = m[k2];
				Assert.Equal("k1 and k2 hashcode not equal", result, k1.ToString
					());
				result = m[v2];
				Assert.Equal("v1 and v2 hashcode not equal", result, v1.ToString
					());
			}
			// Decrypt data byte-at-a-time
			originalData.Reset(data.GetData(), 0, data.GetLength());
			decryptedDataBuffer.Reset(encryptedDataBuffer.GetData(), 0, encryptedDataBuffer.GetLength
				());
			@in = new CryptoInputStream(decryptedDataBuffer, decCodec, bufferSize, key, iv);
			// Check
			originalIn = new DataInputStream(new BufferedInputStream(originalData));
			int expected;
			do
			{
				expected = originalIn.Read();
				Assert.Equal("Decrypted stream read by byte does not match", expected
					, @in.Read());
			}
			while (expected != -1);
			// Seek to a certain position and decrypt
			originalData.Reset(data.GetData(), 0, data.GetLength());
			decryptedDataBuffer.Reset(encryptedDataBuffer.GetData(), 0, encryptedDataBuffer.GetLength
				());
			@in = new CryptoInputStream(new TestCryptoStreams.FakeInputStream(decryptedDataBuffer
				), decCodec, bufferSize, key, iv);
			int seekPos = data.GetLength() / 3;
			@in.Seek(seekPos);
			// Check
			TestCryptoStreams.FakeInputStream originalInput = new TestCryptoStreams.FakeInputStream
				(originalData);
			originalInput.Seek(seekPos);
			do
			{
				expected = originalInput.Read();
				Assert.Equal("Decrypted stream read by byte does not match", expected
					, @in.Read());
			}
			while (expected != -1);
			Log.Info("SUCCESS! Completed checking " + count + " records");
			// Check secure random generator
			TestSecureRandom(encCodec);
		}

		/// <summary>Test secure random generator</summary>
		private void TestSecureRandom(CryptoCodec codec)
		{
			// len = 16
			CheckSecureRandom(codec, 16);
			// len = 32
			CheckSecureRandom(codec, 32);
			// len = 128
			CheckSecureRandom(codec, 128);
		}

		private void CheckSecureRandom(CryptoCodec codec, int len)
		{
			byte[] rand = new byte[len];
			byte[] rand1 = new byte[len];
			codec.GenerateSecureRandom(rand);
			codec.GenerateSecureRandom(rand1);
			Assert.Equal(len, rand.Length);
			Assert.Equal(len, rand1.Length);
			NUnit.Framework.Assert.IsFalse(Arrays.Equals(rand, rand1));
		}

		/// <summary>Regression test for IV calculation, see HADOOP-11343</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCalculateIV()
		{
			JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
			codec.SetConf(conf);
			SecureRandom sr = new SecureRandom();
			byte[] initIV = new byte[16];
			byte[] Iv = new byte[16];
			long iterations = 1000;
			long counter = 10000;
			// Overflow test, IV: 00 00 00 00 00 00 00 00 ff ff ff ff ff ff ff ff 
			for (int i = 0; i < 8; i++)
			{
				initIV[8 + i] = unchecked((byte)unchecked((int)(0xff)));
			}
			for (long j = 0; j < counter; j++)
			{
				AssertIVCalculation(codec, initIV, j, Iv);
			}
			// Random IV and counter sequence test
			for (long i_1 = 0; i_1 < iterations; i_1++)
			{
				sr.NextBytes(initIV);
				for (long j_1 = 0; j_1 < counter; j_1++)
				{
					AssertIVCalculation(codec, initIV, j_1, Iv);
				}
			}
			// Random IV and random counter test
			for (long i_2 = 0; i_2 < iterations; i_2++)
			{
				sr.NextBytes(initIV);
				for (long j_1 = 0; j_1 < counter; j_1++)
				{
					long c = sr.NextLong();
					AssertIVCalculation(codec, initIV, c, Iv);
				}
			}
		}

		private void AssertIVCalculation(CryptoCodec codec, byte[] initIV, long counter, 
			byte[] Iv)
		{
			codec.CalculateIV(initIV, counter, Iv);
			BigInteger iv = new BigInteger(1, Iv);
			BigInteger @ref = CalculateRef(initIV, counter);
			Assert.True("Calculated IV don't match with the reference", iv.
				Equals(@ref));
		}

		private static BigInteger CalculateRef(byte[] initIV, long counter)
		{
			byte[] cb = Longs.ToByteArray(counter);
			BigInteger bi = new BigInteger(1, initIV);
			return bi.Add(new BigInteger(1, cb));
		}
	}
}
