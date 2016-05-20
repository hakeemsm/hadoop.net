using Sharpen;

namespace org.apache.hadoop.crypto
{
	public class TestCryptoCodec
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.crypto.TestCryptoCodec
			)));

		private static byte[] key = new byte[16];

		private static byte[] iv = new byte[16];

		private const int bufferSize = 4096;

		private org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		private int count = 10000;

		private int seed = new java.util.Random().nextInt();

		private readonly string jceCodecClass = "org.apache.hadoop.crypto.JceAesCtrCryptoCodec";

		private readonly string opensslCodecClass = "org.apache.hadoop.crypto.OpensslAesCtrCryptoCodec";

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			java.util.Random random = new java.security.SecureRandom();
			random.nextBytes(key);
			random.nextBytes(iv);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testJceAesCtrCryptoCodec()
		{
			org.apache.hadoop.test.GenericTestUtils.assumeInNativeProfile();
			if (!org.apache.hadoop.util.NativeCodeLoader.buildSupportsOpenssl())
			{
				LOG.warn("Skipping test since openSSL library not loaded");
				NUnit.Framework.Assume.assumeTrue(false);
			}
			NUnit.Framework.Assert.AreEqual(null, org.apache.hadoop.crypto.OpensslCipher.getLoadingFailureReason
				());
			cryptoCodecTest(conf, seed, 0, jceCodecClass, jceCodecClass, iv);
			cryptoCodecTest(conf, seed, count, jceCodecClass, jceCodecClass, iv);
			cryptoCodecTest(conf, seed, count, jceCodecClass, opensslCodecClass, iv);
			// Overflow test, IV: xx xx xx xx xx xx xx xx ff ff ff ff ff ff ff ff 
			for (int i = 0; i < 8; i++)
			{
				iv[8 + i] = unchecked((byte)unchecked((int)(0xff)));
			}
			cryptoCodecTest(conf, seed, count, jceCodecClass, jceCodecClass, iv);
			cryptoCodecTest(conf, seed, count, jceCodecClass, opensslCodecClass, iv);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testOpensslAesCtrCryptoCodec()
		{
			org.apache.hadoop.test.GenericTestUtils.assumeInNativeProfile();
			if (!org.apache.hadoop.util.NativeCodeLoader.buildSupportsOpenssl())
			{
				LOG.warn("Skipping test since openSSL library not loaded");
				NUnit.Framework.Assume.assumeTrue(false);
			}
			NUnit.Framework.Assert.AreEqual(null, org.apache.hadoop.crypto.OpensslCipher.getLoadingFailureReason
				());
			cryptoCodecTest(conf, seed, 0, opensslCodecClass, opensslCodecClass, iv);
			cryptoCodecTest(conf, seed, count, opensslCodecClass, opensslCodecClass, iv);
			cryptoCodecTest(conf, seed, count, opensslCodecClass, jceCodecClass, iv);
			// Overflow test, IV: xx xx xx xx xx xx xx xx ff ff ff ff ff ff ff ff 
			for (int i = 0; i < 8; i++)
			{
				iv[8 + i] = unchecked((byte)unchecked((int)(0xff)));
			}
			cryptoCodecTest(conf, seed, count, opensslCodecClass, opensslCodecClass, iv);
			cryptoCodecTest(conf, seed, count, opensslCodecClass, jceCodecClass, iv);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.security.GeneralSecurityException"/>
		private void cryptoCodecTest(org.apache.hadoop.conf.Configuration conf, int seed, 
			int count, string encCodecClass, string decCodecClass, byte[] iv)
		{
			org.apache.hadoop.crypto.CryptoCodec encCodec = null;
			try
			{
				encCodec = (org.apache.hadoop.crypto.CryptoCodec)org.apache.hadoop.util.ReflectionUtils
					.newInstance(conf.getClassByName(encCodecClass), conf);
			}
			catch (java.lang.ClassNotFoundException)
			{
				throw new System.IO.IOException("Illegal crypto codec!");
			}
			LOG.info("Created a Codec object of type: " + encCodecClass);
			// Generate data
			org.apache.hadoop.io.DataOutputBuffer data = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.RandomDatum.Generator generator = new org.apache.hadoop.io.RandomDatum.Generator
				(seed);
			for (int i = 0; i < count; ++i)
			{
				generator.next();
				org.apache.hadoop.io.RandomDatum key = generator.getKey();
				org.apache.hadoop.io.RandomDatum value = generator.getValue();
				key.write(data);
				value.write(data);
			}
			LOG.info("Generated " + count + " records");
			// Encrypt data
			org.apache.hadoop.io.DataOutputBuffer encryptedDataBuffer = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.crypto.CryptoOutputStream @out = new org.apache.hadoop.crypto.CryptoOutputStream
				(encryptedDataBuffer, encCodec, bufferSize, key, iv);
			@out.write(data.getData(), 0, data.getLength());
			@out.flush();
			@out.close();
			LOG.info("Finished encrypting data");
			org.apache.hadoop.crypto.CryptoCodec decCodec = null;
			try
			{
				decCodec = (org.apache.hadoop.crypto.CryptoCodec)org.apache.hadoop.util.ReflectionUtils
					.newInstance(conf.getClassByName(decCodecClass), conf);
			}
			catch (java.lang.ClassNotFoundException)
			{
				throw new System.IO.IOException("Illegal crypto codec!");
			}
			LOG.info("Created a Codec object of type: " + decCodecClass);
			// Decrypt data
			org.apache.hadoop.io.DataInputBuffer decryptedDataBuffer = new org.apache.hadoop.io.DataInputBuffer
				();
			decryptedDataBuffer.reset(encryptedDataBuffer.getData(), 0, encryptedDataBuffer.getLength
				());
			org.apache.hadoop.crypto.CryptoInputStream @in = new org.apache.hadoop.crypto.CryptoInputStream
				(decryptedDataBuffer, decCodec, bufferSize, key, iv);
			java.io.DataInputStream dataIn = new java.io.DataInputStream(new java.io.BufferedInputStream
				(@in));
			// Check
			org.apache.hadoop.io.DataInputBuffer originalData = new org.apache.hadoop.io.DataInputBuffer
				();
			originalData.reset(data.getData(), 0, data.getLength());
			java.io.DataInputStream originalIn = new java.io.DataInputStream(new java.io.BufferedInputStream
				(originalData));
			for (int i_1 = 0; i_1 < count; ++i_1)
			{
				org.apache.hadoop.io.RandomDatum k1 = new org.apache.hadoop.io.RandomDatum();
				org.apache.hadoop.io.RandomDatum v1 = new org.apache.hadoop.io.RandomDatum();
				k1.readFields(originalIn);
				v1.readFields(originalIn);
				org.apache.hadoop.io.RandomDatum k2 = new org.apache.hadoop.io.RandomDatum();
				org.apache.hadoop.io.RandomDatum v2 = new org.apache.hadoop.io.RandomDatum();
				k2.readFields(dataIn);
				v2.readFields(dataIn);
				NUnit.Framework.Assert.IsTrue("original and encrypted-then-decrypted-output not equal"
					, k1.Equals(k2) && v1.Equals(v2));
				// original and encrypted-then-decrypted-output have the same hashCode
				System.Collections.Generic.IDictionary<org.apache.hadoop.io.RandomDatum, string> 
					m = new System.Collections.Generic.Dictionary<org.apache.hadoop.io.RandomDatum, 
					string>();
				m[k1] = k1.ToString();
				m[v1] = v1.ToString();
				string result = m[k2];
				NUnit.Framework.Assert.AreEqual("k1 and k2 hashcode not equal", result, k1.ToString
					());
				result = m[v2];
				NUnit.Framework.Assert.AreEqual("v1 and v2 hashcode not equal", result, v1.ToString
					());
			}
			// Decrypt data byte-at-a-time
			originalData.reset(data.getData(), 0, data.getLength());
			decryptedDataBuffer.reset(encryptedDataBuffer.getData(), 0, encryptedDataBuffer.getLength
				());
			@in = new org.apache.hadoop.crypto.CryptoInputStream(decryptedDataBuffer, decCodec
				, bufferSize, key, iv);
			// Check
			originalIn = new java.io.DataInputStream(new java.io.BufferedInputStream(originalData
				));
			int expected;
			do
			{
				expected = originalIn.read();
				NUnit.Framework.Assert.AreEqual("Decrypted stream read by byte does not match", expected
					, @in.read());
			}
			while (expected != -1);
			// Seek to a certain position and decrypt
			originalData.reset(data.getData(), 0, data.getLength());
			decryptedDataBuffer.reset(encryptedDataBuffer.getData(), 0, encryptedDataBuffer.getLength
				());
			@in = new org.apache.hadoop.crypto.CryptoInputStream(new org.apache.hadoop.crypto.TestCryptoStreams.FakeInputStream
				(decryptedDataBuffer), decCodec, bufferSize, key, iv);
			int seekPos = data.getLength() / 3;
			@in.seek(seekPos);
			// Check
			org.apache.hadoop.crypto.TestCryptoStreams.FakeInputStream originalInput = new org.apache.hadoop.crypto.TestCryptoStreams.FakeInputStream
				(originalData);
			originalInput.seek(seekPos);
			do
			{
				expected = originalInput.read();
				NUnit.Framework.Assert.AreEqual("Decrypted stream read by byte does not match", expected
					, @in.read());
			}
			while (expected != -1);
			LOG.info("SUCCESS! Completed checking " + count + " records");
			// Check secure random generator
			testSecureRandom(encCodec);
		}

		/// <summary>Test secure random generator</summary>
		private void testSecureRandom(org.apache.hadoop.crypto.CryptoCodec codec)
		{
			// len = 16
			checkSecureRandom(codec, 16);
			// len = 32
			checkSecureRandom(codec, 32);
			// len = 128
			checkSecureRandom(codec, 128);
		}

		private void checkSecureRandom(org.apache.hadoop.crypto.CryptoCodec codec, int len
			)
		{
			byte[] rand = new byte[len];
			byte[] rand1 = new byte[len];
			codec.generateSecureRandom(rand);
			codec.generateSecureRandom(rand1);
			NUnit.Framework.Assert.AreEqual(len, rand.Length);
			NUnit.Framework.Assert.AreEqual(len, rand1.Length);
			NUnit.Framework.Assert.IsFalse(java.util.Arrays.equals(rand, rand1));
		}

		/// <summary>Regression test for IV calculation, see HADOOP-11343</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testCalculateIV()
		{
			org.apache.hadoop.crypto.JceAesCtrCryptoCodec codec = new org.apache.hadoop.crypto.JceAesCtrCryptoCodec
				();
			codec.setConf(conf);
			java.security.SecureRandom sr = new java.security.SecureRandom();
			byte[] initIV = new byte[16];
			byte[] IV = new byte[16];
			long iterations = 1000;
			long counter = 10000;
			// Overflow test, IV: 00 00 00 00 00 00 00 00 ff ff ff ff ff ff ff ff 
			for (int i = 0; i < 8; i++)
			{
				initIV[8 + i] = unchecked((byte)unchecked((int)(0xff)));
			}
			for (long j = 0; j < counter; j++)
			{
				assertIVCalculation(codec, initIV, j, IV);
			}
			// Random IV and counter sequence test
			for (long i_1 = 0; i_1 < iterations; i_1++)
			{
				sr.nextBytes(initIV);
				for (long j_1 = 0; j_1 < counter; j_1++)
				{
					assertIVCalculation(codec, initIV, j_1, IV);
				}
			}
			// Random IV and random counter test
			for (long i_2 = 0; i_2 < iterations; i_2++)
			{
				sr.nextBytes(initIV);
				for (long j_1 = 0; j_1 < counter; j_1++)
				{
					long c = sr.nextLong();
					assertIVCalculation(codec, initIV, c, IV);
				}
			}
		}

		private void assertIVCalculation(org.apache.hadoop.crypto.CryptoCodec codec, byte
			[] initIV, long counter, byte[] IV)
		{
			codec.calculateIV(initIV, counter, IV);
			java.math.BigInteger iv = new java.math.BigInteger(1, IV);
			java.math.BigInteger @ref = calculateRef(initIV, counter);
			NUnit.Framework.Assert.IsTrue("Calculated IV don't match with the reference", iv.
				Equals(@ref));
		}

		private static java.math.BigInteger calculateRef(byte[] initIV, long counter)
		{
			byte[] cb = com.google.common.primitives.Longs.toByteArray(counter);
			java.math.BigInteger bi = new java.math.BigInteger(1, initIV);
			return bi.add(new java.math.BigInteger(1, cb));
		}
	}
}
