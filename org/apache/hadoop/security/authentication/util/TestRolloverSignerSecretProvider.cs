using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	public class TestRolloverSignerSecretProvider
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetAndRollSecrets()
		{
			long rolloverFrequency = 15 * 1000;
			// rollover every 15 sec
			byte[] secret1 = Sharpen.Runtime.getBytesForString("doctor");
			byte[] secret2 = Sharpen.Runtime.getBytesForString("who");
			byte[] secret3 = Sharpen.Runtime.getBytesForString("tardis");
			org.apache.hadoop.security.authentication.util.TestRolloverSignerSecretProvider.TRolloverSignerSecretProvider
				 secretProvider = new org.apache.hadoop.security.authentication.util.TestRolloverSignerSecretProvider.TRolloverSignerSecretProvider
				(this, new byte[][] { secret1, secret2, secret3 });
			try
			{
				secretProvider.init(null, null, rolloverFrequency);
				byte[] currentSecret = secretProvider.getCurrentSecret();
				byte[][] allSecrets = secretProvider.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(secret1, currentSecret);
				NUnit.Framework.Assert.AreEqual(2, allSecrets.Length);
				NUnit.Framework.Assert.assertArrayEquals(secret1, allSecrets[0]);
				NUnit.Framework.Assert.IsNull(allSecrets[1]);
				java.lang.Thread.sleep(rolloverFrequency + 2000);
				currentSecret = secretProvider.getCurrentSecret();
				allSecrets = secretProvider.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(secret2, currentSecret);
				NUnit.Framework.Assert.AreEqual(2, allSecrets.Length);
				NUnit.Framework.Assert.assertArrayEquals(secret2, allSecrets[0]);
				NUnit.Framework.Assert.assertArrayEquals(secret1, allSecrets[1]);
				java.lang.Thread.sleep(rolloverFrequency + 2000);
				currentSecret = secretProvider.getCurrentSecret();
				allSecrets = secretProvider.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(secret3, currentSecret);
				NUnit.Framework.Assert.AreEqual(2, allSecrets.Length);
				NUnit.Framework.Assert.assertArrayEquals(secret3, allSecrets[0]);
				NUnit.Framework.Assert.assertArrayEquals(secret2, allSecrets[1]);
				java.lang.Thread.sleep(rolloverFrequency + 2000);
			}
			finally
			{
				secretProvider.destroy();
			}
		}

		internal class TRolloverSignerSecretProvider : org.apache.hadoop.security.authentication.util.RolloverSignerSecretProvider
		{
			private byte[][] newSecretSequence;

			private int newSecretSequenceIndex;

			/// <exception cref="System.Exception"/>
			public TRolloverSignerSecretProvider(TestRolloverSignerSecretProvider _enclosing, 
				byte[][] newSecretSequence)
				: base()
			{
				this._enclosing = _enclosing;
				this.newSecretSequence = newSecretSequence;
				this.newSecretSequenceIndex = 0;
			}

			protected internal override byte[] generateNewSecret()
			{
				return this.newSecretSequence[this.newSecretSequenceIndex++];
			}

			private readonly TestRolloverSignerSecretProvider _enclosing;
		}
	}
}
