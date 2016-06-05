using NUnit.Framework;


namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class TestRolloverSignerSecretProvider
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetAndRollSecrets()
		{
			long rolloverFrequency = 15 * 1000;
			// rollover every 15 sec
			byte[] secret1 = Runtime.GetBytesForString("doctor");
			byte[] secret2 = Runtime.GetBytesForString("who");
			byte[] secret3 = Runtime.GetBytesForString("tardis");
			TestRolloverSignerSecretProvider.TRolloverSignerSecretProvider secretProvider = new 
				TestRolloverSignerSecretProvider.TRolloverSignerSecretProvider(this, new byte[][]
				 { secret1, secret2, secret3 });
			try
			{
				secretProvider.Init(null, null, rolloverFrequency);
				byte[] currentSecret = secretProvider.GetCurrentSecret();
				byte[][] allSecrets = secretProvider.GetAllSecrets();
				Assert.AssertArrayEquals(secret1, currentSecret);
				Assert.Equal(2, allSecrets.Length);
				Assert.AssertArrayEquals(secret1, allSecrets[0]);
				NUnit.Framework.Assert.IsNull(allSecrets[1]);
				Thread.Sleep(rolloverFrequency + 2000);
				currentSecret = secretProvider.GetCurrentSecret();
				allSecrets = secretProvider.GetAllSecrets();
				Assert.AssertArrayEquals(secret2, currentSecret);
				Assert.Equal(2, allSecrets.Length);
				Assert.AssertArrayEquals(secret2, allSecrets[0]);
				Assert.AssertArrayEquals(secret1, allSecrets[1]);
				Thread.Sleep(rolloverFrequency + 2000);
				currentSecret = secretProvider.GetCurrentSecret();
				allSecrets = secretProvider.GetAllSecrets();
				Assert.AssertArrayEquals(secret3, currentSecret);
				Assert.Equal(2, allSecrets.Length);
				Assert.AssertArrayEquals(secret3, allSecrets[0]);
				Assert.AssertArrayEquals(secret2, allSecrets[1]);
				Thread.Sleep(rolloverFrequency + 2000);
			}
			finally
			{
				secretProvider.Destroy();
			}
		}

		internal class TRolloverSignerSecretProvider : RolloverSignerSecretProvider
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

			protected internal override byte[] GenerateNewSecret()
			{
				return this.newSecretSequence[this.newSecretSequenceIndex++];
			}

			private readonly TestRolloverSignerSecretProvider _enclosing;
		}
	}
}
