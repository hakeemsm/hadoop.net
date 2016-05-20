using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	public class FailureInjectingJavaKeyStoreProvider : org.apache.hadoop.crypto.key.JavaKeyStoreProvider
	{
		public const string SCHEME_NAME = "failjceks";

		private bool backupFail = false;

		private bool writeFail = false;

		internal FailureInjectingJavaKeyStoreProvider(org.apache.hadoop.crypto.key.JavaKeyStoreProvider
			 prov)
			: base(prov)
		{
		}

		public virtual void setBackupFail(bool b)
		{
			backupFail = b;
		}

		public virtual void setWriteFail(bool b)
		{
			backupFail = b;
		}

		// Failure injection methods..
		/// <exception cref="System.IO.IOException"/>
		protected internal override void writeToNew(org.apache.hadoop.fs.Path newPath)
		{
			if (writeFail)
			{
				throw new System.IO.IOException("Injecting failure on write");
			}
			base.writeToNew(newPath);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override bool backupToOld(org.apache.hadoop.fs.Path oldPath)
		{
			if (backupFail)
			{
				throw new System.IO.IOException("Inejection Failure on backup");
			}
			return base.backupToOld(oldPath);
		}

		public class Factory : org.apache.hadoop.crypto.key.KeyProviderFactory
		{
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.crypto.key.KeyProvider createProvider(java.net.URI
				 providerName, org.apache.hadoop.conf.Configuration conf)
			{
				if (SCHEME_NAME.Equals(providerName.getScheme()))
				{
					try
					{
						return new org.apache.hadoop.crypto.key.FailureInjectingJavaKeyStoreProvider((org.apache.hadoop.crypto.key.JavaKeyStoreProvider
							)new org.apache.hadoop.crypto.key.JavaKeyStoreProvider.Factory().createProvider(
							new java.net.URI(providerName.ToString().Replace(SCHEME_NAME, org.apache.hadoop.crypto.key.JavaKeyStoreProvider
							.SCHEME_NAME)), conf));
					}
					catch (java.net.URISyntaxException e)
					{
						throw new System.Exception(e);
					}
				}
				return null;
			}
		}
	}
}
