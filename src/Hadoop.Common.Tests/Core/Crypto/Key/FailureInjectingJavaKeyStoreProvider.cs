using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.Crypto.Key
{
	public class FailureInjectingJavaKeyStoreProvider : JavaKeyStoreProvider
	{
		public const string SchemeName = "failjceks";

		private bool backupFail = false;

		private bool writeFail = false;

		internal FailureInjectingJavaKeyStoreProvider(JavaKeyStoreProvider prov)
			: base(prov)
		{
		}

		public virtual void SetBackupFail(bool b)
		{
			backupFail = b;
		}

		public virtual void SetWriteFail(bool b)
		{
			backupFail = b;
		}

		// Failure injection methods..
		/// <exception cref="System.IO.IOException"/>
		protected internal override void WriteToNew(Path newPath)
		{
			if (writeFail)
			{
				throw new IOException("Injecting failure on write");
			}
			base.WriteToNew(newPath);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override bool BackupToOld(Path oldPath)
		{
			if (backupFail)
			{
				throw new IOException("Inejection Failure on backup");
			}
			return base.BackupToOld(oldPath);
		}

		public class Factory : KeyProviderFactory
		{
			/// <exception cref="System.IO.IOException"/>
			public override KeyProvider CreateProvider(URI providerName, Configuration conf)
			{
				if (SchemeName.Equals(providerName.GetScheme()))
				{
					try
					{
						return new FailureInjectingJavaKeyStoreProvider((JavaKeyStoreProvider)new JavaKeyStoreProvider.Factory
							().CreateProvider(new URI(providerName.ToString().Replace(SchemeName, JavaKeyStoreProvider
							.SchemeName)), conf));
					}
					catch (URISyntaxException e)
					{
						throw new RuntimeException(e);
					}
				}
				return null;
			}
		}
	}
}
