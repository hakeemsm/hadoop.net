using Sharpen;

namespace org.apache.hadoop.security.alias
{
	/// <summary>CredentialProvider based on Java's KeyStore file format.</summary>
	/// <remarks>
	/// CredentialProvider based on Java's KeyStore file format. The file may be
	/// stored in any Hadoop FileSystem using the following name mangling:
	/// jceks://hdfs@nn1.example.com/my/creds.jceks -&gt;
	/// hdfs://nn1.example.com/my/creds.jceks jceks://file/home/larry/creds.jceks -&gt;
	/// file:///home/larry/creds.jceks
	/// </remarks>
	public class JavaKeyStoreProvider : org.apache.hadoop.security.alias.AbstractJavaKeyStoreProvider
	{
		public const string SCHEME_NAME = "jceks";

		private org.apache.hadoop.fs.FileSystem fs;

		private org.apache.hadoop.fs.permission.FsPermission permissions;

		/// <exception cref="System.IO.IOException"/>
		private JavaKeyStoreProvider(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf)
			: base(uri, conf)
		{
		}

		protected internal override string getSchemeName()
		{
			return SCHEME_NAME;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override java.io.OutputStream getOutputStreamForKeystore()
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = org.apache.hadoop.fs.FileSystem.create
				(fs, getPath(), permissions);
			return @out;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override bool keystoreExists()
		{
			return fs.exists(getPath());
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override java.io.InputStream getInputStreamForFile()
		{
			return fs.open(getPath());
		}

		protected internal override void createPermissions(string perms)
		{
			permissions = new org.apache.hadoop.fs.permission.FsPermission(perms);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void stashOriginalFilePermissions()
		{
			// save off permissions in case we need to
			// rewrite the keystore in flush()
			org.apache.hadoop.fs.FileStatus s = fs.getFileStatus(getPath());
			permissions = s.getPermission();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void initFileSystem(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf)
		{
			base.initFileSystem(uri, conf);
			fs = getPath().getFileSystem(conf);
		}

		/// <summary>The factory to create JksProviders, which is used by the ServiceLoader.</summary>
		public class Factory : org.apache.hadoop.security.alias.CredentialProviderFactory
		{
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.security.alias.CredentialProvider createProvider
				(java.net.URI providerName, org.apache.hadoop.conf.Configuration conf)
			{
				if (SCHEME_NAME.Equals(providerName.getScheme()))
				{
					return new org.apache.hadoop.security.alias.JavaKeyStoreProvider(providerName, conf
						);
				}
				return null;
			}
		}
	}
}
