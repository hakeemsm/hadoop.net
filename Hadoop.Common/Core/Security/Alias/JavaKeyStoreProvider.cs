using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Alias
{
	/// <summary>CredentialProvider based on Java's KeyStore file format.</summary>
	/// <remarks>
	/// CredentialProvider based on Java's KeyStore file format. The file may be
	/// stored in any Hadoop FileSystem using the following name mangling:
	/// jceks://hdfs@nn1.example.com/my/creds.jceks -&gt;
	/// hdfs://nn1.example.com/my/creds.jceks jceks://file/home/larry/creds.jceks -&gt;
	/// file:///home/larry/creds.jceks
	/// </remarks>
	public class JavaKeyStoreProvider : AbstractJavaKeyStoreProvider
	{
		public const string SchemeName = "jceks";

		private FileSystem fs;

		private FsPermission permissions;

		/// <exception cref="System.IO.IOException"/>
		private JavaKeyStoreProvider(URI uri, Configuration conf)
			: base(uri, conf)
		{
		}

		protected internal override string GetSchemeName()
		{
			return SchemeName;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override OutputStream GetOutputStreamForKeystore()
		{
			FSDataOutputStream @out = FileSystem.Create(fs, GetPath(), permissions);
			return @out;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override bool KeystoreExists()
		{
			return fs.Exists(GetPath());
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override InputStream GetInputStreamForFile()
		{
			return fs.Open(GetPath());
		}

		protected internal override void CreatePermissions(string perms)
		{
			permissions = new FsPermission(perms);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StashOriginalFilePermissions()
		{
			// save off permissions in case we need to
			// rewrite the keystore in flush()
			FileStatus s = fs.GetFileStatus(GetPath());
			permissions = s.GetPermission();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void InitFileSystem(URI uri, Configuration conf)
		{
			base.InitFileSystem(uri, conf);
			fs = GetPath().GetFileSystem(conf);
		}

		/// <summary>The factory to create JksProviders, which is used by the ServiceLoader.</summary>
		public class Factory : CredentialProviderFactory
		{
			/// <exception cref="System.IO.IOException"/>
			public override CredentialProvider CreateProvider(URI providerName, Configuration
				 conf)
			{
				if (SchemeName.Equals(providerName.GetScheme()))
				{
					return new JavaKeyStoreProvider(providerName, conf);
				}
				return null;
			}
		}
	}
}
