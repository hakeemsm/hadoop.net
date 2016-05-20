using Sharpen;

namespace org.apache.hadoop.security.alias
{
	/// <summary>CredentialProvider based on Java's KeyStore file format.</summary>
	/// <remarks>
	/// CredentialProvider based on Java's KeyStore file format. The file may be
	/// stored only on the local filesystem using the following name mangling:
	/// localjceks://file/home/larry/creds.jceks -&gt; file:///home/larry/creds.jceks
	/// </remarks>
	public sealed class LocalJavaKeyStoreProvider : org.apache.hadoop.security.alias.AbstractJavaKeyStoreProvider
	{
		public const string SCHEME_NAME = "localjceks";

		private java.io.File file;

		private System.Collections.Generic.ICollection<java.nio.file.attribute.PosixFilePermission
			> permissions;

		/// <exception cref="System.IO.IOException"/>
		private LocalJavaKeyStoreProvider(java.net.URI uri, org.apache.hadoop.conf.Configuration
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
			java.io.FileOutputStream @out = new java.io.FileOutputStream(file);
			return @out;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override bool keystoreExists()
		{
			return file.exists();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override java.io.InputStream getInputStreamForFile()
		{
			java.io.FileInputStream @is = new java.io.FileInputStream(file);
			return @is;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void createPermissions(string perms)
		{
			int mode = 700;
			try
			{
				mode = System.Convert.ToInt32(perms, 8);
			}
			catch (java.lang.NumberFormatException nfe)
			{
				throw new System.IO.IOException("Invalid permissions mode provided while " + "trying to createPermissions"
					, nfe);
			}
			permissions = modeToPosixFilePermission(mode);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void stashOriginalFilePermissions()
		{
			// save off permissions in case we need to
			// rewrite the keystore in flush()
			if (!org.apache.hadoop.util.Shell.WINDOWS)
			{
				java.nio.file.Path path = java.nio.file.Paths.get(file.getCanonicalPath());
				permissions = java.nio.file.Files.getPosixFilePermissions(path);
			}
			else
			{
				// On Windows, the JDK does not support the POSIX file permission APIs.
				// Instead, we can do a winutils call and translate.
				string[] cmd = org.apache.hadoop.util.Shell.getGetPermissionCommand();
				string[] args = new string[cmd.Length + 1];
				System.Array.Copy(cmd, 0, args, 0, cmd.Length);
				args[cmd.Length] = file.getCanonicalPath();
				string @out = org.apache.hadoop.util.Shell.execCommand(args);
				java.util.StringTokenizer t = new java.util.StringTokenizer(@out, org.apache.hadoop.util.Shell
					.TOKEN_SEPARATOR_REGEX);
				// The winutils output consists of 10 characters because of the leading
				// directory indicator, i.e. "drwx------".  The JDK parsing method expects
				// a 9-character string, so remove the leading character.
				string permString = Sharpen.Runtime.substring(t.nextToken(), 1);
				permissions = java.nio.file.attribute.PosixFilePermissions.fromString(permString);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void initFileSystem(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf)
		{
			base.initFileSystem(uri, conf);
			try
			{
				file = new java.io.File(new java.net.URI(getPath().ToString()));
			}
			catch (java.net.URISyntaxException e)
			{
				throw new System.IO.IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void flush()
		{
			base.flush();
			if (!org.apache.hadoop.util.Shell.WINDOWS)
			{
				java.nio.file.Files.setPosixFilePermissions(java.nio.file.Paths.get(file.getCanonicalPath
					()), permissions);
			}
			else
			{
				// FsPermission expects a 10-character string because of the leading
				// directory indicator, i.e. "drwx------". The JDK toString method returns
				// a 9-character string, so prepend a leading character.
				org.apache.hadoop.fs.permission.FsPermission fsPermission = org.apache.hadoop.fs.permission.FsPermission
					.valueOf("-" + java.nio.file.attribute.PosixFilePermissions.toString(permissions
					));
				org.apache.hadoop.fs.FileUtil.setPermission(file, fsPermission);
			}
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
					return new org.apache.hadoop.security.alias.LocalJavaKeyStoreProvider(providerName
						, conf);
				}
				return null;
			}
		}

		private static System.Collections.Generic.ICollection<java.nio.file.attribute.PosixFilePermission
			> modeToPosixFilePermission(int mode)
		{
			System.Collections.Generic.ICollection<java.nio.file.attribute.PosixFilePermission
				> perms = java.util.EnumSet.noneOf<java.nio.file.attribute.PosixFilePermission>(
				);
			if ((mode & 0x1) != 0)
			{
				perms.add(java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE);
			}
			if ((mode & 0x2) != 0)
			{
				perms.add(java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE);
			}
			if ((mode & 0x4) != 0)
			{
				perms.add(java.nio.file.attribute.PosixFilePermission.OTHERS_READ);
			}
			if ((mode & 0x8) != 0)
			{
				perms.add(java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE);
			}
			if ((mode & 0x10) != 0)
			{
				perms.add(java.nio.file.attribute.PosixFilePermission.GROUP_WRITE);
			}
			if ((mode & 0x20) != 0)
			{
				perms.add(java.nio.file.attribute.PosixFilePermission.GROUP_READ);
			}
			if ((mode & 0x40) != 0)
			{
				perms.add(java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE);
			}
			if ((mode & 0x80) != 0)
			{
				perms.add(java.nio.file.attribute.PosixFilePermission.OWNER_WRITE);
			}
			if ((mode & 0x100) != 0)
			{
				perms.add(java.nio.file.attribute.PosixFilePermission.OWNER_READ);
			}
			return perms;
		}
	}
}
