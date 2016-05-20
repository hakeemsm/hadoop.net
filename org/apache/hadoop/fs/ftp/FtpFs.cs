using Sharpen;

namespace org.apache.hadoop.fs.ftp
{
	/// <summary>The FtpFs implementation of AbstractFileSystem.</summary>
	/// <remarks>
	/// The FtpFs implementation of AbstractFileSystem.
	/// This impl delegates to the old FileSystem
	/// </remarks>
	public class FtpFs : org.apache.hadoop.fs.DelegateToFileSystem
	{
		/// <summary>
		/// This constructor has the signature needed by
		/// <see cref="org.apache.hadoop.fs.AbstractFileSystem.createFileSystem(java.net.URI, org.apache.hadoop.conf.Configuration)
		/// 	"/>
		/// .
		/// </summary>
		/// <param name="theUri">which must be that of localFs</param>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.URISyntaxException"></exception>
		internal FtpFs(java.net.URI theUri, org.apache.hadoop.conf.Configuration conf)
			: base(theUri, new org.apache.hadoop.fs.ftp.FTPFileSystem(), conf, org.apache.hadoop.fs.FsConstants
				.FTP_SCHEME, true)
		{
		}

		/*Evolving for a release,to be changed to Stable */
		public override int getUriDefaultPort()
		{
			return org.apache.commons.net.ftp.FTP.DEFAULT_PORT;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults()
		{
			return org.apache.hadoop.fs.ftp.FtpConfigKeys.getServerDefaults();
		}
	}
}
