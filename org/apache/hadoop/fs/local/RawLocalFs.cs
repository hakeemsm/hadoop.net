using Sharpen;

namespace org.apache.hadoop.fs.local
{
	/// <summary>The RawLocalFs implementation of AbstractFileSystem.</summary>
	/// <remarks>
	/// The RawLocalFs implementation of AbstractFileSystem.
	/// This impl delegates to the old FileSystem
	/// </remarks>
	public class RawLocalFs : org.apache.hadoop.fs.DelegateToFileSystem
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.URISyntaxException"/>
		internal RawLocalFs(org.apache.hadoop.conf.Configuration conf)
			: this(org.apache.hadoop.fs.FsConstants.LOCAL_FS_URI, conf)
		{
		}

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
		internal RawLocalFs(java.net.URI theUri, org.apache.hadoop.conf.Configuration conf
			)
			: base(theUri, new org.apache.hadoop.fs.RawLocalFileSystem(), conf, org.apache.hadoop.fs.FsConstants
				.LOCAL_FS_URI.getScheme(), false)
		{
		}

		/*Evolving for a release,to be changed to Stable */
		public override int getUriDefaultPort()
		{
			return -1;
		}

		// No default port for file:///
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults()
		{
			return org.apache.hadoop.fs.local.LocalConfigKeys.getServerDefaults();
		}

		public override bool isValidName(string src)
		{
			// Different local file systems have different validation rules. Skip
			// validation here and just let the OS handle it. This is consistent with
			// RawLocalFileSystem.
			return true;
		}
	}
}
