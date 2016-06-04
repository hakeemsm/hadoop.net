using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Local
{
	/// <summary>The RawLocalFs implementation of AbstractFileSystem.</summary>
	/// <remarks>
	/// The RawLocalFs implementation of AbstractFileSystem.
	/// This impl delegates to the old FileSystem
	/// </remarks>
	public class RawLocalFs : DelegateToFileSystem
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		internal RawLocalFs(Configuration conf)
			: this(FsConstants.LocalFsUri, conf)
		{
		}

		/// <summary>
		/// This constructor has the signature needed by
		/// <see cref="Org.Apache.Hadoop.FS.AbstractFileSystem.CreateFileSystem(Sharpen.URI, Configuration)
		/// 	"/>
		/// .
		/// </summary>
		/// <param name="theUri">which must be that of localFs</param>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"></exception>
		internal RawLocalFs(URI theUri, Configuration conf)
			: base(theUri, new RawLocalFileSystem(), conf, FsConstants.LocalFsUri.GetScheme()
				, false)
		{
		}

		/*Evolving for a release,to be changed to Stable */
		public override int GetUriDefaultPort()
		{
			return -1;
		}

		// No default port for file:///
		/// <exception cref="System.IO.IOException"/>
		public override FsServerDefaults GetServerDefaults()
		{
			return LocalConfigKeys.GetServerDefaults();
		}

		public override bool IsValidName(string src)
		{
			// Different local file systems have different validation rules. Skip
			// validation here and just let the OS handle it. This is consistent with
			// RawLocalFileSystem.
			return true;
		}
	}
}
