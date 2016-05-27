using Sharpen;

namespace Org.Apache.Hadoop.FS.Permission
{
	/// <summary>
	/// Parse umask value provided as a string, either in octal or symbolic
	/// format and return it as a short value.
	/// </summary>
	/// <remarks>
	/// Parse umask value provided as a string, either in octal or symbolic
	/// format and return it as a short value. Umask values are slightly
	/// different from standard modes as they cannot specify sticky bit
	/// or X.
	/// </remarks>
	internal class UmaskParser : PermissionParser
	{
		private static Sharpen.Pattern chmodOctalPattern = Sharpen.Pattern.Compile("^\\s*[+]?()([0-7]{3})\\s*$"
			);

		private static Sharpen.Pattern umaskSymbolicPattern = Sharpen.Pattern.Compile("\\G\\s*([ugoa]*)([+=-]+)([rwx]*)([,\\s]*)\\s*"
			);

		internal readonly short umaskMode;

		/// <exception cref="System.ArgumentException"/>
		public UmaskParser(string modeStr)
			: base(modeStr, umaskSymbolicPattern, chmodOctalPattern)
		{
			// no leading 1 for sticky bit
			/* not allow X or t */
			umaskMode = (short)CombineModes(0, false);
		}

		/// <summary>To be used for file/directory creation only.</summary>
		/// <remarks>
		/// To be used for file/directory creation only. Symbolic umask is applied
		/// relative to file mode creation mask; the permission op characters '+'
		/// results in clearing the corresponding bit in the mask, '-' results in bits
		/// for indicated permission to be set in the mask.
		/// For octal umask, the specified bits are set in the file mode creation mask.
		/// </remarks>
		/// <returns>umask</returns>
		public virtual short GetUMask()
		{
			if (symbolic)
			{
				// Return the complement of octal equivalent of umask that was computed
				return (short)(~umaskMode & 0x1ff);
			}
			return umaskMode;
		}
	}
}
