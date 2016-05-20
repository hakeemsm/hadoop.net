using Sharpen;

namespace org.apache.hadoop.fs.permission
{
	/// <summary>
	/// Parse a permission mode passed in from a chmod command and apply that
	/// mode against an existing file.
	/// </summary>
	public class ChmodParser : org.apache.hadoop.fs.permission.PermissionParser
	{
		private static java.util.regex.Pattern chmodOctalPattern = java.util.regex.Pattern
			.compile("^\\s*[+]?([01]?)([0-7]{3})\\s*$");

		private static java.util.regex.Pattern chmodNormalPattern = java.util.regex.Pattern
			.compile("\\G\\s*([ugoa]*)([+=-]+)([rwxXt]+)([,\\s]*)\\s*");

		/// <exception cref="System.ArgumentException"/>
		public ChmodParser(string modeStr)
			: base(modeStr, chmodNormalPattern, chmodOctalPattern)
		{
		}

		/// <summary>
		/// Apply permission against specified file and determine what the
		/// new mode would be
		/// </summary>
		/// <param name="file">File against which to apply mode</param>
		/// <returns>File's new mode if applied.</returns>
		public virtual short applyNewPermission(org.apache.hadoop.fs.FileStatus file)
		{
			org.apache.hadoop.fs.permission.FsPermission perms = file.getPermission();
			int existing = perms.toShort();
			bool exeOk = file.isDirectory() || (existing & 0x49) != 0;
			return (short)combineModes(existing, exeOk);
		}
	}
}
