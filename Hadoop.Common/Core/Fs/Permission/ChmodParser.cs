using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Permission
{
	/// <summary>
	/// Parse a permission mode passed in from a chmod command and apply that
	/// mode against an existing file.
	/// </summary>
	public class ChmodParser : PermissionParser
	{
		private static Sharpen.Pattern chmodOctalPattern = Sharpen.Pattern.Compile("^\\s*[+]?([01]?)([0-7]{3})\\s*$"
			);

		private static Sharpen.Pattern chmodNormalPattern = Sharpen.Pattern.Compile("\\G\\s*([ugoa]*)([+=-]+)([rwxXt]+)([,\\s]*)\\s*"
			);

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
		public virtual short ApplyNewPermission(FileStatus file)
		{
			FsPermission perms = file.GetPermission();
			int existing = perms.ToShort();
			bool exeOk = file.IsDirectory() || (existing & 0x49) != 0;
			return (short)CombineModes(existing, exeOk);
		}
	}
}
