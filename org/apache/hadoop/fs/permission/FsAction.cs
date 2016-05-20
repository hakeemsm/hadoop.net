using Sharpen;

namespace org.apache.hadoop.fs.permission
{
	/// <summary>File system actions, e.g.</summary>
	/// <remarks>File system actions, e.g. read, write, etc.</remarks>
	[System.Serializable]
	public sealed class FsAction
	{
		public static readonly org.apache.hadoop.fs.permission.FsAction NONE = new org.apache.hadoop.fs.permission.FsAction
			("---");

		public static readonly org.apache.hadoop.fs.permission.FsAction EXECUTE = new org.apache.hadoop.fs.permission.FsAction
			("--x");

		public static readonly org.apache.hadoop.fs.permission.FsAction WRITE = new org.apache.hadoop.fs.permission.FsAction
			("-w-");

		public static readonly org.apache.hadoop.fs.permission.FsAction WRITE_EXECUTE = new 
			org.apache.hadoop.fs.permission.FsAction("-wx");

		public static readonly org.apache.hadoop.fs.permission.FsAction READ = new org.apache.hadoop.fs.permission.FsAction
			("r--");

		public static readonly org.apache.hadoop.fs.permission.FsAction READ_EXECUTE = new 
			org.apache.hadoop.fs.permission.FsAction("r-x");

		public static readonly org.apache.hadoop.fs.permission.FsAction READ_WRITE = new 
			org.apache.hadoop.fs.permission.FsAction("rw-");

		public static readonly org.apache.hadoop.fs.permission.FsAction ALL = new org.apache.hadoop.fs.permission.FsAction
			("rwx");

		/// <summary>Retain reference to value array.</summary>
		private static readonly org.apache.hadoop.fs.permission.FsAction[] vals = values(
			);

		/// <summary>Symbolic representation</summary>
		public readonly string SYMBOL;

		private FsAction(string s)
		{
			// POSIX style
			org.apache.hadoop.fs.permission.FsAction.SYMBOL = s;
		}

		/// <summary>Return true if this action implies that action.</summary>
		/// <param name="that"/>
		public bool implies(org.apache.hadoop.fs.permission.FsAction that)
		{
			if (that != null)
			{
				return (ordinal() & (int)(that)) == (int)(that);
			}
			return false;
		}

		/// <summary>AND operation.</summary>
		public org.apache.hadoop.fs.permission.FsAction and(org.apache.hadoop.fs.permission.FsAction
			 that)
		{
			return org.apache.hadoop.fs.permission.FsAction.vals[ordinal() & (int)(that)];
		}

		/// <summary>OR operation.</summary>
		public org.apache.hadoop.fs.permission.FsAction or(org.apache.hadoop.fs.permission.FsAction
			 that)
		{
			return org.apache.hadoop.fs.permission.FsAction.vals[ordinal() | (int)(that)];
		}

		/// <summary>NOT operation.</summary>
		public org.apache.hadoop.fs.permission.FsAction not()
		{
			return org.apache.hadoop.fs.permission.FsAction.vals[7 - ordinal()];
		}

		/// <summary>Get the FsAction enum for String representation of permissions</summary>
		/// <param name="permission">3-character string representation of permission. ex: rwx
		/// 	</param>
		/// <returns>
		/// Returns FsAction enum if the corresponding FsAction exists for permission.
		/// Otherwise returns null
		/// </returns>
		public static org.apache.hadoop.fs.permission.FsAction getFsAction(string permission
			)
		{
			foreach (org.apache.hadoop.fs.permission.FsAction fsAction in org.apache.hadoop.fs.permission.FsAction
				.vals)
			{
				if (fsAction.SYMBOL.Equals(permission))
				{
					return fsAction;
				}
			}
			return null;
		}
	}
}
