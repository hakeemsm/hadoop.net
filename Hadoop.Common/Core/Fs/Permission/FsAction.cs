using Sharpen;

namespace Org.Apache.Hadoop.FS.Permission
{
	/// <summary>File system actions, e.g.</summary>
	/// <remarks>File system actions, e.g. read, write, etc.</remarks>
	[System.Serializable]
	public sealed class FsAction
	{
		public static readonly Org.Apache.Hadoop.FS.Permission.FsAction None = new Org.Apache.Hadoop.FS.Permission.FsAction
			("---");

		public static readonly Org.Apache.Hadoop.FS.Permission.FsAction Execute = new Org.Apache.Hadoop.FS.Permission.FsAction
			("--x");

		public static readonly Org.Apache.Hadoop.FS.Permission.FsAction Write = new Org.Apache.Hadoop.FS.Permission.FsAction
			("-w-");

		public static readonly Org.Apache.Hadoop.FS.Permission.FsAction WriteExecute = new 
			Org.Apache.Hadoop.FS.Permission.FsAction("-wx");

		public static readonly Org.Apache.Hadoop.FS.Permission.FsAction Read = new Org.Apache.Hadoop.FS.Permission.FsAction
			("r--");

		public static readonly Org.Apache.Hadoop.FS.Permission.FsAction ReadExecute = new 
			Org.Apache.Hadoop.FS.Permission.FsAction("r-x");

		public static readonly Org.Apache.Hadoop.FS.Permission.FsAction ReadWrite = new Org.Apache.Hadoop.FS.Permission.FsAction
			("rw-");

		public static readonly Org.Apache.Hadoop.FS.Permission.FsAction All = new Org.Apache.Hadoop.FS.Permission.FsAction
			("rwx");

		/// <summary>Retain reference to value array.</summary>
		private static readonly Org.Apache.Hadoop.FS.Permission.FsAction[] vals = Values(
			);

		/// <summary>Symbolic representation</summary>
		public readonly string Symbol;

		private FsAction(string s)
		{
			// POSIX style
			Org.Apache.Hadoop.FS.Permission.FsAction.Symbol = s;
		}

		/// <summary>Return true if this action implies that action.</summary>
		/// <param name="that"/>
		public bool Implies(Org.Apache.Hadoop.FS.Permission.FsAction that)
		{
			if (that != null)
			{
				return (Ordinal() & (int)(that)) == (int)(that);
			}
			return false;
		}

		/// <summary>AND operation.</summary>
		public Org.Apache.Hadoop.FS.Permission.FsAction And(Org.Apache.Hadoop.FS.Permission.FsAction
			 that)
		{
			return Org.Apache.Hadoop.FS.Permission.FsAction.vals[Ordinal() & (int)(that)];
		}

		/// <summary>OR operation.</summary>
		public Org.Apache.Hadoop.FS.Permission.FsAction Or(Org.Apache.Hadoop.FS.Permission.FsAction
			 that)
		{
			return Org.Apache.Hadoop.FS.Permission.FsAction.vals[Ordinal() | (int)(that)];
		}

		/// <summary>NOT operation.</summary>
		public Org.Apache.Hadoop.FS.Permission.FsAction Not()
		{
			return Org.Apache.Hadoop.FS.Permission.FsAction.vals[7 - Ordinal()];
		}

		/// <summary>Get the FsAction enum for String representation of permissions</summary>
		/// <param name="permission">3-character string representation of permission. ex: rwx
		/// 	</param>
		/// <returns>
		/// Returns FsAction enum if the corresponding FsAction exists for permission.
		/// Otherwise returns null
		/// </returns>
		public static Org.Apache.Hadoop.FS.Permission.FsAction GetFsAction(string permission
			)
		{
			foreach (Org.Apache.Hadoop.FS.Permission.FsAction fsAction in Org.Apache.Hadoop.FS.Permission.FsAction
				.vals)
			{
				if (fsAction.Symbol.Equals(permission))
				{
					return fsAction;
				}
			}
			return null;
		}
	}
}
