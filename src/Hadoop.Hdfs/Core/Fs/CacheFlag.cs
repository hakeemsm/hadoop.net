using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Specifies semantics for CacheDirective operations.</summary>
	/// <remarks>
	/// Specifies semantics for CacheDirective operations. Multiple flags can
	/// be combined in an EnumSet.
	/// </remarks>
	[System.Serializable]
	public sealed class CacheFlag
	{
		/// <summary>Ignore cache pool resource limits when performing this operation.</summary>
		public static readonly Org.Apache.Hadoop.FS.CacheFlag Force = new Org.Apache.Hadoop.FS.CacheFlag
			((short)unchecked((int)(0x01)));

		private readonly short mode;

		private CacheFlag(short mode)
		{
			this.mode = mode;
		}

		internal short GetMode()
		{
			return Org.Apache.Hadoop.FS.CacheFlag.mode;
		}
	}
}
