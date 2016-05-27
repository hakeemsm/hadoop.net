using Org.Apache.Hadoop.Nfs.Nfs3;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>
	/// An NFS request that uses
	/// <see cref="Org.Apache.Hadoop.Nfs.Nfs3.FileHandle"/>
	/// to identify a file.
	/// </summary>
	public abstract class RequestWithHandle : NFS3Request
	{
		protected internal readonly FileHandle handle;

		internal RequestWithHandle(FileHandle handle)
		{
			this.handle = handle;
		}

		public virtual FileHandle GetHandle()
		{
			return this.handle;
		}
	}
}
