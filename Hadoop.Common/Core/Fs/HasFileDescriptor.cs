using System.IO;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>Having a FileDescriptor</summary>
	public interface HasFileDescriptor
	{
		/// <returns>the FileDescriptor</returns>
		/// <exception cref="System.IO.IOException"/>
		FileDescriptor GetFileDescriptor();
	}
}
