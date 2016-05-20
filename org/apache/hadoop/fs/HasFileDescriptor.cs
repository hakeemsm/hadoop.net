using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Having a FileDescriptor</summary>
	public interface HasFileDescriptor
	{
		/// <returns>the FileDescriptor</returns>
		/// <exception cref="System.IO.IOException"/>
		java.io.FileDescriptor getFileDescriptor();
	}
}
