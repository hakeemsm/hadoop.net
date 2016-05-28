using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset
{
	/// <summary>This is the interface for holding reference count as AutoClosable resource.
	/// 	</summary>
	/// <remarks>
	/// This is the interface for holding reference count as AutoClosable resource.
	/// It increases the reference count by one in the constructor, and decreases
	/// the reference count by one in
	/// <see cref="Close()"/>
	/// .
	/// <pre>
	/// <c/>
	/// try (FsVolumeReference ref = volume.obtainReference())
	/// // Do IOs on the volume
	/// volume.createRwb(...);
	/// ...
	/// }
	/// }
	/// </pre>
	/// </remarks>
	public interface FsVolumeReference : IDisposable
	{
		/// <summary>Descrese the reference count of the volume.</summary>
		/// <exception cref="System.IO.IOException">it never throws IOException.</exception>
		void Close();

		/// <summary>Returns the underlying volume object</summary>
		FsVolumeSpi GetVolume();
	}
}
