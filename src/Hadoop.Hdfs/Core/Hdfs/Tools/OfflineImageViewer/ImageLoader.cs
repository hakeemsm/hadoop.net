using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// An ImageLoader can accept a DataInputStream to an Hadoop FSImage file
	/// and walk over its structure using the supplied ImageVisitor.
	/// </summary>
	/// <remarks>
	/// An ImageLoader can accept a DataInputStream to an Hadoop FSImage file
	/// and walk over its structure using the supplied ImageVisitor.
	/// Each implementation of ImageLoader is designed to rapidly process an
	/// image file.  As long as minor changes are made from one layout version
	/// to another, it is acceptable to tweak one implementation to read the next.
	/// However, if the layout version changes enough that it would make a
	/// processor slow or difficult to read, another processor should be created.
	/// This allows each processor to quickly read an image without getting
	/// bogged down in dealing with significant differences between layout versions.
	/// </remarks>
	internal abstract class ImageLoader
	{
		/// <param name="in">DataInputStream pointing to an Hadoop FSImage file</param>
		/// <param name="v">Visit to apply to the FSImage file</param>
		/// <param name="enumerateBlocks">Should visitor visit each of the file blocks?</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void LoadImage(DataInputStream @in, ImageVisitor v, bool enumerateBlocks
			);

		/// <summary>Can this processor handle the specified version of FSImage file?</summary>
		/// <param name="version">FSImage version file</param>
		/// <returns>True if this instance can process the file</returns>
		public abstract bool CanLoadVersion(int version);

		/// <summary>
		/// Factory for obtaining version of image loader that can read
		/// a particular image format.
		/// </summary>
		public class LoaderFactory
		{
			// Java doesn't support static methods on interfaces, which necessitates
			// this factory class
			/// <summary>
			/// Find an image loader capable of interpreting the specified
			/// layout version number.
			/// </summary>
			/// <remarks>
			/// Find an image loader capable of interpreting the specified
			/// layout version number.  If none, return null;
			/// </remarks>
			/// <param name="version">fsimage layout version number to be processed</param>
			/// <returns>ImageLoader that can interpret specified version, or null</returns>
			public static ImageLoader GetLoader(int version)
			{
				// Easy to add more image processors as they are written
				ImageLoader[] loaders = new ImageLoader[] { new ImageLoaderCurrent() };
				foreach (ImageLoader l in loaders)
				{
					if (l.CanLoadVersion(version))
					{
						return l;
					}
				}
				return null;
			}
		}
	}

	internal static class ImageLoaderConstants
	{
	}
}
